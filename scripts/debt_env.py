"""
Debt Collection Environment — RL Environment for debt collection.

Run from project root:
  python scripts/debt_env.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import gymnasium as gym
from gymnasium import spaces
import pandas as pd
import numpy as np
import torch
from stable_baselines3 import PPO
from stable_baselines3.common.vec_env import DummyVecEnv, VecNormalize
from config import cfg, logger


class DebtCollectionEnv(gym.Env):
    """
    Enhanced debt collection environment with graph-derived features.

    Observation space now includes:
      - 8 original borrower features
      - 3 structural features (degree, pagerank, betweenness)
      - 4 community features (risk %, avg overdue, total demand, size)
      - 3 multi-hop neighborhood signals (1-hop, 2-hop, 3-hop)
      - 16 GAT embedding dimensions

    Total: 34 dimensions (was 10)
    """
    def __init__(self, df_path):
        super(DebtCollectionEnv, self).__init__()

        # 1. Load the Graph-Enhanced Data
        self.df = pd.read_csv(df_path)
        self.n_rows = len(self.df)
        self.current_row = 0
        self.steps_taken = 0
        self.max_steps = 100  # Process 100 borrowers per episode

        # Episode tracking (for VecInfo/episode logging)
        self._episode_reward = 0.0
        self._episode_length = 0

        # 2. Determine observation dimension dynamically
        # Read the first row to count all numeric columns used as features
        self.feature_cols = self._get_feature_columns()
        obs_dim = len(self.feature_cols)

        # 3. Action Space: [No Action, SMS/Call, Field Visit, Legal Notice]
        self.action_space = spaces.Discrete(4)

        # 4. Observation Space: Dynamic based on available features
        self.observation_space = spaces.Box(
            low=-np.inf, high=np.inf, shape=(obs_dim,), dtype=np.float32
        )

        print(f"  Environment observation space: {obs_dim} dimensions")
        print(f"  Features: {self.feature_cols}")

    def _get_feature_columns(self):
        """
        Dynamically detect which feature columns exist in the CSV.
        Supports both old (10-dim) and new (34-dim) formats.
        """
        cols = []

        # Core borrower features (always present)
        core_features = [
            'income', 'cibil_score', 'overdue_months', 'bounce_count',
            'coll_success_rate', 'age', 'occ_idx', 'reg_idx'
        ]
        for f in core_features:
            if f in self.df.columns:
                cols.append(f)

        # Structural features
        structural = ['node_degree', 'pagerank', 'betweenness']
        for f in structural:
            if f in self.df.columns:
                cols.append(f)

        # Community features
        community = ['community_risk_pct', 'community_avg_overdue',
                     'community_total_demand', 'community_size']
        for f in community:
            if f in self.df.columns:
                cols.append(f)

        # Multi-hop signals
        multihop = ['neighborhood_stress_1hop', 'neighborhood_stress_2hop',
                    'neighborhood_stress_3hop']
        for f in multihop:
            if f in self.df.columns:
                cols.append(f)

        # GAT embeddings (detect all gat_embedding_N columns)
        gat_cols = [c for c in self.df.columns if c.startswith('gat_embedding_')]
        if gat_cols:
            # Sort by numeric suffix to ensure consistent ordering
            gat_cols.sort(key=lambda x: int(x.split('_')[-1]))
            cols.extend(gat_cols)

        # Legacy fallback: old single neighborhood_stress_signal
        if 'neighborhood_stress_signal' in self.df.columns and 'neighborhood_stress_1hop' not in self.df.columns:
            cols.append('neighborhood_stress_signal')

        return cols

    def _get_obs(self):
        """Extract features for the current borrower."""
        row = self.df.iloc[self.current_row]
        obs = np.array([row[col] for col in self.feature_cols], dtype=np.float32)
        return obs

    def reset(self, seed=None, options=None):
        super().reset(seed=seed)
        # Use the seeded np_random from Gymnasium for reproducibility
        self.current_row = int(self.np_random.integers(0, self.n_rows))
        self._episode_reward = 0.0
        self._episode_length = 0
        return self._get_obs(), {}

    def step(self, action):
        row = self.df.iloc[self.current_row]

        reward = 0
        done = True

        risk = row["risk_category"]

        # -- Cost and Base Recovery by Risk Category --
        if risk in ["Very Low", "Low"]:
            cost_map = {0: 0, 1: 30, 2: 300, 3: 800}
            base_recovery = {0: 0.0, 1: 0.4, 2: 0.7, 3: 0.3}
        elif risk == "Medium":
            cost_map = {0: 0, 1: 50, 2: 400, 3: 500}
            base_recovery = {0: 0.0, 1: 0.3, 2: 0.6, 3: 0.5}
        else:  # High, Very High
            cost_map = {0: 0, 1: 80, 2: 500, 3: 250}
            base_recovery = {0: 0.0, 1: 0.15, 2: 0.65, 3: 0.7}

        # -- 1. Action Cost --
        reward -= cost_map[action]

        # -- 2. Recovery Probability --
        # Use 1-hop stress if available, else fall back to legacy signal
        if 'neighborhood_stress_1hop' in row:
            stress = row['neighborhood_stress_1hop']
        elif 'neighborhood_stress_signal' in row:
            stress = row['neighborhood_stress_signal']
        else:
            stress = 0.0

        # Normalize stress: typical overdue range is 0-12
        stress_factor = max(0.0, 1.0 - (stress / 12.0))

        success_prob = base_recovery[action] * row["coll_success_rate"] * stress_factor

        if self.np_random.random() < success_prob:
            if action == 1:  # SMS
                recovered_amt = row["total_demand"] * self.np_random.uniform(0.05, 0.15)
            elif action == 2:  # Field Visit
                recovered_amt = row["total_demand"] * self.np_random.uniform(0.15, 0.35)
            elif action == 3:  # Legal
                recovered_amt = row["total_demand"] * self.np_random.uniform(0.20, 0.40)
            else:
                recovered_amt = 0
            reward += recovered_amt

        # -- 3. Penalty for Wrong Action on Risk Profile --
        # Penalties tuned per evaluation report analysis to match optimal action distributions.
        # Target distributions (from domain knowledge):
        #   Very Low: No Action 30%, SMS 60%, FV 10%, Legal 0%
        #   Low:      No Action 20%, SMS 50%, FV 25%, Legal 5%
        #   Medium:   No Action 10%, SMS 30%, FV 40%, Legal 20%
        #   High:     No Action 0%,  SMS 10%, FV 50%, Legal 40%
        #   Very High:No Action 0%,  SMS 5%,  FV 45%, Legal 50%
        if risk == "Very Low" and action == 2:
            reward -= row["total_demand"] * 0.50   # Field Visit on Very Low (was 0.45)
        elif risk == "Very Low" and action == 3:
            reward -= row["total_demand"] * 0.70   # Legal on Very Low (was 0.65)
        elif risk == "Low" and action == 3:
            reward -= row["total_demand"] * 0.70   # Legal on Low (was 0.60, target <5% usage)
        elif risk == "Low" and action == 2:
            reward -= row["total_demand"] * 0.50   # Field Visit on Low (was 0.35, target 25%)
        elif risk == "Medium" and action == 3:
            # Warning tier: penalize Legal on Medium unless borrower is severely overdue
            if row["overdue_months"] <= 5:
                reward -= row["total_demand"] * 0.45  # Increased from 0.40
        elif risk == "Medium" and action == 2:
            # Mild penalty to curb Field Visit over-indexing (49.7% → target 40%)
            # Only apply when overdue is moderate (not severe)
            if row["overdue_months"] <= 4:
                reward -= row["total_demand"] * 0.15
        elif risk in ["High", "Very High"] and action == 0:
            reward -= row["total_demand"] * 0.50   # No action on high risk
        elif risk in ["High", "Very High"] and action == 1:
            reward -= row["total_demand"] * 0.40   # SMS on high risk

        # -- 3b. Incentivize No Action / SMS on low-risk borrowers --
        if risk == "Very Low" and action == 0:
            if row["overdue_months"] <= 1:
                reward += row["total_demand"] * 0.05  # Bonus for restraint
        if risk == "Very Low" and action == 1:
            if row["overdue_months"] <= 1:
                reward += row["total_demand"] * 0.02  # Small bonus for SMS on very low
        if risk == "Low" and action == 0:
            if row["overdue_months"] <= 2:
                reward += row["total_demand"] * 0.05  # Increased from 0.03
        if risk == "Low" and action == 1:
            if row["overdue_months"] <= 3:
                reward += row["total_demand"] * 0.03  # Encourage SMS over FV/Legal

        # -- 3c. Discourage No Action on Medium risk (generates zero revenue) --
        # The agent currently uses No Action 25.5% on Medium risk, causing losses.
        # Apply a mild penalty to push toward productive actions (SMS or Field Visit).
        if risk == "Medium" and action == 0:
            if row["overdue_months"] >= 3:
                reward -= row["total_demand"] * 0.10  # Mild nudge away from No Action

        # -- 4. Bonus: Contagion Prevention (new) --
        # Reward collecting from high-centrality borrowers in risky communities
        # This teaches the agent to prioritize "influential" nodes
        if 'pagerank' in row and 'community_risk_pct' in row:
            if row['community_risk_pct'] > 50:  # Community is >50% high-risk
                # Bonus proportional to node's influence in the graph
                contagion_bonus = row['pagerank'] * row['total_demand'] * 0.1
                reward += contagion_bonus

        # -- 5. Advance to Next Borrower --
        self.current_row = (self.current_row + 1) % self.n_rows
        self.steps_taken += 1
        self._episode_reward += reward
        self._episode_length += 1
        done = self.steps_taken >= self.max_steps
        if done:
            self.steps_taken = 0

        info = {}
        if done:
            info['episode'] = {
                'r': self._episode_reward,
                'l': self._episode_length,
                't': self._episode_length,  # time steps
            }

        return self._get_obs(), reward, done, False, info

    def get_action_mask(self, row):
        """
        Get a valid action mask for the current borrower based on risk category.

        Returns a numpy array of shape (4,) with 1 for valid actions, 0 for invalid.
        This implements domain-knowledge constraints to prevent obviously wrong actions:

          Very Low: No Action (✓), SMS (✓), FV (✓ for overdue>1), Legal (✗)
          Low:      No Action (✓), SMS (✓), FV (✓ for overdue>2), Legal (✗)
          Medium:   All actions allowed
          High:     SMS (✓), FV (✓), Legal (✓), No Action (✗)
          Very High:SMS (✓ for overdue>3), FV (✓), Legal (✓), No Action (✗)

        Use with: masked_probs = probs * mask; masked_probs /= masked_probs.sum()
        """
        mask = np.ones(4, dtype=np.float32)  # All actions valid by default
        risk = row.get("risk_category", "Medium")
        overdue = row.get("overdue_months", 0)

        if risk == "Very Low":
            mask[3] = 0.0  # Never use Legal on Very Low
            if overdue <= 1:
                mask[2] = 0.0  # No Field Visit when barely overdue
        elif risk == "Low":
            mask[3] = 0.0  # Never use Legal on Low
            if overdue <= 2:
                mask[2] = 0.0  # No Field Visit when barely overdue
        elif risk == "High":
            mask[0] = 0.0  # Never use No Action on High
        elif risk == "Very High":
            mask[0] = 0.0  # Never use No Action on Very High
            if overdue <= 3:
                mask[1] = 0.0  # SMS too weak for Very High with low overdue

        return mask

    def apply_action_mask(self, probs, row):
        """
        Apply risk-aware action masking to probability distribution.

        Args:
            probs: Array of action probabilities from PPO policy
            row: Borrower data row

        Returns:
            masked_probs: Probabilities with invalid actions zeroed and renormalized
            mask: The mask that was applied
        """
        mask = self.get_action_mask(row)
        masked_probs = probs * mask
        if masked_probs.sum() > 0:
            masked_probs = masked_probs / masked_probs.sum()
        else:
            # If all actions are masked out, fall back to original probabilities
            masked_probs = probs.copy()
        return masked_probs, mask

    def get_action_probs(self, model, obs=None, deterministic=False):
        """
        Extract action probabilities and confidence from the PPO policy.

        Returns:
            dict with:
              - action: chosen action index
              - probs: array of 4 action probabilities
              - confidence: max probability (decision certainty)
              - entropy: policy entropy (higher = more uncertain)
              - margin: difference between top 2 actions
        """
        if obs is None:
            obs = self._get_obs()

        obs_tensor = torch.tensor(obs).unsqueeze(0).float()
        if hasattr(model, 'device') and model.device is not None:
            obs_tensor = obs_tensor.to(model.device)

        dist = model.policy.get_distribution(obs_tensor)
        probs = dist.distribution.probs.cpu().detach().numpy()[0]
        action = int(np.argmax(probs)) if deterministic else int(dist.sample().cpu().numpy()[0])

        confidence = float(np.max(probs))
        entropy = float(-np.sum(probs * np.log(probs + 1e-8)))
        sorted_probs = np.sort(probs)[::-1]
        margin = float(sorted_probs[0] - sorted_probs[1]) if len(sorted_probs) > 1 else 1.0

        return {
            'action': action,
            'probs': probs.tolist(),
            'confidence': confidence,
            'entropy': entropy,
            'margin': margin,
            'is_uncertain': entropy > 1.2,  # Threshold for flagging
        }


ACTION_NAMES = ["No Action", "SMS/Call", "Field Visit", "Legal Notice"]


def _filter_risks(env, allowed=None):
    """
    Filter the DataFrame in the environment to only include specified risk categories.

    Args:
        env: DebtCollectionEnv instance
        allowed: Set of risk categories to include, or None to include all
    """
    full_df = pd.read_csv("rl_ready_with_graph_features.csv")
    if allowed is None:
        env.df = full_df
    else:
        env.df = full_df[full_df["risk_category"].isin(allowed)].copy()
    env.n_rows = len(env.df)
    logger.info(f"  DataFrame filtered to: {allowed or 'ALL'} — {env.n_rows:,} rows")


if __name__ == "__main__":
    # 1. Instantiate the raw environment
    base_env = DebtCollectionEnv("rl_ready_with_graph_features.csv")

    num_rows = base_env.n_rows
    logger.info(f"Training on {num_rows} data points...")

    # 2. Adaptive timesteps based on feature set
    feature_set = cfg.feature_set
    base_timesteps = cfg.rl_total_timesteps
    if feature_set == "full":
        # Full 150-feature set needs 10x more training
        total_timesteps = max(base_timesteps * 10, 5_000_000)
        logger.info(f"Feature set: {feature_set} (150 features) — adjusting timesteps to {total_timesteps:,}")
    else:
        total_timesteps = base_timesteps
        logger.info(f"Feature set: {feature_set} — using {total_timesteps:,} timesteps")

    # 3. Wrap in DummyVecEnv
    env = DummyVecEnv([lambda: base_env])

    # 4. Wrap in VecNormalize for feature scaling
    env = VecNormalize(env, norm_obs=True, norm_reward=True, clip_obs=10.0)

    # 5. Initialize PPO (using config settings)
    model = PPO(
        "MlpPolicy", env, verbose=1,
        learning_rate=cfg.rl_learning_rate,
        n_steps=cfg.rl_n_steps,
        device=cfg.rl_device,
    )

    # 6. Training with optional curriculum learning
    if cfg.curriculum_learning:
        logger.info("=== CURRICULUM LEARNING ENABLED ===")
        logger.info("Training in 3 phases: High/Very High → +Medium → All risks")

        # Phase 1: High + Very High only (25% of timesteps)
        phase1_steps = total_timesteps // 4
        logger.info(f"Phase 1: {phase1_steps:,} timesteps — High + Very High risk only")
        _filter_risks(base_env, allowed={"High", "Very High"})
        model.learn(total_timesteps=phase1_steps, reset_num_timesteps=False)

        # Phase 2: High + Very High + Medium (25% of timesteps)
        phase2_steps = total_timesteps // 4
        logger.info(f"Phase 2: {phase2_steps:,} timesteps — High + Very High + Medium risk")
        _filter_risks(base_env, allowed={"High", "Very High", "Medium"})
        model.learn(total_timesteps=phase2_steps, reset_num_timesteps=False)

        # Phase 3: All risk categories (50% of timesteps)
        phase3_steps = total_timesteps - phase1_steps - phase2_steps
        logger.info(f"Phase 3: {phase3_steps:,} timesteps — All risk categories")
        _filter_risks(base_env, allowed=None)  # None = no filtering
        model.learn(total_timesteps=phase3_steps, reset_num_timesteps=False)

        logger.info("=== CURRICULUM LEARNING COMPLETE ===")
    else:
        logger.info("Training the Graph-Based RL Agent (Enhanced Features)...")
        model.learn(total_timesteps=total_timesteps)

    # 7. Save model and normalization stats
    model.save("graph_rl_debt_model")
    env.save("vec_normalize.pkl")
    logger.info("Model and Normalization Stats Saved Successfully!")
