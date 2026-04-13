"""
Hybrid Policy — Rule-based + PPO wrapper.

Per the evaluation report:
  - Rule-based achieves Rs. 3,671/decision vs PPO's Rs. 2,513 (46% more efficient)
  - PPO excels at High/Very High risk (95.9%/99.8% correctness)
  - Rule-based works well for Low/Medium risk where PPO over-escalates

Strategy:
  - Very Low risk → Rule-based (No Action or SMS)
  - Low risk      → Rule-based (SMS/Call)
  - Medium risk   → PPO (with rule-based guardrails)
  - High risk     → PPO (where nuance matters)
  - Very High risk→ PPO (where graph features add value)

Usage:
    python scripts/hybrid_policy.py
    python scripts/hybrid_policy.py --evaluate   # Run full evaluation
    python scripts/hybrid_policy.py --demo       # Show sample decisions
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import json
import time
import numpy as np
import pandas as pd
import torch
import warnings

from stable_baselines3 import PPO
from stable_baselines3.common.vec_env import DummyVecEnv, VecNormalize

from scripts.debt_env import DebtCollectionEnv, ACTION_NAMES
from config import cfg, logger

warnings.filterwarnings('ignore')

# Rule-based policy for Very Low / Low risk borrowers
RULE_BASED_POLICY = {
    'Very Low': 1,   # SMS/Call (60% No Action for low overdue, else SMS)
    'Low': 1,        # SMS/Call
}

# Risk categories handled by rules vs PPO
RULE_BASED_RISKS = {'Very Low', 'Low'}
PPO_RISKS = {'Medium', 'High', 'Very High'}


class HybridPolicy:
    """
    Wraps a trained PPO model and applies rule-based actions for
    Very Low and Low risk borrowers, delegating to PPO for
    Medium, High, and Very High risk borrowers.
    """

    def __init__(self, ppo_model, vec_env=None):
        """
        Args:
            ppo_model: Trained PPO model (loaded via PPO.load)
            vec_env: VecNormalize environment (for observation normalization)
        """
        self.ppo_model = ppo_model
        self.vec_env = vec_env

        # Extract observation normalization stats if available
        self.obs_rms = None
        if vec_env is not None and hasattr(vec_env, 'obs_rms'):
            self.obs_rms = vec_env.obs_rms

    def _normalize_obs(self, obs):
        """Normalize observation using VecNormalize stats."""
        if self.obs_rms is None:
            return obs.astype(np.float32)

        mean = self.obs_rms.mean
        var = self.obs_rms.var
        if hasattr(mean, 'detach'):
            mean = mean.detach().cpu().numpy()
        if hasattr(var, 'detach'):
            var = var.detach().cpu().numpy()

        clipped_obs = np.clip((obs - mean) / np.sqrt(var + 1e-8), -10.0, 10.0)
        return clipped_obs.astype(np.float32)

    def _get_risk_category(self, df, current_row_idx):
        """Extract risk category from the DataFrame."""
        return df.iloc[current_row_idx].get('risk_category', 'Medium')

    def _rule_based_action(self, df, current_row_idx, base_action=None):
        """
        Apply rule-based policy for Very Low / Low risk.

        For Very Low: prefer No Action if overdue_months <= 1, else SMS
        For Low: prefer SMS/Call, occasional Field Visit for higher overdue
        """
        row = df.iloc[current_row_idx]
        risk = row.get('risk_category', 'Medium')
        overdue = row.get('overdue_months', 0)

        if risk == 'Very Low':
            if overdue <= 1:
                return 0  # No Action
            else:
                return 1  # SMS/Call

        elif risk == 'Low':
            if overdue <= 2:
                return 1  # SMS/Call
            elif overdue <= 3:
                # Mix of SMS and occasional Field Visit
                return np.random.choice([1, 2], p=[0.7, 0.3])
            else:
                return 2  # Field Visit for higher overdue Low risk

        # Fallback to default
        return RULE_BASED_POLICY.get(risk, 1)

    def predict(self, obs, df, current_row_idx, deterministic=True):
        """
        Predict action using hybrid policy.

        Args:
            obs: Current observation (raw, not normalized)
            df: DataFrame with borrower data
            current_row_idx: Index of current borrower in df
            deterministic: Whether to use deterministic PPO action

        Returns:
            action: Selected action index
            confidence: Decision confidence (1.0 for rule-based, PPO confidence otherwise)
            policy_used: 'rule_based' or 'ppo'
        """
        risk = self._get_risk_category(df, current_row_idx)

        if risk in RULE_BASED_RISKS:
            action = self._rule_based_action(df, current_row_idx)
            return action, 1.0, 'rule_based'
        else:
            # Use PPO with normalized observation
            norm_obs = self._normalize_obs(obs)
            action, _ = self.ppo_model.predict(norm_obs, deterministic=deterministic)
            action = int(np.asarray(action).item()) if np.asarray(action).ndim == 0 else int(action[0])

            # Get confidence and apply action mask
            try:
                conf_result = self._get_ppo_confidence(norm_obs)
                probs = np.array(conf_result['probs'])

                # Apply risk-aware action masking
                row = df.iloc[current_row_idx]
                mask = self._get_action_mask(row)
                masked_probs = probs * mask
                if masked_probs.sum() > 0:
                    masked_probs = masked_probs / masked_probs.sum()
                    action = int(np.argmax(masked_probs))
                    confidence = float(masked_probs[action])
                else:
                    confidence = conf_result['confidence']

                return action, confidence, 'ppo'
            except Exception:
                return action, 0.9, 'ppo'

    def _get_action_mask(self, row):
        """
        Get action mask matching the environment's masking logic.
        This is a duplicate of DebtCollectionEnv.get_action_mask to avoid
        circular imports.
        """
        mask = np.ones(4, dtype=np.float32)
        risk = row.get('risk_category', 'Medium')
        overdue = row.get('overdue_months', 0)

        if risk == 'Very Low':
            mask[3] = 0.0  # Never Legal
            if overdue <= 1:
                mask[2] = 0.0  # No FV when barely overdue
        elif risk == 'Low':
            mask[3] = 0.0  # Never Legal
            if overdue <= 2:
                mask[2] = 0.0  # No FV when barely overdue
        elif risk == 'High':
            mask[0] = 0.0  # Never No Action
        elif risk == 'Very High':
            mask[0] = 0.0  # Never No Action
            if overdue <= 3:
                mask[1] = 0.0  # SMS too weak

        return mask

    def _get_ppo_confidence(self, obs):
        """Get action probabilities and confidence from PPO policy."""
        obs_tensor = torch.tensor(obs).unsqueeze(0).float()
        if hasattr(self.ppo_model, 'device') and self.ppo_model.device is not None:
            obs_tensor = obs_tensor.to(self.ppo_model.device)

        dist = self.ppo_model.policy.get_distribution(obs_tensor)
        probs = dist.distribution.probs.cpu().detach().numpy()[0]
        confidence = float(np.max(probs))
        entropy = float(-np.sum(probs * np.log(probs + 1e-8)))

        return {
            'confidence': confidence,
            'entropy': entropy,
            'probs': probs.tolist(),
        }


def evaluate_hybrid_policy(model_path, data_path, vec_normalize_path, n_episodes=50):
    """
    Evaluate the hybrid policy and return comprehensive metrics.
    """
    logger.info(f"Evaluating hybrid policy over {n_episodes} episodes...")

    # Load model and env
    device = "cuda" if torch.cuda.is_available() else "cpu"
    ppo_model = PPO.load(model_path, device=device)

    # Load VecNormalize for observation normalization
    vec_env = DummyVecEnv([lambda: DebtCollectionEnv(data_path)])
    vec_env = VecNormalize.load(vec_normalize_path, vec_env)
    vec_env.training = False
    vec_env.norm_reward = False

    # Create base env with manual normalization
    base_env = DebtCollectionEnv(data_path)

    # Set up normalization wrapper
    obs_rms = vec_env.obs_rms if hasattr(vec_env, 'obs_rms') else None

    def _norm_obs(obs):
        if obs_rms is None:
            return obs.astype(np.float32)
        mean = obs_rms.mean
        var = obs_rms.var
        if hasattr(mean, 'detach'):
            mean = mean.detach().cpu().numpy()
        if hasattr(var, 'detach'):
            var = var.detach().cpu().numpy()
        clipped = np.clip((obs - mean) / np.sqrt(var + 1e-8), -10.0, 10.0)
        return clipped.astype(np.float32)

    # Create hybrid policy
    hybrid = HybridPolicy(ppo_model, vec_env)

    # Run evaluation
    df = base_env.df
    all_steps = []
    total_reward = 0
    n_successful = 0
    action_counts = {a: 0 for a in ACTION_NAMES}
    policy_usage = {'rule_based': 0, 'ppo': 0}
    risk_metrics = {}

    for ep in range(n_episodes):
        reset_result = base_env.reset()
        obs = reset_result[0] if isinstance(reset_result, tuple) else reset_result
        raw_obs = base_env._get_obs()  # Get raw observation before normalization

        while True:
            current_idx = base_env.current_row
            borrower_row = df.iloc[current_idx]
            risk = borrower_row.get('risk_category', 'Unknown')

            # Get action from hybrid policy
            action, confidence, policy_used = hybrid.predict(
                raw_obs, df, current_idx, deterministic=True
            )
            action_name = ACTION_NAMES[action]
            action_counts[action_name] += 1
            policy_usage[policy_used] += 1

            # Step environment
            step_result = base_env.step(action)
            next_obs = step_result[0]
            reward_val = step_result[1]
            done = step_result[2]
            truncated = step_result[3] if len(step_result) > 3 else False

            total_reward += reward_val
            if reward_val > 0:
                n_successful += 1

            step_record = {
                'episode': ep,
                'risk_category': risk,
                'action': action_name,
                'action_index': action,
                'reward': reward_val,
                'total_demand': borrower_row.get('total_demand', 0),
                'confidence': confidence,
                'policy_used': policy_used,
            }
            all_steps.append(step_record)

            # Per-risk metrics
            if risk not in risk_metrics:
                risk_metrics[risk] = {
                    'total_reward': 0, 'n_decisions': 0, 'n_successful': 0,
                    'action_counts': {a: 0 for a in ACTION_NAMES},
                }
            risk_metrics[risk]['total_reward'] += reward_val
            risk_metrics[risk]['n_decisions'] += 1
            if reward_val > 0:
                risk_metrics[risk]['n_successful'] += 1
            risk_metrics[risk]['action_counts'][action_name] += 1

            raw_obs = base_env._get_obs()
            obs = next_obs

            if done or truncated:
                break

    # Compile results
    n_total = len(all_steps)
    total_cost_estimate = n_total * 100
    net_reward = total_reward

    results = {
        'policy': 'Hybrid (Rule-Based + PPO)',
        'n_episodes': n_episodes,
        'n_total_decisions': n_total,
        'n_successful_recoveries': n_successful,
        'total_reward': round(total_reward, 2),
        'net_reward': round(net_reward, 2),
        'roi': round((total_reward - total_cost_estimate) / (total_cost_estimate + 1), 4),
        'cost_per_recovery': round(total_cost_estimate / (n_successful + 1), 2),
        'pct_debts_resolved': round(n_successful / n_total * 100, 2),
        'avg_reward_per_decision': round(net_reward / n_total, 2),
        'action_distribution': action_counts,
        'action_distribution_pct': {
            a: round(c / n_total * 100, 1) for a, c in action_counts.items()
        },
        'policy_usage': {
            'rule_based': policy_usage['rule_based'],
            'ppo': policy_usage['ppo'],
            'rule_based_pct': round(policy_usage['rule_based'] / n_total * 100, 1),
            'ppo_pct': round(policy_usage['ppo'] / n_total * 100, 1),
        },
        'per_risk_metrics': {},
        'per_step_details': all_steps,
    }

    for risk, metrics in risk_metrics.items():
        results['per_risk_metrics'][risk] = {
            'n_decisions': metrics['n_decisions'],
            'total_reward': round(metrics['total_reward'], 2),
            'avg_reward': round(metrics['total_reward'] / (metrics['n_decisions'] + 1), 2),
            'pct_resolved': round(metrics['n_successful'] / metrics['n_decisions'] * 100, 2),
            'action_distribution': metrics['action_counts'],
        }

    # Log results
    logger.info(f"  Total decisions: {n_total}")
    logger.info(f"  Successful recoveries: {n_successful} ({results['pct_debts_resolved']}%)")
    logger.info(f"  Net reward: Rs. {net_reward:,.2f}")
    logger.info(f"  Avg reward/decision: Rs. {results['avg_reward_per_decision']:,.2f}")
    logger.info(f"  Policy usage: {results['policy_usage']}")

    # Per-risk breakdown
    logger.info(f"\n  Per-Risk Performance:")
    for risk, metrics in results['per_risk_metrics'].items():
        logger.info(f"    {risk:12s}: reward=Rs. {metrics['total_reward']:>12,}  "
                     f"avg=Rs. {metrics['avg_reward']:>8,}  "
                     f"resolved={metrics['pct_resolved']:.1f}%")

    return results


def demo_hybrid_policy(model_path, data_path, vec_normalize_path, n_samples=10):
    """
    Demonstrate hybrid policy on sample borrowers.
    """
    logger.info(f"Demo: Hybrid Policy on {n_samples} sample borrowers")

    device = "cuda" if torch.cuda.is_available() else "cpu"
    ppo_model = PPO.load(model_path, device=device)

    vec_env = DummyVecEnv([lambda: DebtCollectionEnv(data_path)])
    vec_env = VecNormalize.load(vec_normalize_path, vec_env)
    vec_env.training = False
    vec_env.norm_reward = False

    hybrid = HybridPolicy(ppo_model, vec_env)

    # Sample borrowers across risk categories
    df = pd.read_csv(data_path)
    risk_categories = df['risk_category'].unique()

    samples = []
    for risk in risk_categories:
        risk_df = df[df['risk_category'] == risk]
        if len(risk_df) > 0:
            samples.append(risk_df.sample(1, random_state=42).iloc[0])

    # Fill up to n_samples
    while len(samples) < n_samples:
        samples.append(df.sample(1, random_state=len(samples)).iloc[0])

    logger.info(f"\n  {'='*100}")
    logger.info(f"  {'Risk':<12} {'Overdue':>8} {'Demand':>12} {'Action':<15} {'Policy':<12} {'Confidence':>10}")
    logger.info(f"  {'='*100}")

    for _, borrower in enumerate(samples):
        risk = borrower['risk_category']
        overdue = borrower['overdue_months']
        demand = borrower['total_demand']

        # Find index in original df
        idx = borrower.name if borrower.name in df.index else df.index[0]

        # Get observation
        feature_cols = [c for c in df.columns if c not in [
            'customer_id', 'risk_category', 'occupation', 'region',
            'qualification', 'pending_status', 'last_call_status',
            'collector_id', 'coll_tier', 'total_demand', 'risk_label',
            'community_id'
        ] and pd.api.types.is_numeric_dtype(df[c])]
        obs = np.array([borrower.get(c, 0.0) for c in feature_cols], dtype=np.float32)

        action, confidence, policy_used = hybrid.predict(obs, df, idx, deterministic=True)

        logger.info(f"  {risk:<12} {overdue:>8} {demand:>12,.0f} {ACTION_NAMES[action]:<15} "
                     f"{policy_used:<12} {confidence:>10.1%}")


def main():
    parser = argparse.ArgumentParser(description="Hybrid Policy: Rule-Based + PPO")
    parser.add_argument("--evaluate", action="store_true", help="Run full evaluation")
    parser.add_argument("--demo", action="store_true", help="Show sample decisions")
    parser.add_argument("--episodes", type=int, default=50, help="Number of evaluation episodes")
    parser.add_argument("--model", type=str, default="graph_rl_debt_model.zip", help="PPO model path")
    parser.add_argument("--data", type=str, default="rl_ready_with_graph_features.csv", help="Data path")
    parser.add_argument("--vecnorm", type=str, default="vec_normalize.pkl", help="VecNormalize path")
    args = parser.parse_args()

    if not args.evaluate and not args.demo:
        args.evaluate = True  # Default to evaluation

    if args.demo:
        demo_hybrid_policy(args.model, args.data, args.vecnorm)

    if args.evaluate:
        start_time = time.time()
        results = evaluate_hybrid_policy(args.model, args.data, args.vecnorm, n_episodes=args.episodes)

        # Save results
        output_dir = "evaluation_outputs"
        os.makedirs(output_dir, exist_ok=True)

        results_no_steps = {k: v for k, v in results.items() if k != 'per_step_details'}
        with open(os.path.join(output_dir, "hybrid_evaluation_results.json"), 'w') as f:
            json.dump(results_no_steps, f, indent=2, default=str)

        pd.DataFrame(results['per_step_details']).to_csv(
            os.path.join(output_dir, "hybrid_evaluation_steps.csv"), index=False
        )

        elapsed = time.time() - start_time
        logger.info(f"\n  Hybrid evaluation complete in {elapsed:.1f}s")
        logger.info(f"  Results saved to: {output_dir}/hybrid_evaluation_results.json")


if __name__ == "__main__":
    main()
