"""
Explainable AI Analysis Module
==============================
Contains:
  1. SHAP Analysis on RL decisions
  2. Counterfactual Explanations
  3. Permutation Feature Importance
  4. Trajectory (Episode-Level) Analysis with per-step explanations

Run from project root:
  python xai/xai_analysis.py
"""

import os
import sys
# Add project root to path so imports work from any subdirectory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import warnings
import numpy as np
import pandas as pd
import torch
import matplotlib.pyplot as plt
from tqdm import tqdm
import shap
from scipy.optimize import minimize

from stable_baselines3 import PPO
from stable_baselines3.common.vec_env import DummyVecEnv, VecNormalize

from scripts.debt_env import DebtCollectionEnv, ACTION_NAMES
from config import cfg, logger

warnings.filterwarnings('ignore')

# -- Configuration --
MODEL_PATH = "graph_rl_debt_model.zip"
VEC_NORM_PATH = "vec_normalize.pkl"
DATA_PATH = "rl_ready_with_graph_features.csv"
ATTENTION_PATH = "gat_attention_weights.csv"
OUTPUT_DIR = "xai_outputs"
N_SHAP_SAMPLES = 200      # Number of borrowers to run SHAP on
N_BACKGROUND = 100        # Background samples for SHAP
N_CF_SAMPLES = 50          # Number of counterfactuals to generate
N_TRAJECTORIES = 5        # Number of episodes to analyze

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(os.path.join(OUTPUT_DIR, "trajectory_reports"), exist_ok=True)

print("=" * 70)
print("EXPLAINABLE AI (XAI) ANALYSIS MODULE")
print("=" * 70)


def _manual_normalize_obs(env, obs):
    """Normalize observations without using VecNormalize.normalize_obs() to avoid CUDA tensor issues."""
    obs_rms = env.obs_rms
    mean = obs_rms.mean
    var = obs_rms.var
    if hasattr(mean, 'cpu'):
        mean = mean.cpu().numpy()
    if hasattr(var, 'cpu'):
        var = var.cpu().numpy()
    if hasattr(obs, 'cpu'):
        obs = obs.cpu().numpy()
    result = np.clip((obs - mean) / np.sqrt(var + env.epsilon), -env.clip_obs, env.clip_obs)
    return result.astype(np.float32)


# ==============================================================================
# UTILITY: Load trained model + VecNormalize
# ==============================================================================
def load_trained_model():
    """Load the trained PPO model with VecNormalize stats."""
    print("\n[1/4] Loading trained model...")

    base_env = DebtCollectionEnv(DATA_PATH)
    env = DummyVecEnv([lambda: base_env])
    env = VecNormalize.load(VEC_NORM_PATH, env)
    env.training = False
    env.norm_reward = False  # Don't normalize rewards during inference

    # Load on CPU — SHAP needs thousands of forward passes,
    # CPU avoids CUDA tensor → numpy conversion issues
    model = PPO.load(MODEL_PATH, device="cpu")

    print(f"  Model loaded from {MODEL_PATH}")
    print(f"  VecNormalize stats loaded from {VEC_NORM_PATH}")
    print(f"  Observation space: {env.observation_space.shape}")

    return model, env, base_env


# ==============================================================================
# UTILITY: Get policy action probabilities
# ==============================================================================
def get_action_probs(model, obs_tensor):
    """Get action probabilities from the PPO policy."""
    dist = model.policy.get_distribution(obs_tensor)
    probs = dist.distribution.probs
    if hasattr(probs, 'cpu'):
        probs = probs.cpu()
    return probs.detach().numpy()


# ==============================================================================
# 1. SHAP ANALYSIS
# ==============================================================================
def run_shap_analysis(model, env, base_env):
    """
    Run KernelSHAP on the PPO policy to decompose each decision into
    feature contributions.

    Outputs:
      - xai_outputs/shap_summary_plot.png
      - xai_outputs/shap_summary_beeswarm.png
      - xai_outputs/shap_per_decision.json
      - xai_outputs/shap_global_importance.csv
    """
    print("\n" + "=" * 70)
    print("[2/4] Running SHAP Analysis on RL Policy")
    print("=" * 70)

    df = base_env.df
    feature_cols = base_env.feature_cols
    n_features = len(feature_cols)

    # -- Prepare background data --
    print(f"  Selecting {N_BACKGROUND} background samples...")
    bg_indices = np.random.choice(len(df), size=min(N_BACKGROUND, len(df)), replace=False)
    background_obs = df.iloc[bg_indices][feature_cols].values.astype(np.float32)

    # -- Prepare test samples --
    print(f"  Selecting {N_SHAP_SAMPLES} test samples...")
    test_indices = np.random.choice(len(df), size=min(N_SHAP_SAMPLES, len(df)), replace=False)
    test_obs = df.iloc[test_indices][feature_cols].values.astype(np.float32)

    # -- Define policy wrapper for SHAP --
    # SHAP needs a function that takes (n_samples, n_features) -> (n_samples, n_actions)
    def policy_fn(obs_batch):
        # Manual normalization to avoid CUDA tensor -> numpy issues
        obs_rms = env.obs_rms
        mean = obs_rms.mean
        var = obs_rms.var
        if hasattr(mean, 'cpu'):
            mean = mean.cpu().numpy()
        if hasattr(var, 'cpu'):
            var = var.cpu().numpy()
        obs_norm = np.clip((obs_batch - mean) / np.sqrt(var + env.epsilon), -env.clip_obs, env.clip_obs)
        obs_norm_tensor = torch.tensor(obs_norm, dtype=torch.float32).to(model.device)
        dist = model.policy.get_distribution(obs_norm_tensor)
        probs = dist.distribution.probs.cpu().detach().numpy()
        return probs

    # -- Run KernelSHAP --
    # With 146 features, 'auto' tries to enumerate 2^146 coalitions and runs OOM.
    # Use explicit nsamples=500 for a good speed/accuracy trade-off.
    n_shap_samples = min(500, 2 ** min(n_features, 20))
    print(f"  Running KernelSHAP ({n_features} features, {N_SHAP_SAMPLES} test samples, {n_shap_samples} SHAP samples)...")
    print("  This may take a few minutes...")

    explainer = shap.KernelExplainer(policy_fn, background_obs)
    shap_values = explainer.shap_values(test_obs, nsamples=n_shap_samples, l1_reg='auto')

    # shap_values is a list of arrays (one per action), each shape (n_samples, n_features)
    # We analyze the CHOSEN action for each sample

    # -- Compute chosen action for each test sample --
    test_tensor = torch.tensor(test_obs, dtype=torch.float32)
    # Manual normalization (avoids VecNormalize CUDA tensor issues)
    obs_rms = env.obs_rms
    mean = obs_rms.mean.cpu().numpy() if hasattr(obs_rms.mean, 'cpu') else obs_rms.mean
    var = obs_rms.var.cpu().numpy() if hasattr(obs_rms.var, 'cpu') else obs_rms.var
    test_norm = np.clip((test_obs - mean) / np.sqrt(var + env.epsilon), -env.clip_obs, env.clip_obs)
    test_norm_tensor = torch.tensor(test_norm, dtype=torch.float32).to(model.device)
    dist = model.policy.get_distribution(test_norm_tensor)
    probs = dist.distribution.probs.cpu().detach().numpy()
    chosen_actions = np.argmax(probs, axis=1)

    # -- Global Feature Importance (mean |SHAP| value across all actions) --
    all_shap = np.array(shap_values)  # shape: (n_actions, n_samples, n_features)
    shap_n_samples = all_shap.shape[1]
    shap_n_features = all_shap.shape[2]
    global_importance = np.mean(np.abs(all_shap), axis=(0, 1))

    # Align feature_cols and test_obs to actual SHAP dimensions
    if shap_n_features != len(feature_cols):
        logger.warning(f"  SHAP has {shap_n_features} features, env has {len(feature_cols)}. Truncating to match.")
        feature_cols = feature_cols[:shap_n_features]

    # Use the actual SHAP feature count for test_obs
    test_obs = test_obs[:, :shap_n_features]

    # Verify row counts match
    if test_obs.shape[0] != shap_n_samples:
        logger.warning(f"  test_obs rows ({test_obs.shape[0]}) != SHAP rows ({shap_n_samples}). Aligning.")
        min_rows = min(test_obs.shape[0], shap_n_samples)
        test_obs = test_obs[:min_rows]
        all_shap = all_shap[:, :min_rows, :]
        shap_values = [sv[:min_rows] for sv in shap_values]
        # Also slice probs and chosen_actions to match
        probs = probs[:min_rows]
        chosen_actions = chosen_actions[:min_rows]
        shap_n_samples = min_rows

    n_features = len(feature_cols)
    n_valid_samples = shap_n_samples
    logger.info(f"  Final SHAP dimensions: {all_shap.shape} (actions={all_shap.shape[0]}, samples={all_shap.shape[1]}, features={all_shap.shape[2]})")

    importance_df = pd.DataFrame({
        'feature': feature_cols,
        'mean_abs_shap': global_importance[:n_features],
        'importance_pct': global_importance[:n_features] / global_importance[:n_features].sum() * 100,
    }).sort_values('mean_abs_shap', ascending=False).reset_index(drop=True)

    importance_df.to_csv(os.path.join(OUTPUT_DIR, "shap_global_importance.csv"), index=False)
    print(f"\n  Global Feature Importance (top 10):")
    for i, row in importance_df.head(10).iterrows():
        print(f"    {row['feature']:35s} {row['importance_pct']:6.2f}%")

    # -- SHAP Summary Plot (Beeswarm) --
    # Use the SHAP library's built-in visualization
    # We'll use the action with highest mean probability
    mean_probs = np.mean(probs, axis=0)
    target_action = np.argmax(mean_probs)
    shap_values_target = shap_values[target_action]

    plt.figure(figsize=(12, 8))
    shap.summary_plot(
        shap_values_target, test_obs,
        feature_names=feature_cols,
        plot_type="dot",
        show=False,
        color=plt.cm.RdBu_r,
    )
    plt.title(f"SHAP Summary for Action: {ACTION_NAMES[target_action]}", fontweight='bold', fontsize=14)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "shap_summary_beeswarm.png"), dpi=200, bbox_inches='tight')
    plt.close()
    print(f"  Saved: shap_summary_beeswarm.png")

    # -- SHAP Bar Plot --
    plt.figure(figsize=(12, 8))
    shap.summary_plot(
        shap_values_target, test_obs,
        feature_names=feature_cols,
        plot_type="bar",
        show=False,
    )
    plt.title(f"Mean |SHAP| Value by Feature — {ACTION_NAMES[target_action]}",
              fontweight='bold', fontsize=14)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "shap_summary_bar.png"), dpi=200, bbox_inches='tight')
    plt.close()
    print(f"  Saved: shap_summary_bar.png")

    # -- Per-Decision Explanations --
    print(f"  Saving per-decision explanations for {n_valid_samples} samples...")
    per_decision = []
    for idx in range(n_valid_samples):
        borrower_id = df.iloc[test_indices[idx]].get('customer_id', f'idx_{test_indices[idx]}')
        risk = df.iloc[test_indices[idx]].get('risk_category', 'Unknown')
        action = chosen_actions[idx]
        action_prob = probs[idx][action]

        # Top 5 contributing features (by |SHAP| value)
        shap_vals = all_shap[:, idx, :]  # shape: (n_actions, n_features)
        # Aggregate: for the chosen action, get SHAP values
        action_shap = shap_vals[action]
        top_features_idx = np.argsort(np.abs(action_shap))[::-1][:5]
        top_features = []
        for fi in top_features_idx:
            top_features.append({
                'feature': feature_cols[fi],
                'value': float(test_obs[idx, fi]),
                'shap_contribution': float(action_shap[fi]),
                'direction': 'toward' if action_shap[fi] > 0 else 'away',
            })

        per_decision.append({
            'borrower_id': borrower_id,
            'risk_category': risk,
            'chosen_action': ACTION_NAMES[action],
            'action_probability': float(action_prob),
            'all_probabilities': {ACTION_NAMES[a]: float(probs[idx][a]) for a in range(4)},
            'top_5_features': top_features,
        })

    with open(os.path.join(OUTPUT_DIR, "shap_per_decision.json"), 'w') as f:
        json.dump(per_decision, f, indent=2)
    print(f"  Saved: shap_per_decision.json")

    print("  SHAP Analysis complete.")
    return importance_df, per_decision


# ==============================================================================
# 2. COUNTERFACTUAL EXPLANATIONS
# ==============================================================================
def generate_counterfactuals(model, env, base_env, n_samples=N_CF_SAMPLES):
    """
    For each sampled borrower, find the minimal feature change that would
    flip the agent's decision to a different action.

    Outputs:
      - xai_outputs/counterfactuals.json
      - xai_outputs/counterfactuals_summary.csv
    """
    print("\n" + "=" * 70)
    print("[3/4] Generating Counterfactual Explanations")
    print("=" * 70)

    df = base_env.df
    feature_cols = base_env.feature_cols
    n_features = len(feature_cols)

    # -- Feature constraints for realistic counterfactuals --
    # (min_change, max_change) relative to current value
    feature_constraints = {
        'age': (0, 20),                    # Can only age, not reverse
        'income': (-0.5, 1.0),             # ±50% to 100%
        'cibil_score': (0, 200),           # Can only improve
        'overdue_months': (-12, 0),        # Can only decrease (pay off)
        'bounce_count': (-10, 0),          # Can only decrease
        'coll_success_rate': (-0.3, 0.3),  # Small change possible
        'neighborhood_stress_1hop': (-10, 0),  # Can only decrease
        'neighborhood_stress_2hop': (-10, 0),
        'neighborhood_stress_3hop': (-10, 0),
        'node_degree': (-10, 10),
        'pagerank': (-0.01, 0.01),
        'betweenness': (-0.01, 0.01),
        'community_risk_pct': (-50, 0),    # Can only decrease
        'community_avg_overdue': (-10, 0),
        'community_total_demand': (-10000, 0),
        'community_size': (-10, 10),
    }

    # Default constraint for features not listed (e.g., GAT embeddings, indices)
    default_constraint = (-1.0, 1.0)

    # -- Select samples --
    sample_indices = np.random.choice(len(df), size=min(n_samples, len(df)), replace=False)

    counterfactuals = []
    cf_summary_rows = []

    for idx in tqdm(sample_indices, desc="Generating counterfactuals", ncols=80):
        borrower_row = df.iloc[idx]
        borrower_id = borrower_row.get('customer_id', f'idx_{idx}')
        original_obs = borrower_row[feature_cols].values.astype(np.float32)

        # Get original action
        obs_tensor = torch.tensor(original_obs).unsqueeze(0).float().to(model.device)
        obs_norm = _manual_normalize_obs(env, obs_tensor)
        obs_norm_tensor = torch.tensor(obs_norm).float().to(model.device)
        original_probs = get_action_probs(model, obs_norm_tensor)[0]
        original_action = int(np.argmax(original_probs))

        # Target: second-best action
        sorted_actions = np.argsort(original_probs)[::-1]
        if len(sorted_actions) < 2:
            continue
        target_action = sorted_actions[1]

        # -- Optimize counterfactual --
        x_cf = original_obs.copy()

        def objective(x_flat):
            """Minimize: -log(P(target|x')) + λ * ||x' - x||_1"""
            x_tensor = torch.tensor(x_flat, dtype=torch.float32).unsqueeze(0).to(model.device)
            x_norm = _manual_normalize_obs(env, x_tensor)
            x_norm_tensor = torch.tensor(x_norm).float().to(model.device)
            dist = model.policy.get_distribution(x_norm_tensor)
            cf_probs = dist.distribution.probs.cpu().detach().numpy()[0]

            # Primary: maximize target action probability
            loss = -np.log(cf_probs[target_action] + 1e-8)

            # Regularization: minimize L1 distance from original
            l1_weight = 0.05
            loss += l1_weight * np.sum(np.abs(x_flat - original_obs))

            # Constraint penalty
            for fi, fname in enumerate(feature_cols):
                if fname in feature_constraints:
                    min_d, max_d = feature_constraints[fname]
                    delta = x_flat[fi] - original_obs[fi]
                    if delta < min_d:
                        loss += 10.0 * (delta - min_d) ** 2
                    elif delta > max_d:
                        loss += 10.0 * (delta - max_d) ** 2

            return float(loss)

        # Run optimization
        result = minimize(
            objective, x0=original_obs, method='Nelder-Mead',
            options={'maxiter': 500, 'xatol': 1e-4, 'fatol': 1e-4}
        )

        if not result.success:
            continue

        x_cf = result.x

        # Verify the counterfactual actually changes the decision
        cf_tensor = torch.tensor(x_cf, dtype=torch.float32).unsqueeze(0).to(model.device)
        cf_norm = _manual_normalize_obs(env, cf_tensor)
        cf_norm_tensor = torch.tensor(cf_norm).float().to(model.device)
        cf_probs = get_action_probs(model, cf_norm_tensor)[0]
        cf_action = int(np.argmax(cf_probs))

        # Compute feature changes
        changes = {}
        for fi, fname in enumerate(feature_cols):
            diff = x_cf[fi] - original_obs[fi]
            if abs(diff) > 1e-4:
                changes[fname] = {
                    'original': float(original_obs[fi]),
                    'counterfactual': float(x_cf[fi]),
                    'change': float(diff),
                    'change_pct': float(diff / (abs(original_obs[fi]) + 1e-8) * 100),
                }

        success = bool(cf_action == target_action)

        cf_record = {
            'borrower_id': borrower_id,
            'risk_category': borrower_row.get('risk_category', 'Unknown'),
            'original_action': ACTION_NAMES[original_action],
            'original_probs': {ACTION_NAMES[a]: float(original_probs[a]) for a in range(4)},
            'counterfactual_action': ACTION_NAMES[cf_action],
            'counterfactual_probs': {ACTION_NAMES[a]: float(cf_probs[a]) for a in range(4)},
            'target_action': ACTION_NAMES[target_action],
            'success': success,
            'n_features_changed': len(changes),
            'feature_changes': changes,
        }
        counterfactuals.append(cf_record)

        if success:
            cf_summary_rows.append({
                'borrower_id': borrower_id,
                'original_action': ACTION_NAMES[original_action],
                'counterfactual_action': ACTION_NAMES[cf_action],
                'n_features_changed': len(changes),
                'top_change_feature': max(changes, key=lambda k: abs(changes[k]['change'])) if changes else 'none',
            })

    # Save outputs
    with open(os.path.join(OUTPUT_DIR, "counterfactuals.json"), 'w') as f:
        json.dump(counterfactuals, f, indent=2)

    if cf_summary_rows:
        cf_summary_df = pd.DataFrame(cf_summary_rows)
        cf_summary_df.to_csv(os.path.join(OUTPUT_DIR, "counterfactuals_summary.csv"), index=False)

    success_rate = sum(1 for c in counterfactuals if c['success']) / len(counterfactuals) if counterfactuals else 0
    print(f"\n  Generated {len(counterfactuals)} counterfactuals")
    print(f"  Success rate: {success_rate:.1%}")
    if cf_summary_rows:
        avg_changes = pd.DataFrame(cf_summary_rows)['n_features_changed'].mean()
        print(f"  Avg features changed: {avg_changes:.1f}")

    print("  Counterfactual generation complete.")
    return counterfactuals


# ==============================================================================
# 3. PERMUTATION FEATURE IMPORTANCE
# ==============================================================================
def compute_feature_importance(model, env, base_env, n_episodes=30):
    """
    Measure how much each feature contributes to RL performance by
    shuffling one feature at a time and measuring reward degradation.

    Outputs:
      - xai_outputs/feature_importance.csv
      - xai_outputs/feature_importance_plot.png
    """
    print("\n" + "=" * 70)
    print("[3/4] Computing Permutation Feature Importance")
    print("=" * 70)

    df = base_env.df
    feature_cols = base_env.feature_cols

    # -- Baseline performance --
    print("  Computing baseline performance...")
    baseline_rewards = []
    for ep in range(n_episodes):
        obs = env.reset()
        total_reward = 0
        done = False
        while not done:
            action, _ = model.predict(obs, deterministic=True)
            obs, reward, done, _ = env.step(action)
            total_reward += float(reward[0]) if hasattr(reward, '__iter__') else float(reward)
        baseline_rewards.append(total_reward)
    baseline_mean = np.mean(baseline_rewards)
    print(f"  Baseline mean reward (over {n_episodes} episodes): {baseline_mean:.2f}")

    # -- Per-feature importance --
    print("  Shuffling features one at a time...")
    importance_results = []

    for col in tqdm(feature_cols, desc="Evaluating features", ncols=80):
        # Save original
        original_col = df[col].values.copy()

        # Shuffle
        df[col] = df[col].sample(frac=1, random_state=42).values

        # Evaluate
        shuffled_rewards = []
        for ep in range(n_episodes):
            obs = env.reset()
            total_reward = 0
            done = False
            while not done:
                action, _ = model.predict(obs, deterministic=True)
                obs, reward, done, _ = env.step(action)
                total_reward += float(reward[0]) if hasattr(reward, '__iter__') else float(reward)
            shuffled_rewards.append(total_reward)

        shuffled_mean = np.mean(shuffled_rewards)

        # Restore
        df[col] = original_col

        importance = baseline_mean - shuffled_mean
        importance_pct = (importance / (abs(baseline_mean) + 1e-8)) * 100

        importance_results.append({
            'feature': col,
            'baseline_reward': baseline_mean,
            'shuffled_reward': shuffled_mean,
            'importance': importance,
            'importance_pct': importance_pct,
        })

    importance_df = pd.DataFrame(importance_results)
    importance_df = importance_df.sort_values('importance', ascending=False).reset_index(drop=True)

    # Normalize to percentages
    total_importance = importance_df['importance'].sum()
    if total_importance > 0:
        importance_df['relative_importance'] = importance_df['importance'] / total_importance * 100
    else:
        importance_df['relative_importance'] = 0

    importance_df.to_csv(os.path.join(OUTPUT_DIR, "feature_importance.csv"), index=False)

    # -- Plot --
    plt.figure(figsize=(12, 8))
    sorted_df = importance_df.sort_values('importance', ascending=True)
    colors = ['green' if v > 0 else 'red' for v in sorted_df['importance']]
    plt.barh(sorted_df['feature'], sorted_df['importance'], color=colors)
    plt.xlabel('Reward Drop When Feature Shuffled', fontsize=12)
    plt.title('Permutation Feature Importance — RL Agent Performance', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "feature_importance_plot.png"), dpi=200, bbox_inches='tight')
    plt.close()

    print(f"\n  Top 10 most important features:")
    for _, row in importance_df.head(10).iterrows():
        print(f"    {row['feature']:35s} importance={row['importance']:+.2f} ({row.get('relative_importance', 0):.1f}%)")

    print("  Feature importance complete.")
    return importance_df


# ==============================================================================
# 4. TRAJECTORY ANALYSIS
# ==============================================================================
def analyze_trajectories(model, env, base_env, n_episodes=N_TRAJECTORIES):
    """
    Run full episodes and log per-step explanations including SHAP contributions.

    Outputs:
      - xai_outputs/trajectory_reports/episode_XX.csv
      - xai_outputs/trajectory_reports/episode_XX_summary.json
    """
    print("\n" + "=" * 70)
    print("[4/4] Analyzing Trajectories (Episode-Level Explanations)")
    print("=" * 70)

    df = base_env.df
    feature_cols = base_env.feature_cols

    # For SHAP in trajectory, we need a lightweight explainer
    # Use a subset for background
    bg_indices = np.random.choice(len(df), size=50, replace=False)
    background_obs = df.iloc[bg_indices][feature_cols].values.astype(np.float32)

    def policy_fn(obs_batch):
        obs_tensor = torch.tensor(obs_batch, dtype=torch.float32).to(model.device)
        obs_norm = _manual_normalize_obs(env, obs_tensor)
        obs_norm_tensor = torch.tensor(obs_norm).float().to(model.device)
        dist = model.policy.get_distribution(obs_norm_tensor)
        return dist.distribution.probs.cpu().detach().numpy()

    # Pre-build SHAP explainer (small background for speed)
    print("  Building lightweight SHAP explainer for trajectory analysis...")
    traj_explainer = shap.KernelExplainer(policy_fn, background_obs)

    all_trajectories = []

    for ep in range(n_episodes):
        print(f"\n  Episode {ep + 1}/{n_episodes}...")
        obs = env.reset()
        steps = []
        total_reward = 0
        action_counts = {a: 0 for a in ACTION_NAMES}
        uncertain_count = 0

        step_num = 0
        while True:
            # Get action and confidence
            conf_info = base_env.get_action_probs(model, obs, deterministic=True)
            action = conf_info['action']
            action_name = ACTION_NAMES[action]
            action_counts[action_name] = action_counts.get(action_name, 0) + 1

            if conf_info['is_uncertain']:
                uncertain_count += 1

            # Get borrower info
            borrower_row = df.iloc[base_env.current_row]
            borrower_id = borrower_row.get('customer_id', f'step_{step_num}')
            risk = borrower_row.get('risk_category', 'Unknown')

            # Compute SHAP for this step (quick, single-sample)
            shap_vals = traj_explainer.shap_values(obs.reshape(1, -1), nsamples=20, l1_reg=False)
            # Get SHAP for chosen action
            action_shap = shap_vals[action][0] if isinstance(shap_vals, list) else shap_vals[0]
            action_shap = action_shap.flatten()
            top_shap_idx = np.argsort(np.abs(action_shap))[::-1][:3]
            # Guard against feature index out of bounds
            top_shap_idx = [fi for fi in top_shap_idx if fi < len(feature_cols)]
            top_shap_features = [
                {
                    'feature': feature_cols[fi],
                    'value': float(obs[fi]) if fi < len(obs) else 0.0,
                    'contribution': float(action_shap[fi]) if fi < len(action_shap) else 0.0,
                }
                for fi in top_shap_idx
            ]

            # Step environment
            next_obs, reward, done, info = env.step(np.array([action]))
            truncated = info.get('truncated', False) if isinstance(info, dict) else False
            total_reward += float(reward[0]) if hasattr(reward, '__iter__') else float(reward)

            steps.append({
                'step': step_num,
                'borrower_id': borrower_id,
                'risk_category': risk,
                'action': action_name,
                'action_prob': conf_info['probs'],
                'confidence': conf_info['confidence'],
                'entropy': conf_info['entropy'],
                'is_uncertain': conf_info['is_uncertain'],
                'reward': reward,
                'cumulative_reward': total_reward,
                'top_3_shap_features': top_shap_features,
            })

            obs = next_obs
            step_num += 1

            if (done[0] if hasattr(done, '__iter__') else done) or truncated:
                break

        # Save trajectory
        traj_df = pd.DataFrame(steps)
        traj_path = os.path.join(OUTPUT_DIR, "trajectory_reports", f"episode_{ep+1:02d}.csv")
        traj_df.to_csv(traj_path, index=False)

        # Save summary
        traj_summary = {
            'episode': ep + 1,
            'total_steps': len(steps),
            'total_reward': float(total_reward),
            'action_distribution': action_counts,
            'uncertain_decisions': uncertain_count,
            'avg_confidence': float(np.mean([s['confidence'] for s in steps])),
            'avg_entropy': float(np.mean([s['entropy'] for s in steps])),
        }

        summary_path = os.path.join(OUTPUT_DIR, "trajectory_reports", f"episode_{ep+1:02d}_summary.json")
        with open(summary_path, 'w') as f:
            json.dump(traj_summary, f, indent=2)

        print(f"    Steps: {len(steps)} | Total Reward: {total_reward:.2f} | "
              f"Avg Confidence: {traj_summary['avg_confidence']:.2f} | "
              f"Uncertain: {uncertain_count}")

        all_trajectories.append(traj_summary)

    print(f"\n  Saved {n_episodes} trajectory reports to xai_outputs/trajectory_reports/")
    print("  Trajectory analysis complete.")
    return all_trajectories


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    # Load model
    model, env, base_env = load_trained_model()

    # 1. SHAP Analysis
    shap_importance, shap_decisions = run_shap_analysis(model, env, base_env)

    # 2. Counterfactual Explanations
    counterfactuals = generate_counterfactuals(model, env, base_env)

    # 3. Permutation Feature Importance
    perm_importance = compute_feature_importance(model, env, base_env, n_episodes=30)

    # 4. Trajectory Analysis
    trajectories = analyze_trajectories(model, env, base_env, n_episodes=5)

    # -- Final Summary --
    print("\n" + "=" * 70)
    print("XAI ANALYSIS COMPLETE")
    print("=" * 70)
    print(f"\n  All outputs saved to: {OUTPUT_DIR}/")
    print(f"  ┣━ shap_global_importance.csv")
    print(f"  ┣━ shap_summary_beeswarm.png")
    print(f"  ┣━ shap_summary_bar.png")
    print(f"  ┣━ shap_per_decision.json")
    print(f"  ┣━ counterfactuals.json")
    print(f"  ┣━ counterfactuals_summary.csv")
    print(f"  ┣━ feature_importance.csv")
    print(f"  ┣━ feature_importance_plot.png")
    print(f"  ┗━ trajectory_reports/")
    print(f"      ┣━ episode_01.csv + episode_01_summary.json")
    print(f"      ┣━ episode_02.csv + episode_02_summary.json")
    print(f"      ┗━ ...")


if __name__ == "__main__":
    main()
