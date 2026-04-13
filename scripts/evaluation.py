"""
Evaluation Module
==================
Train/test split, metrics, ablation study, confusion matrix, baselines.

Run from project root:
  python scripts/evaluation.py
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time
import warnings
import numpy as np
import pandas as pd
import torch
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix, classification_report
from tqdm import tqdm

import gymnasium as gym
from stable_baselines3 import PPO
from stable_baselines3.common.vec_env import DummyVecEnv, VecNormalize

from scripts.debt_env import DebtCollectionEnv, ACTION_NAMES
from scripts.hybrid_policy import HybridPolicy, evaluate_hybrid_policy
from config import cfg, logger

warnings.filterwarnings('ignore')

# -- Configuration --
DATA_PATH = "rl_ready_with_graph_features.csv"
OUTPUT_DIR = "evaluation_outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Ground truth "optimal" actions per risk category (from domain knowledge)
OPTIMAL_ACTIONS = {
    'Very Low': {'SMS/Call': 0.6, 'No Action': 0.3, 'Field Visit': 0.1, 'Legal Notice': 0.0},
    'Low':      {'SMS/Call': 0.5, 'No Action': 0.2, 'Field Visit': 0.25, 'Legal Notice': 0.05},
    'Medium':   {'SMS/Call': 0.3, 'No Action': 0.1, 'Field Visit': 0.4, 'Legal Notice': 0.2},
    'High':     {'SMS/Call': 0.1, 'No Action': 0.0, 'Field Visit': 0.5, 'Legal Notice': 0.4},
    'Very High':{'SMS/Call': 0.05, 'No Action': 0.0, 'Field Visit': 0.45, 'Legal Notice': 0.5},
}

# Severity scores for actions
ACTION_SEVERITY = {0: 0, 1: 1, 2: 2, 3: 3}

print("=" * 70)
print("EVALUATION MODULE")
print("=" * 70)


# ==============================================================================
# 1. TRAIN/TEST SPLIT
# ==============================================================================
def create_train_test_split(df_path, train_ratio=None, output_dir=OUTPUT_DIR):
    """
    Create stratified train/test split by risk category.
    Saves two separate CSV files.

    Returns: (train_df, test_df)
    """
    if train_ratio is None:
        train_ratio = cfg.eval_train_ratio

    logger.info(f"Creating train/test split (ratio={train_ratio})")
    df = pd.read_csv(df_path)

    # Stratified split by risk_category
    train_df, test_df = train_test_split(
        df, test_size=(1 - train_ratio),
        stratify=df['risk_category'],
        random_state=42
    )

    train_path = os.path.join(output_dir, "train_data.csv")
    test_path = os.path.join(output_dir, "test_data.csv")
    train_df.to_csv(train_path, index=False)
    test_df.to_csv(test_path, index=False)

    logger.info(f"  Train: {len(train_df)} borrowers → {train_path}")
    logger.info(f"  Test:  {len(test_df)} borrowers → {test_path}")
    logger.info(f"  Risk distribution preserved:")
    for risk in sorted(df['risk_category'].unique()):
        orig_pct = (df['risk_category'] == risk).mean() * 100
        train_pct = (train_df['risk_category'] == risk).mean() * 100
        test_pct = (test_df['risk_category'] == risk).mean() * 100
        logger.info(f"    {risk:12s}: orig={orig_pct:.1f}% train={train_pct:.1f}% test={test_pct:.1f}%")

    return train_df, test_df


# ==============================================================================
# 2. EVALUATION METRICS
# ==============================================================================
def evaluate_agent_comprehensive(model, env, base_env, n_episodes=None):
    """
    Run comprehensive evaluation with detailed metrics.
    Uses base_env directly (not VecEnv) for clean step tracking.
    """
    if n_episodes is None:
        n_episodes = cfg.eval_n_episodes

    logger.info(f"Evaluating agent over {n_episodes} episodes...")

    df = base_env.df
    feature_cols = base_env.feature_cols

    all_steps = []
    total_recovered = 0
    total_cost = 0
    n_successful = 0
    action_counts = {a: 0 for a in ACTION_NAMES}
    risk_metrics = {}

    for ep in range(n_episodes):
        reset_result = base_env.reset()
        obs = reset_result[0] if isinstance(reset_result, tuple) else reset_result
        ep_steps = 0

        while True:
            action, _ = model.predict(obs, deterministic=True)
            action = int(np.asarray(action).item()) if np.asarray(action).ndim == 0 else int(action[0])

            # Apply risk-aware action masking
            borrower_row = df.iloc[base_env.current_row]
            if hasattr(base_env, 'apply_action_mask'):
                obs_raw = base_env._get_obs()
                conf_data = base_env.get_action_probs(model, obs_raw, deterministic=True)
                probs = np.array(conf_data['probs'])
                masked_probs, mask = base_env.apply_action_mask(probs, borrower_row)
                action = int(np.argmax(masked_probs))

            action_name = ACTION_NAMES[action]
            action_counts[action_name] += 1

            # Get borrower info
            risk = borrower_row.get('risk_category', 'Unknown')
            total_demand = borrower_row.get('total_demand', 0)

            # Get confidence
            conf = base_env.get_action_probs(model, obs, deterministic=True)

            # Step
            step_result = base_env.step(action)
            next_obs = step_result[0]
            reward_val = step_result[1]
            done = step_result[2]
            truncated = step_result[3] if len(step_result) > 3 else False

            # Parse reward to extract recovery and cost
            # reward = recovery - cost - penalties + bonuses
            # We track reward directly
            if reward_val > 0:
                total_recovered += reward_val
                n_successful += 1

            step_record = {
                'episode': ep,
                'step': ep_steps,
                'borrower_id': borrower_row.get('customer_id', 'unknown'),
                'risk_category': risk,
                'action': action_name,
                'action_index': action,
                'reward': reward_val,
                'total_demand': total_demand,
                'confidence': conf['confidence'],
                'entropy': conf['entropy'],
                'is_uncertain': conf['is_uncertain'],
            }
            all_steps.append(step_record)

            # Per-risk accumulation
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

            obs = next_obs
            ep_steps += 1

            if done or truncated:
                break

    n_total = len(all_steps)
    total_cost_estimate = n_total * 100  # rough average cost per action
    net_reward = sum(s['reward'] for s in all_steps)

    results = {
        'n_episodes': n_episodes,
        'n_total_decisions': n_total,
        'n_successful_recoveries': n_successful,
        'total_recovered': round(total_recovered, 2),
        'total_cost_estimate': round(total_cost_estimate, 2),
        'net_reward': round(net_reward, 2),
        'roi': round((total_recovered - total_cost_estimate) / (total_cost_estimate + 1), 4),
        'cost_per_recovery': round(total_cost_estimate / (n_successful + 1), 2),
        'pct_debts_resolved': round(n_successful / n_total * 100, 2),
        'avg_reward_per_decision': round(net_reward / n_total, 2),
        'action_distribution': action_counts,
        'action_distribution_pct': {
            a: round(c / n_total * 100, 1) for a, c in action_counts.items()
        },
        'confidence_stats': {
            'avg_confidence': round(np.mean([s['confidence'] for s in all_steps]), 4),
            'avg_entropy': round(np.mean([s['entropy'] for s in all_steps]), 4),
            'pct_uncertain': round(
                sum(1 for s in all_steps if s['is_uncertain']) / n_total * 100, 2
            ),
        },
        'per_risk_metrics': {},
        'per_step_details': all_steps,
    }

    # Per-risk breakdown
    for risk, metrics in risk_metrics.items():
        results['per_risk_metrics'][risk] = {
            'n_decisions': metrics['n_decisions'],
            'total_reward': round(metrics['total_reward'], 2),
            'avg_reward': round(metrics['total_reward'] / (metrics['n_decisions'] + 1), 2),
            'pct_resolved': round(metrics['n_successful'] / metrics['n_decisions'] * 100, 2),
            'action_distribution': metrics['action_counts'],
        }

    logger.info(f"  Total decisions: {n_total}")
    logger.info(f"  Successful recoveries: {n_successful} ({results['pct_debts_resolved']}%)")
    logger.info(f"  Net reward: {net_reward:.2f}")
    logger.info(f"  ROI: {results['roi']:.2f}")
    logger.info(f"  Avg confidence: {results['confidence_stats']['avg_confidence']:.2%}")

    return results


# ==============================================================================
# 3. ABLATION STUDY
# ==============================================================================
def run_ablation_study(df_path, n_episodes=20):
    """
    Train multiple agents with different feature subsets and compare.

    Feature subsets tested:
      1. baseline: core features only (income, cibil, overdue, etc.) — no graph
      2. +graph: baseline + single neighborhood_stress_signal (original approach)
      3. +structural: baseline + node_degree, pagerank, betweenness
      4. +community: baseline + community_risk_pct, community_avg_overdue
      5. +multihop: baseline + 1-hop, 2-hop, 3-hop signals
      6. +all_graph: all graph features combined
      7. +full: all features including GAT embeddings (complete system)

    Returns: comparison DataFrame
    """
    logger.info("=" * 60)
    logger.info("ABLATION STUDY: Testing feature subsets")
    logger.info("=" * 60)

    df = pd.read_csv(df_path)
    device = "cuda" if torch.cuda.is_available() else "cpu"

    # Define feature subsets
    core_features = ['income', 'cibil_score', 'overdue_months', 'bounce_count',
                     'coll_success_rate', 'age', 'occ_idx', 'reg_idx']

    ablation_configs = {
        'baseline': core_features,
        '+graph': core_features + ['neighborhood_stress_signal'],
        '+structural': core_features + ['node_degree', 'pagerank', 'betweenness'],
        '+community': core_features + ['community_risk_pct', 'community_avg_overdue'],
        '+multihop': core_features + ['neighborhood_stress_1hop', 'neighborhood_stress_2hop',
                                       'neighborhood_stress_3hop'],
        '+all_graph': core_features + [c for c in df.columns if
                                        c in ['neighborhood_stress_signal', 'node_degree',
                                               'pagerank', 'betweenness', 'community_risk_pct',
                                               'community_avg_overdue', 'neighborhood_stress_1hop',
                                               'neighborhood_stress_2hop', 'neighborhood_stress_3hop']],
        '+full': None,  # Use all available features
    }

    # Filter configs to only use features that exist in the data
    available_configs = {}
    for name, features in ablation_configs.items():
        if features is None:
            available_configs[name] = None  # All features
        else:
            available = [f for f in features if f in df.columns]
            missing = set(features) - set(available)
            if missing:
                logger.warning(f"  Config '{name}': missing features {missing}, skipping")
                continue
            available_configs[name] = available

    results = []

    for config_name, feature_subset in available_configs.items():
        logger.info(f"\n  Training with config: {config_name}")

        if feature_subset is None:
            # Use all numeric columns that aren't identifiers or targets
            exclude_cols = ['customer_id', 'risk_category', 'occupation', 'region',
                            'qualification', 'pending_status', 'last_call_status',
                            'collector_id', 'coll_tier', 'total_demand',
                            'risk_label', 'community_id']
            feature_subset = [c for c in df.columns
                              if c not in exclude_cols and pd.api.types.is_numeric_dtype(df[c])]
            label = "full (all)"
        else:
            label = config_name

        n_features = len(feature_subset)
        logger.info(f"    Features: {n_features} — {feature_subset[:5]}...")

        # Create custom environment with subset
        class SubsetEnv(DebtCollectionEnv):
            def __init__(self, df_path):
                super().__init__(df_path)
                self.feature_cols = [c for c in feature_subset if c in self.df.columns]
                self.observation_space = gym.spaces.Box(
                    low=-np.inf, high=np.inf, shape=(len(self.feature_cols),), dtype=np.float32
                )

            def _get_obs(self):
                row = self.df.iloc[self.current_row]
                return np.array([row.get(c, 0.0) for c in self.feature_cols], dtype=np.float32)

        # Use 100K timesteps for ablation (enough for relative rankings, 5x faster than full 500K)
        ablation_timesteps = min(100000, cfg.rl_total_timesteps)
        try:
            base_env = SubsetEnv(df_path)
            env = DummyVecEnv([lambda: base_env])
            env = VecNormalize(env, norm_obs=True, norm_reward=True, clip_obs=10.0)

            model = PPO("MlpPolicy", env, verbose=0,
                        learning_rate=cfg.rl_learning_rate,
                        n_steps=512,
                        device=device)
            model.learn(total_timesteps=ablation_timesteps)

            # Evaluate
            env.training = False
            env.norm_reward = False
            eval_results = evaluate_agent_comprehensive(model, env, base_env, n_episodes=n_episodes)

            results.append({
                'feature_set': label,
                'n_features': n_features,
                'net_reward': eval_results['net_reward'],
                'avg_reward_per_decision': eval_results['avg_reward_per_decision'],
                'pct_debts_resolved': eval_results['pct_debts_resolved'],
                'roi': eval_results['roi'],
                'avg_confidence': eval_results['confidence_stats']['avg_confidence'],
                'n_episodes': n_episodes,
            })

            logger.info(f"    ✓ Net reward: {eval_results['net_reward']:.2f} | "
                        f"ROI: {eval_results['roi']:.2f} | "
                        f"Resolved: {eval_results['pct_debts_resolved']:.1f}%")

        except Exception as e:
            logger.error(f"    ✗ Failed: {e}")
            results.append({
                'feature_set': label,
                'n_features': n_features,
                'net_reward': 0,
                'avg_reward_per_decision': 0,
                'pct_debts_resolved': 0,
                'roi': 0,
                'avg_confidence': 0,
                'error': str(e),
            })

    results_df = pd.DataFrame(results)
    results_df.to_csv(os.path.join(OUTPUT_DIR, "ablation_study.csv"), index=False)

    logger.info(f"\n  Ablation Results:")
    logger.info(f"  {'='*80}")
    logger.info(f"  {'Feature Set':<20} {'Features':>9} {'Net Reward':>12} {'ROI':>8} {'Resolved%':>10}")
    logger.info(f"  {'-'*80}")
    for _, row in results_df.iterrows():
        logger.info(f"  {row['feature_set']:<20} {row['n_features']:>9} "
                     f"{row['net_reward']:>12.2f} {row['roi']:>8.2f} {row['pct_debts_resolved']:>9.1f}%")

    # Plot
    plt.figure(figsize=(10, 6))
    x = range(len(results_df))
    plt.bar(x, results_df['net_reward'], color='#3498db', edgecolor='white')
    plt.xticks(x, results_df['feature_set'], rotation=30, ha='right')
    plt.ylabel('Net Reward', fontsize=12)
    plt.title('Ablation Study: Impact of Feature Subsets on Performance', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "ablation_study.png"), dpi=200, bbox_inches='tight')
    plt.close()
    logger.info(f"  Saved: ablation_study.png")

    return results_df


# ==============================================================================
# 4. CONFUSION MATRIX ANALYSIS
# ==============================================================================
def compute_confusion_matrix(eval_results):
    """
    Compare agent's action distribution per risk category against
    the "optimal" distribution from domain knowledge.

    Also computes a "correctness" score: how often the agent picks one of
    the top-2 optimal actions for each risk category.

    Returns: dict with confusion matrix, correctness scores, and plot
    """
    logger.info("\n" + "=" * 60)
    logger.info("CONFUSION MATRIX ANALYSIS")
    logger.info("=" * 60)

    steps = eval_results['per_step_details']
    df_steps = pd.DataFrame(steps)

    if len(df_steps) == 0:
        logger.warning("  No steps to analyze.")
        return None

    # -- Confusion Matrix: Risk Category vs Chosen Action --
    risk_order = ['Very Low', 'Low', 'Medium', 'High', 'Very High']
    existing_risks = [r for r in risk_order if r in df_steps['risk_category'].unique()]

    cm = pd.crosstab(
        df_steps['risk_category'].astype('category').cat.reorder_categories(existing_risks),
        df_steps['action'].astype('category').cat.reorder_categories(ACTION_NAMES),
    ).reindex(index=existing_risks, columns=ACTION_NAMES, fill_value=0)

    # Convert to percentages
    cm_pct = cm.div(cm.sum(axis=1), axis=0) * 100

    # -- Correctness: Is the chosen action among the top-2 optimal? --
    def get_top_2_optimal(risk):
        if risk in OPTIMAL_ACTIONS:
            sorted_actions = sorted(OPTIMAL_ACTIONS[risk].items(), key=lambda x: -x[1])
            return {a for a, _ in sorted_actions[:2]}
        return set()

    df_steps['optimal_top2'] = df_steps['risk_category'].apply(get_top_2_optimal)
    df_steps['is_optimal'] = df_steps.apply(
        lambda row: row['action'] in row['optimal_top2'], axis=1
    )

    accuracy_by_risk = df_steps.groupby('risk_category')['is_optimal'].mean() * 100
    overall_accuracy = df_steps['is_optimal'].mean() * 100

    logger.info(f"\n  Overall 'Correctness' (top-2 optimal action): {overall_accuracy:.1f}%")
    logger.info(f"\n  Correctness by Risk Category:")
    for risk, acc in accuracy_by_risk.items():
        logger.info(f"    {risk:12s}: {acc:.1f}%")

    logger.info(f"\n  Confusion Matrix (action % by risk category):")
    logger.info(f"  {cm_pct.round(1).to_string()}")

    # -- Plot --
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # Left: Confusion matrix heatmap
    ax1 = axes[0]
    sns.heatmap(cm_pct, annot=True, fmt='.1f', cmap='Blues', ax=ax1,
                cbar_kws={'label': 'Percentage (%)'})
    ax1.set_xlabel('Chosen Action', fontsize=12)
    ax1.set_ylabel('Risk Category', fontsize=12)
    ax1.set_title('Action Distribution by Risk Category (%)', fontsize=13, fontweight='bold')

    # Right: Correctness bar chart
    ax2 = axes[1]
    colors_acc = ['#2ecc71' if v >= 70 else '#f39c12' if v >= 50 else '#e74c3c'
                  for v in accuracy_by_risk.values]
    ax2.bar(accuracy_by_risk.index, accuracy_by_risk.values, color=colors_acc, edgecolor='white')
    ax2.axhline(y=overall_accuracy, color='red', linestyle='--', linewidth=2,
                label=f'Overall: {overall_accuracy:.1f}%')
    ax2.set_xlabel('Risk Category', fontsize=12)
    ax2.set_ylabel('Correctness (%)', fontsize=12)
    ax2.set_title('Agent Picks Top-2 Optimal Action', fontsize=13, fontweight='bold')
    ax2.legend()
    ax2.set_ylim(0, 105)

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "confusion_matrix.png"), dpi=200, bbox_inches='tight')
    plt.close()
    logger.info(f"  Saved: confusion_matrix.png")

    # -- Save data --
    cm_pct.to_csv(os.path.join(OUTPUT_DIR, "confusion_matrix_pct.csv"))
    accuracy_by_risk.to_csv(os.path.join(OUTPUT_DIR, "correctness_by_risk.csv"))

    return {
        'confusion_matrix': cm,
        'confusion_matrix_pct': cm_pct,
        'correctness_by_risk': accuracy_by_risk.to_dict(),
        'overall_correctness': float(overall_accuracy),
    }


# ==============================================================================
# 5. BASELINE COMPARISON
# ==============================================================================
def run_baselines(df_path, n_episodes=20):
    """
    Compare PPO agent against:
      1. Random policy
      2. Rule-based heuristic (based on risk category)
      3. Hybrid policy (Rule-based for Very Low/Low, PPO for Medium/High/Very High)

    Returns: comparison dict
    """
    logger.info("\n" + "=" * 60)
    logger.info("BASELINE COMPARISON")
    logger.info("=" * 60)

    # Rule-based heuristic
    RULE_BASED_POLICY = {
        'Very Low': 1,   # SMS/Call
        'Low': 1,         # SMS/Call
        'Medium': 2,      # Field Visit
        'High': 2,        # Field Visit
        'Very High': 3,   # Legal Notice
    }

    def evaluate_policy(policy_fn, label, df_path, n_episodes):
        """Evaluate a fixed policy function using base env directly."""
        base_env_eval = DebtCollectionEnv(df_path)
        total_reward = 0
        n_decisions = 0
        action_counts = {a: 0 for a in ACTION_NAMES}

        for ep in range(n_episodes):
            reset_result = base_env_eval.reset()
            obs = reset_result[0] if isinstance(reset_result, tuple) else reset_result
            while True:
                action = policy_fn(base_env_eval.df.iloc[base_env_eval.current_row], obs, base_env_eval)
                action_counts[ACTION_NAMES[action]] += 1
                step_result = base_env_eval.step(action)
                next_obs = step_result[0]
                reward_val = step_result[1]
                done = step_result[2]
                truncated = step_result[3] if len(step_result) > 3 else False
                total_reward += reward_val
                n_decisions += 1
                obs = next_obs
                if done or truncated:
                    break

        return {
            'policy': label,
            'n_episodes': n_episodes,
            'n_decisions': n_decisions,
            'net_reward': round(total_reward, 2),
            'avg_reward': round(total_reward / n_decisions, 2),
            'action_distribution': action_counts,
        }

    # Random policy
    random_results = evaluate_policy(
        lambda row, obs, env: np.random.randint(0, 4),
        "Random", df_path, n_episodes
    )

    # Rule-based policy
    def rule_policy(row, obs, env):
        risk = row.get('risk_category', 'Medium')
        return RULE_BASED_POLICY.get(risk, 2)

    rule_results = evaluate_policy(rule_policy, "Rule-Based", df_path, n_episodes)

    logger.info(f"\n  Random Policy:    Net reward={random_results['net_reward']:.2f}, "
                f"Avg={random_results['avg_reward']:.2f}")
    logger.info(f"  Rule-Based Policy: Net reward={rule_results['net_reward']:.2f}, "
                f"Avg={rule_results['avg_reward']:.2f}")

    return {
        'random': random_results,
        'rule_based': rule_results,
    }


# ==============================================================================
# PLOT: Learning Curve (single run)
# ==============================================================================
def plot_learning_curve(model, output_path=None):
    """Plot the learning curve from a trained PPO model."""
    if output_path is None:
        output_path = os.path.join(OUTPUT_DIR, "learning_curve.png")

    try:
        # Extract episode rewards from the model's logger
        if hasattr(model, 'logger') and model.logger is not None:
            # This works if the model was trained with verbose > 0
            logger_data = model.logger.name_to_value
            if 'rollout/ep_rew_mean' in logger_data:
                rewards = logger_data['rollout/ep_rew_mean']
                plt.figure(figsize=(10, 5))
                plt.plot(rewards, color='#3498db', linewidth=2)
                plt.xlabel('Episode', fontsize=12)
                plt.ylabel('Episode Reward', fontsize=12)
                plt.title('Learning Curve', fontsize=14, fontweight='bold')
                plt.grid(True, alpha=0.3)
                plt.tight_layout()
                plt.savefig(output_path, dpi=200, bbox_inches='tight')
                plt.close()
                logger.info(f"  Saved: learning_curve.png")
                return True
    except Exception as e:
        logger.warning(f"  Could not plot learning curve: {e}")
    return False


# ==============================================================================
# MAIN: Full Evaluation Pipeline
# ==============================================================================
def main():
    start_time = time.time()

    # -- Step 1: Train/Test Split --
    train_df, test_df = create_train_test_split(DATA_PATH)

    # -- Step 2: Evaluate on test set --
    logger.info("\n" + "=" * 60)
    logger.info("EVALUATING ON TEST SET")
    logger.info("=" * 60)

    base_env = DebtCollectionEnv(DATA_PATH)  # Use full data for model compatibility

    # Load VecNormalize stats for observation normalization
    vec_env = DummyVecEnv([lambda: DebtCollectionEnv(DATA_PATH)])
    vec_env = VecNormalize.load("vec_normalize.pkl", vec_env)
    vec_env.training = False
    vec_env.norm_reward = False

    model = PPO.load("graph_rl_debt_model.zip",
                     device="cuda" if torch.cuda.is_available() else "cpu")

    # Manual observation normalization using VecNormalize stats
    _vec_ref = vec_env
    _obs_rms = _vec_ref.obs_rms if hasattr(_vec_ref, 'obs_rms') else None

    def _norm_obs(obs):
        """Manually normalize using saved mean/var from VecNormalize."""
        if _obs_rms is None:
            return obs.astype(np.float32)
        # Get mean/var as numpy (handle both numpy and tensor storage)
        mean = _obs_rms.mean
        var = _obs_rms.var
        if hasattr(mean, 'detach'):
            mean = mean.detach().cpu().numpy()
        if hasattr(var, 'detach'):
            var = var.detach().cpu().numpy()
        clipped_obs = np.clip((obs - mean) / np.sqrt(var + 1e-8), -10.0, 10.0)
        return clipped_obs.astype(np.float32)

    # Store originals on the instance
    base_env._vec_env = vec_env
    base_env._norm_obs_fn = _norm_obs

    # Override reset and _get_obs to return normalized obs
    _orig_reset = base_env.__class__.reset
    _orig_get_obs = base_env.__class__._get_obs

    def _wrapped_reset(seed=None, options=None):
        result = _orig_reset(base_env, seed=seed, options=options)
        obs = result[0] if isinstance(result, tuple) else result
        return _norm_obs(obs), {}

    def _wrapped_get_obs():
        obs = _orig_get_obs(base_env)
        return _norm_obs(obs)

    base_env.reset = _wrapped_reset
    base_env._get_obs = _wrapped_get_obs

    eval_results = evaluate_agent_comprehensive(model, None, base_env, n_episodes=cfg.eval_n_episodes)

    # Save evaluation results
    eval_output = {k: v for k, v in eval_results.items() if k != 'per_step_details'}
    with open(os.path.join(OUTPUT_DIR, "evaluation_results.json"), 'w') as f:
        json.dump(eval_output, f, indent=2, default=str)
    pd.DataFrame(eval_results['per_step_details']).to_csv(
        os.path.join(OUTPUT_DIR, "evaluation_steps.csv"), index=False
    )

    # -- Step 3: Confusion Matrix --
    cm_results = compute_confusion_matrix(eval_results)

    # -- Step 4: Baselines --
    baseline_results = run_baselines(DATA_PATH, n_episodes=20)

    # -- Step 5: Hybrid Policy Evaluation --
    logger.info("\n" + "=" * 60)
    logger.info("HYBRID POLICY EVALUATION (Rule-Based + PPO)")
    logger.info("=" * 60)
    try:
        hybrid_results = evaluate_hybrid_policy(
            model_path="graph_rl_debt_model.zip",
            data_path=DATA_PATH,
            vec_normalize_path="vec_normalize.pkl",
            n_episodes=cfg.eval_n_episodes,
        )
        # Save hybrid results
        hybrid_output = {k: v for k, v in hybrid_results.items() if k != 'per_step_details'}
        with open(os.path.join(OUTPUT_DIR, "hybrid_evaluation_results.json"), 'w') as f:
            json.dump(hybrid_output, f, indent=2, default=str)
        pd.DataFrame(hybrid_results['per_step_details']).to_csv(
            os.path.join(OUTPUT_DIR, "hybrid_evaluation_steps.csv"), index=False
        )
        logger.info(f"  Hybrid policy: Net reward=Rs. {hybrid_results['net_reward']:,.2f}, "
                     f"Avg=Rs. {hybrid_results['avg_reward_per_decision']:,.2f}/decision")
    except Exception as e:
        logger.error(f"  Hybrid policy evaluation failed: {e}")
        hybrid_results = None

    # -- Step 6: Ablation Study --
    ablation_results = run_ablation_study(DATA_PATH, n_episodes=20)

    # -- Summary --
    elapsed = time.time() - start_time

    logger.info("\n" + "=" * 70)
    logger.info("EVALUATION COMPLETE")
    logger.info("=" * 70)
    logger.info(f"  Time elapsed: {elapsed:.1f}s")
    logger.info(f"\n  Outputs saved to: {OUTPUT_DIR}/")
    logger.info(f"  +━ train_data.csv")
    logger.info(f"  +━ test_data.csv")
    logger.info(f"  +━ evaluation_results.json")
    logger.info(f"  +━ evaluation_steps.csv")
    logger.info(f"  +━ confusion_matrix.png")
    logger.info(f"  +━ confusion_matrix_pct.csv")
    logger.info(f"  +━ correctness_by_risk.csv")
    logger.info(f"  +━ ablation_study.csv")
    logger.info(f"  +━ ablation_study.png")
    logger.info(f"  +━ baseline_comparison.csv")
    if hybrid_results is not None:
        logger.info(f"  +━ hybrid_evaluation_results.json")
        logger.info(f"  +━ hybrid_evaluation_steps.csv")

    # Save baseline comparison
    baseline_df = pd.DataFrame([baseline_results['random'], baseline_results['rule_based']])
    baseline_df.to_csv(os.path.join(OUTPUT_DIR, "baseline_comparison.csv"), index=False)

    # Save combined summary
    summary = {
        'evaluation': {k: v for k, v in eval_results.items() if k != 'per_step_details'},
        'confusion_matrix': cm_results,
        'baselines': baseline_results,
        'hybrid_policy': hybrid_output if hybrid_results is not None else None,
        'ablation': ablation_results.to_dict('records') if ablation_results is not None else [],
    }
    with open(os.path.join(OUTPUT_DIR, "full_evaluation_summary.json"), 'w') as f:
        json.dump(summary, f, indent=2, default=str)


if __name__ == "__main__":
    main()
