"""
Explainable Inference Demo
===========================
Real-time explainable inference for any borrower.

Run from project root:
  python xai/xai_inference.py
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import warnings
import numpy as np
import pandas as pd
import torch
import shap

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
DEMO_DIR = os.path.join(OUTPUT_DIR, "borrower_explanations")

os.makedirs(DEMO_DIR, exist_ok=True)

print("=" * 70)
print("EXPLAINABLE INFERENCE DEMO")
print("=" * 70)


def _manual_norm(env, obs):
    """Normalize observations without CUDA tensor issues."""
    obs_rms = env.obs_rms
    mean = obs_rms.mean
    var = obs_rms.var
    if hasattr(mean, 'cpu'): mean = mean.cpu().numpy()
    if hasattr(var, 'cpu'): var = var.cpu().numpy()
    if hasattr(obs, 'cpu'): obs = obs.cpu().numpy()
    return np.clip((obs - mean) / np.sqrt(var + env.epsilon), -env.clip_obs, env.clip_obs).astype(np.float32)


def load_model_and_data():
    """Load trained model and environment."""
    print("\nLoading model and data...")
    base_env = DebtCollectionEnv(DATA_PATH)
    env = DummyVecEnv([lambda: base_env])
    env = VecNormalize.load(VEC_NORM_PATH, env)
    env.training = False
    env.norm_reward = False
    model = PPO.load(MODEL_PATH, device="cpu")
    print(f"  Model loaded: {MODEL_PATH}")
    print(f"  Borrowers: {len(base_env.df)}")
    return model, env, base_env


def load_attention_weights():
    """Load GAT attention weights if available."""
    if os.path.exists(ATTENTION_PATH):
        return pd.read_csv(ATTENTION_PATH)
    return None


def build_shap_explainer(model, env, base_env, n_background=50):
    """Build a SHAP KernelExplainer for the policy."""
    print("  Building SHAP explainer...")
    df = base_env.df
    feature_cols = base_env.feature_cols

    bg_indices = np.random.choice(len(df), size=min(n_background, len(df)), replace=False)
    background_obs = df.iloc[bg_indices][feature_cols].values.astype(np.float32)

    def policy_fn(obs_batch):
        obs_tensor = torch.tensor(obs_batch, dtype=torch.float32).to(model.device)
        obs_norm = _manual_norm(env, obs_tensor)
        dist = model.policy.get_distribution(obs_norm)
        return dist.distribution.probs.cpu().detach().numpy()

    explainer = shap.KernelExplainer(policy_fn, background_obs)
    print("  SHAP explainer ready.")
    return explainer, feature_cols


def get_top_neighbors(borrower_idx, attn_df, df, k=5):
    """Get the top-k most influential neighbors for a borrower."""
    if attn_df is None:
        return []

    neighbors = attn_df[attn_df['target_node'] == borrower_idx]
    if len(neighbors) == 0:
        return []

    top_k = neighbors.nlargest(k, 'attention_weight')
    result = []
    for _, row in top_k.iterrows():
        src = int(row['source_node'])
        if src < len(df):
            borrower_row = df.iloc[src]
            result.append({
                'borrower_id': borrower_row.get('customer_id', f'idx_{src}'),
                'risk_category': borrower_row.get('risk_category', 'Unknown'),
                'overdue_months': borrower_row.get('overdue_months', 0),
                'attention_weight': round(float(row['attention_weight']), 4),
            })
    return result


def generate_counterfactual_fast(model, env, obs, feature_cols, target_action, original_obs):
    """Fast counterfactual using Nelder-Mead optimization."""
    from scipy.optimize import minimize

    feature_constraints = {
        'age': (0, 20),
        'income': (-0.5, 1.0),
        'cibil_score': (0, 200),
        'overdue_months': (-12, 0),
        'bounce_count': (-10, 0),
        'coll_success_rate': (-0.3, 0.3),
        'neighborhood_stress_1hop': (-10, 0),
        'neighborhood_stress_2hop': (-10, 0),
        'neighborhood_stress_3hop': (-10, 0),
        'node_degree': (-10, 10),
        'pagerank': (-0.01, 0.01),
        'betweenness': (-0.01, 0.01),
        'community_risk_pct': (-50, 0),
        'community_avg_overdue': (-10, 0),
        'community_total_demand': (-10000, 0),
        'community_size': (-10, 10),
    }
    default_constraint = (-1.0, 1.0)

    def objective(x_flat):
        x_tensor = torch.tensor(x_flat, dtype=torch.float32).unsqueeze(0).to(model.device)
        x_norm = _manual_norm(env, x_tensor)
        dist = model.policy.get_distribution(x_norm)
        cf_probs = dist.distribution.probs.cpu().detach().numpy()[0]
        loss = -np.log(cf_probs[target_action] + 1e-8)
        loss += 0.05 * np.sum(np.abs(x_flat - original_obs))

        for fi, fname in enumerate(feature_cols):
            if fname in feature_constraints:
                min_d, max_d = feature_constraints[fname]
                delta = x_flat[fi] - original_obs[fi]
                if delta < min_d:
                    loss += 10.0 * (delta - min_d) ** 2
                elif delta > max_d:
                    loss += 10.0 * (delta - max_d) ** 2
        return float(loss)

    result = minimize(objective, x0=original_obs.copy(), method='Nelder-Mead',
                      options={'maxiter': 300, 'xatol': 1e-4, 'fatol': 1e-4})

    if not result.success:
        return None

    x_cf = result.x
    cf_tensor = torch.tensor(x_cf, dtype=torch.float32).unsqueeze(0).to(model.device)
    cf_norm = _manual_norm(env, cf_tensor)
    dist = model.policy.get_distribution(cf_norm)
    cf_probs = dist.distribution.probs.cpu().detach().numpy()[0]
    cf_action = int(np.argmax(cf_probs))

    changes = {}
    for fi, fname in enumerate(feature_cols):
        diff = x_cf[fi] - original_obs[fi]
        if abs(diff) > 1e-3:
            changes[fname] = {
                'original': round(float(original_obs[fi]), 3),
                'counterfactual': round(float(x_cf[fi]), 3),
                'change': round(float(diff), 3),
            }

    return {
        'target_action': ACTION_NAMES[target_action],
        'achieved_action': ACTION_NAMES[cf_action],
        'success': cf_action == target_action,
        'achieved_probability': round(float(cf_probs[target_action]), 4),
        'feature_changes': changes,
    }


def explain_borrower(borrower_idx, model, env, base_env, shap_explainer,
                     feature_cols, attn_df):
    """
    Generate a complete explanation for a single borrower.

    Returns a dict with all explanation components.
    """
    df = base_env.df
    borrower_row = df.iloc[borrower_idx]
    obs = borrower_row[feature_cols].values.astype(np.float32)

    # -- 1. Agent decision + confidence --
    conf = base_env.get_action_probs(model, obs, deterministic=True)
    action = conf['action']
    action_name = ACTION_NAMES[action]

    # -- 2. SHAP explanation --
    shap_vals = shap_explainer.shap_values(obs.reshape(1, -1), nsamples=50, l1_reg='auto')
    action_shap = shap_vals[action][0] if isinstance(shap_vals, list) else shap_vals[0]

    # Sort features by |SHAP| contribution
    sorted_features = np.argsort(np.abs(action_shap))[::-1]
    top_features = []
    for fi in sorted_features[:5]:
        top_features.append({
            'feature': feature_cols[fi],
            'value': round(float(obs[fi]), 3),
            'shap_contribution': round(float(action_shap[fi]), 4),
            'direction': 'toward this decision' if action_shap[fi] > 0 else 'away from this decision',
        })

    # -- 3. Influential neighbors (GAT attention) --
    top_neighbors = get_top_neighbors(borrower_idx, attn_df, df, k=5)

    # -- 4. Counterfactual --
    # Find target action (second best)
    sorted_actions = np.argsort(conf['probs'])[::-1]
    counterfactual = None
    if len(sorted_actions) >= 2:
        target_action = sorted_actions[1]
        counterfactual = generate_counterfactual_fast(
            model, env, obs, feature_cols, target_action, obs
        )

    # -- 5. Uncertainty flag --
    human_review_needed = conf['is_uncertain'] or conf['confidence'] < 0.4

    # -- Assemble explanation --
    explanation = {
        'borrower_id': borrower_row.get('customer_id', f'idx_{borrower_idx}'),
        'borrower_idx': int(borrower_idx),
        'profile': {
            'risk_category': borrower_row.get('risk_category', 'Unknown'),
            'age': int(borrower_row.get('age', 0)),
            'occupation': borrower_row.get('occupation', 'Unknown'),
            'region': borrower_row.get('region', 'Unknown'),
            'income': float(borrower_row.get('income', 0)),
            'cibil_score': int(borrower_row.get('cibil_score', 0)),
            'overdue_months': int(borrower_row.get('overdue_months', 0)),
            'bounce_count': int(borrower_row.get('bounce_count', 0)),
            'total_demand': float(borrower_row.get('total_demand', 0)),
        },
        'recommendation': {
            'action': action_name,
            'confidence': round(conf['confidence'], 4),
            'entropy': round(conf['entropy'], 4),
            'margin': round(conf['margin'], 4),
            'is_uncertain': conf['is_uncertain'],
            'human_review_needed': human_review_needed,
            'action_probabilities': {
                ACTION_NAMES[i]: round(float(conf['probs'][i]), 4) for i in range(4)
            },
        },
        'explanation': {
            'top_5_features': top_features,
            'top_influential_neighbors': top_neighbors,
            'counterfactual': counterfactual,
        },
    }

    return explanation


def print_explanation(explanation):
    """Print a nicely formatted explanation."""
    p = explanation['profile']
    r = explanation['recommendation']
    e = explanation['explanation']

    print("\n" + "=" * 70)
    print(f"EXPLANATION FOR BORROWER: {explanation['borrower_id']}")
    print("=" * 70)

    print(f"\n  Profile:")
    print(f"    Risk Category:    {p['risk_category']}")
    print(f"    Age:              {p['age']}")
    print(f"    Occupation:       {p['occupation']}")
    print(f"    Region:           {p['region']}")
    print(f"    Income:           ₹{p['income']:,.0f}")
    print(f"    CIBIL Score:      {p['cibil_score']}")
    print(f"    Overdue Months:   {p['overdue_months']}")
    print(f"    Bounce Count:     {p['bounce_count']}")
    print(f"    Total Demand:     ₹{p['total_demand']:,.2f}")

    print(f"\n  RL Agent Decision:")
    print(f"    ┌--------------------------------------┐")
    print(f"    │  RECOMMENDED ACTION: {r['action']:<18s}│")
    print(f"    │  Confidence: {r['confidence']*100:.1f}%{'':25s}│")
    if r['human_review_needed']:
        print(f"    │  [WARN]  FLAGGED FOR HUMAN REVIEW           │")
    print(f"    └--------------------------------------┘")

    print(f"\n  Action Probabilities:")
    for action_name, prob in r['action_probabilities'].items():
        bar = '█' * int(prob * 30)
        marker = '  ← CHOSEN' if action_name == r['action'] else ''
        print(f"    {action_name:<15s} {prob:6.1%}  {bar}{marker}")

    print(f"\n  Why this decision (SHAP):")
    for i, feat in enumerate(e['top_5_features']):
        direction_symbol = '▲' if feat['shap_contribution'] > 0 else '▼'
        print(f"    {direction_symbol} [{feat['shap_contribution']:+.4f}] {feat['feature']}={feat['value']}")

    if e['top_influential_neighbors']:
        print(f"\n  Influential Neighbors (GAT Attention):")
        for i, nb in enumerate(e['top_influential_neighbors']):
            print(f"    {i+1}. {nb['borrower_id']} (α={nb['attention_weight']:.3f}, "
                  f"risk={nb['risk_category']}, overdue={nb['overdue_months']})")

    if e['counterfactual']:
        cf = e['counterfactual']
        print(f"\n  Counterfactual:")
        if cf['success']:
            print(f"    \"If the borrower's profile changed as below, the agent would")
            print(f"     choose {cf['target_action']} instead of {r['action']}.\"")
            if cf['feature_changes']:
                top_change = list(cf['feature_changes'].items())[0]
                fname, fdata = top_change
                print(f"    Minimum change: {fname}: {fdata['original']:.1f} → {fdata['counterfactual']:.1f}")
        else:
            print(f"    Could not find a realistic counterfactual that changes the decision.")

    print(f"\n  Uncertainty Metrics:")
    print(f"    Entropy: {r['entropy']:.4f} (max={np.log(4):.4f})")
    print(f"    Margin:  {r['margin']:.4f}")
    print(f"    Human Review: {'YES [WARN]' if r['human_review_needed'] else 'No [OK]'}")

    print("=" * 70)


def main():
    model, env, base_env = load_model_and_data()
    attn_df = load_attention_weights()
    shap_explainer, feature_cols = build_shap_explainer(model, env, base_env, n_background=50)

    df = base_env.df

    # -- Demo: Explain 5 randomly selected borrowers --
    # Ensure we pick diverse borrowers (one from each risk category if possible)
    risk_categories = df['risk_category'].unique()
    demo_indices = []
    for risk in risk_categories:
        subset = df[df['risk_category'] == risk].index
        if len(subset) > 0:
            demo_indices.append(np.random.choice(subset))
        if len(demo_indices) >= 5:
            break

    # Fill remaining with random
    while len(demo_indices) < 5:
        idx = np.random.randint(0, len(df))
        if idx not in demo_indices:
            demo_indices.append(idx)

    demo_indices = demo_indices[:5]

    print(f"\nExplaining 5 borrowers across risk categories...")

    all_explanations = []
    for i, idx in enumerate(demo_indices):
        print(f"\n{'='*70}")
        print(f"  BORROWER {i+1}/5")
        print(f"{'='*70}")

        explanation = explain_borrower(idx, model, env, base_env, shap_explainer,
                                        feature_cols, attn_df)
        all_explanations.append(explanation)
        print_explanation(explanation)

        # Save individual explanation
        borrower_id = explanation['borrower_id'].replace('/', '_').replace('\\', '_')
        path = os.path.join(DEMO_DIR, f"{borrower_id}_explanation.json")
        with open(path, 'w') as f:
            json.dump(explanation, f, indent=2, default=str)
        print(f"  Saved: {path}")

    # Save all explanations in one file
    all_path = os.path.join(OUTPUT_DIR, "borrower_explanations_all.json")
    with open(all_path, 'w') as f:
        json.dump(all_explanations, f, indent=2, default=str)

    print("\n" + "=" * 70)
    print("EXPLAINABLE INFERENCE COMPLETE")
    print("=" * 70)
    print(f"\n  Individual explanations: {DEMO_DIR}/")
    print(f"  All explanations: {all_path}")
    print(f"\n  To use in production, call explain_borrower(idx, ...) for any borrower.")


if __name__ == "__main__":
    main()
