"""
Fairness Audit Module
=====================
Audits the RL agent for demographic bias.

Run from project root:
  python xai/fairness_audit.py
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import warnings
import numpy as np
import pandas as pd
import torch
import matplotlib.pyplot as plt
from scipy import stats
from tqdm import tqdm

from stable_baselines3 import PPO
from stable_baselines3.common.vec_env import DummyVecEnv, VecNormalize

from scripts.debt_env import DebtCollectionEnv, ACTION_NAMES
from config import cfg, logger

warnings.filterwarnings('ignore')

# -- Configuration --
MODEL_PATH = "graph_rl_debt_model.zip"
VEC_NORM_PATH = "vec_normalize.pkl"
DATA_PATH = "rl_ready_with_graph_features.csv"
OUTPUT_DIR = "xai_outputs"

os.makedirs(OUTPUT_DIR, exist_ok=True)

ACTION_SEVERITY = {0: 0, 1: 1, 2: 2, 3: 3}  # No Action=0, Legal=3

print("=" * 70)
print("FAIRNESS AUDIT MODULE")
print("=" * 70)


def load_model_and_data():
    """Load trained model and data."""
    print("\nLoading model and data...")
    base_env = DebtCollectionEnv(DATA_PATH)
    env = DummyVecEnv([lambda: base_env])
    env = VecNormalize.load(VEC_NORM_PATH, env)
    env.training = False
    env.norm_reward = False
    model = PPO.load(MODEL_PATH, device="cpu")
    print(f"  Loaded {len(base_env.df)} borrowers")
    return model, env, base_env


def collect_agent_decisions(model, env, base_env):
    """Run agent on all borrowers and collect decisions + rewards."""
    print("\nCollecting agent decisions across all borrowers...")
    df = base_env.df
    feature_cols = base_env.feature_cols
    n = len(df)

    decisions = []
    total_reward = 0

    for i in tqdm(range(n), desc="Running agent", ncols=80):
        obs = df.iloc[i][feature_cols].values.astype(np.float32)
        conf = base_env.get_action_probs(model, obs, deterministic=True)
        action = conf['action']
        action_name = ACTION_NAMES[action]

        # Simulate one step to get reward
        base_env.current_row = i
        _, reward, _, _, _ = env.envs[0].step(action)

        decisions.append({
            'borrower_idx': i,
            'customer_id': df.iloc[i].get('customer_id', f'idx_{i}'),
            'occupation': df.iloc[i].get('occupation', 'Unknown'),
            'region': df.iloc[i].get('region', 'Unknown'),
            'age': df.iloc[i].get('age', 0),
            'income': df.iloc[i].get('income', 0),
            'risk_category': df.iloc[i].get('risk_category', 'Unknown'),
            'gender': df.iloc[i].get('gender', 'Unknown') if 'gender' in df.columns else None,
            'chosen_action': action_name,
            'action_index': action,
            'severity': ACTION_SEVERITY.get(action, 0),
            'confidence': conf['confidence'],
            'reward': reward,
        })

    decisions_df = pd.DataFrame(decisions)
    print(f"  Collected {len(decisions_df)} decisions")
    return decisions_df


# ==============================================================================
# TEST 1: Occupation Bias
# ==============================================================================
def test_occupation_bias(decisions_df):
    """Chi-squared test: Is action distribution independent of occupation?"""
    print("\n" + "=" * 70)
    print("TEST 1: Occupation Bias")
    print("=" * 70)

    results = {}

    # -- Contingency table: occupation × action --
    contingency = pd.crosstab(decisions_df['occupation'], decisions_df['chosen_action'])

    # -- Severity by occupation --
    severity_by_occ = decisions_df.groupby('occupation').agg(
        avg_severity=('severity', 'mean'),
        pct_legal=('severity', lambda x: (x == 3).mean() * 100),
        pct_field_visit=('severity', lambda x: (x == 2).mean() * 100),
        avg_reward=('reward', 'mean'),
        count=('severity', 'count'),
    ).round(2)

    print(f"\n  Action Severity by Occupation:")
    print(f"  {'='*85}")
    print(f"  {'Occupation':<25} {'Avg Severity':>12} {'% Legal':>10} {'% Field Visit':>14} {'Avg Reward':>12} {'Count':>8}")
    print(f"  {'-'*85}")
    for occ, row in severity_by_occ.iterrows():
        print(f"  {occ:<25} {row['avg_severity']:>12.2f} {row['pct_legal']:>9.1f}% "
              f"{row['pct_field_visit']:>13.1f}% {row['avg_reward']:>12.2f} {int(row['count']):>8}")

    # -- Chi-squared test --
    chi2, p_value, dof, expected = stats.chi2_contingency(contingency)
    results['occupation_chi2'] = {
        'chi2_stat': float(chi2),
        'p_value': float(p_value),
        'dof': int(dof),
        'significant': p_value < 0.05,
    }

    print(f"\n  Chi-squared test: χ²={chi2:.2f}, p={p_value:.6f}")
    if p_value < 0.001:
        print(f"  [WARN]  HIGHLY SIGNIFICANT (p<0.001): Action distribution strongly depends on occupation")
    elif p_value < 0.05:
        print(f"  [WARN]  SIGNIFICANT (p<0.05): Action distribution depends on occupation")
    else:
        print(f"  [OK]  NOT SIGNIFICANT (p={p_value:.3f}): No evidence of occupation bias")

    # -- Control for risk category --
    print(f"\n  Controlling for risk category...")
    risk_specific_pvals = {}
    for risk in decisions_df['risk_category'].unique():
        subset = decisions_df[decisions_df['risk_category'] == risk]
        if len(subset) < 10:
            continue
        cont_sub = pd.crosstab(subset['occupation'], subset['chosen_action'])
        if cont_sub.shape[0] < 2 or cont_sub.shape[1] < 2:
            continue
        try:
            _, p, _, _ = stats.chi2_contingency(cont_sub)
            risk_specific_pvals[risk] = float(p)
        except:
            pass

    results['occupation_controlled_for_risk'] = risk_specific_pvals

    if risk_specific_pvals:
        print(f"  P-values by risk category:")
        for risk, p in risk_specific_pvals.items():
            sig = "[WARN]" if p < 0.05 else "[OK]"
            print(f"    {sig} {risk}: p={p:.4f}")

    return results


# ==============================================================================
# TEST 2: Region Bias
# ==============================================================================
def test_region_bias(decisions_df):
    """ANOVA + Chi-squared: Is treatment different across regions?"""
    print("\n" + "=" * 70)
    print("TEST 2: Region Bias")
    print("=" * 70)

    results = {}

    # -- Severity by region --
    severity_by_region = decisions_df.groupby('region').agg(
        avg_severity=('severity', 'mean'),
        pct_legal=('severity', lambda x: (x == 3).mean() * 100),
        avg_reward=('reward', 'mean'),
        count=('severity', 'count'),
    ).round(2)

    print(f"\n  Action Severity by Region:")
    print(f"  {'='*70}")
    print(f"  {'Region':<10} {'Avg Severity':>12} {'% Legal':>10} {'Avg Reward':>12} {'Count':>8}")
    print(f"  {'-'*70}")
    for region, row in severity_by_region.iterrows():
        print(f"  {region:<10} {row['avg_severity']:>12.2f} {row['pct_legal']:>9.1f}% "
              f"{row['avg_reward']:>12.2f} {int(row['count']):>8}")

    # -- ANOVA: severity across regions --
    groups = [g['severity'].values for _, g in decisions_df.groupby('region')]
    if len(groups) >= 2:
        f_stat, p_value = stats.f_oneway(*groups)
        results['region_anova'] = {
            'f_statistic': float(f_stat),
            'p_value': float(p_value),
            'significant': p_value < 0.05,
        }
        print(f"\n  ANOVA: F={f_stat:.2f}, p={p_value:.6f}")
        if p_value < 0.05:
            print(f"  [WARN]  SIGNIFICANT: Rewards/severity differ across regions")
        else:
            print(f"  [OK]  NOT SIGNIFICANT: No evidence of region bias")

    # -- Chi-squared: action × region --
    contingency = pd.crosstab(decisions_df['region'], decisions_df['chosen_action'])
    try:
        chi2, p_chi, _, _ = stats.chi2_contingency(contingency)
        results['region_chi2'] = {
            'chi2_stat': float(chi2),
            'p_value': float(p_chi),
            'significant': p_chi < 0.05,
        }
        print(f"  Chi-squared: χ²={chi2:.2f}, p={p_chi:.6f}")
    except:
        pass

    return results


# ==============================================================================
# TEST 3: Age Bias
# ==============================================================================
def test_age_bias(decisions_df):
    """Correlation: Does age correlate with action severity?"""
    print("\n" + "=" * 70)
    print("TEST 3: Age Bias")
    print("=" * 70)

    results = {}

    # -- Age brackets --
    decisions_df = decisions_df.copy()
    decisions_df['age_bracket'] = pd.cut(
        decisions_df['age'], bins=[0, 25, 35, 45, 55, 65, 100],
        labels=['18-25', '26-35', '36-45', '46-55', '56-65', '65+']
    )

    severity_by_age = decisions_df.groupby('age_bracket').agg(
        avg_severity=('severity', 'mean'),
        pct_legal=('severity', lambda x: (x == 3).mean() * 100),
        avg_reward=('reward', 'mean'),
        count=('severity', 'count'),
    ).round(2)

    print(f"\n  Action Severity by Age Bracket:")
    print(f"  {'='*70}")
    print(f"  {'Age Bracket':<15} {'Avg Severity':>12} {'% Legal':>10} {'Avg Reward':>12} {'Count':>8}")
    print(f"  {'-'*70}")
    for bracket, row in severity_by_age.iterrows():
        print(f"  {str(bracket):<15} {row['avg_severity']:>12.2f} {row['pct_legal']:>9.1f}% "
              f"{row['avg_reward']:>12.2f} {int(row['count']):>8}")

    # -- Spearman correlation: age vs severity --
    corr, p_value = stats.spearmanr(decisions_df['age'], decisions_df['severity'])
    results['age_correlation'] = {
        'spearman_rho': float(corr),
        'p_value': float(p_value),
        'significant': p_value < 0.05,
    }

    print(f"\n  Spearman correlation (age vs severity): ρ={corr:.4f}, p={p_value:.6f}")
    if p_value < 0.05:
        direction = "older" if corr > 0 else "younger"
        print(f"  [WARN]  SIGNIFICANT: {'Older' if corr > 0 else 'Younger'} borrowers receive {'harsher' if corr > 0 else 'lighter'} actions")
    else:
        print(f"  [OK]  NOT SIGNIFICANT: No evidence of age bias")

    return results


# ==============================================================================
# TEST 4: Income Bias
# ==============================================================================
def test_income_bias(decisions_df):
    """Correlation + quintile analysis: Does income affect action severity?"""
    print("\n" + "=" * 70)
    print("TEST 4: Income Bias")
    print("=" * 70)

    results = {}

    # -- Income quintiles --
    decisions_df = decisions_df.copy()
    decisions_df['income_quintile'] = pd.qcut(
        decisions_df['income'], q=5, labels=['Q1 (Lowest)', 'Q2', 'Q3', 'Q4', 'Q5 (Highest)'],
        duplicates='drop'
    )

    severity_by_income = decisions_df.groupby('income_quintile').agg(
        avg_severity=('severity', 'mean'),
        pct_legal=('severity', lambda x: (x == 3).mean() * 100),
        avg_reward=('reward', 'mean'),
        count=('severity', 'count'),
    ).round(2)

    print(f"\n  Action Severity by Income Quintile:")
    print(f"  {'='*70}")
    print(f"  {'Income Quintile':<20} {'Avg Severity':>12} {'% Legal':>10} {'Avg Reward':>12} {'Count':>8}")
    print(f"  {'-'*70}")
    for quintile, row in severity_by_income.iterrows():
        print(f"  {str(quintile):<20} {row['avg_severity']:>12.2f} {row['pct_legal']:>9.1f}% "
              f"{row['avg_reward']:>12.2f} {int(row['count']):>8}")

    # -- Spearman correlation: income vs severity --
    corr, p_value = stats.spearmanr(decisions_df['income'], decisions_df['severity'])
    results['income_correlation'] = {
        'spearman_rho': float(corr),
        'p_value': float(p_value),
        'significant': p_value < 0.05,
    }

    print(f"\n  Spearman correlation (income vs severity): ρ={corr:.4f}, p={p_value:.6f}")
    if p_value < 0.05:
        direction = "higher" if corr > 0 else "lower"
        print(f"  [WARN]  SIGNIFICANT: {'Higher' if corr > 0 else 'Lower'} income borrowers receive {'harsher' if corr > 0 else 'lighter'} actions")
    else:
        print(f"  [OK]  NOT SIGNIFICANT: No evidence of income bias")

    return results


# ==============================================================================
# TEST 5: Graph Feature Bias
# ==============================================================================
def test_graph_feature_bias(decisions_df, base_env):
    """Does the agent treat borrowers differently based on graph features?"""
    print("\n" + "=" * 70)
    print("TEST 5: Graph Feature Bias")
    print("=" * 70)

    results = {}
    df = base_env.df

    # -- Check if graph features exist --
    graph_features = ['node_degree', 'pagerank', 'betweenness',
                      'community_risk_pct', 'neighborhood_stress_1hop']
    available = [f for f in graph_features if f in df.columns]

    if not available:
        print("  No graph features found. Skipping.")
        return results

    print(f"\n  Testing bias on graph features: {available}")

    for feat in available:
        feat_values = df[feat].values
        severity_values = decisions_df['severity'].values

        corr, p_value = stats.spearmanr(feat_values, severity_values)
        results[f'{feat}_bias'] = {
            'spearman_rho': float(corr),
            'p_value': float(p_value),
            'significant': p_value < 0.05,
        }

        if p_value < 0.05:
            print(f"  [WARN]  {feat}: ρ={corr:.4f}, p={p_value:.6f} (SIGNIFICANT)")
        else:
            print(f"  [OK]  {feat}: ρ={corr:.4f}, p={p_value:.6f}")

    return results


# ==============================================================================
# OVERALL FAIRNESS REPORT
# ==============================================================================
def generate_fairness_report(all_results, decisions_df):
    """Generate a human-readable fairness report."""
    print("\n" + "=" * 70)
    print("GENERATING FAIRNESS REPORT")
    print("=" * 70)

    biases_detected = []
    for test_name, result in all_results.items():
        if isinstance(result, dict) and result.get('significant', False):
            biases_detected.append(test_name)
        elif isinstance(result, dict):
            for sub_key, sub_val in result.items():
                if isinstance(sub_val, dict) and sub_val.get('significant', False):
                    biases_detected.append(f"{test_name}/{sub_key}")

    report_lines = []
    report_lines.append("=" * 70)
    report_lines.append("FAIRNESS AUDIT REPORT")
    report_lines.append("=" * 70)
    report_lines.append("")
    report_lines.append(f"Total borrowers analyzed: {len(decisions_df)}")
    report_lines.append(f"Action distribution:")
    for action in ACTION_NAMES:
        count = (decisions_df['chosen_action'] == action).sum()
        pct = count / len(decisions_df) * 100
        report_lines.append(f"  {action}: {count} ({pct:.1f}%)")
    report_lines.append("")
    report_lines.append(f"Overall average severity: {decisions_df['severity'].mean():.2f} / 3.00")
    report_lines.append(f"Overall average reward: {decisions_df['reward'].mean():.2f}")
    report_lines.append("")

    if biases_detected:
        report_lines.append(f"[WARN]  BIASES DETECTED: {len(biases_detected)} significant findings")
        report_lines.append("")
        for bias in biases_detected:
            report_lines.append(f"  - {bias}")
        report_lines.append("")
        report_lines.append("RECOMMENDATION: Review the biased dimensions and consider:")
        report_lines.append("  1. Removing the biased feature from the observation space")
        report_lines.append("  2. Adding fairness constraints to the reward function")
        report_lines.append("  3. Using adversarial debiasing during training")
    else:
        report_lines.append("[OK]  NO SIGNIFICANT BIASES DETECTED")
        report_lines.append("")
        report_lines.append("The agent's action distribution appears fair across all")
        report_lines.append("tested demographic dimensions (occupation, region, age, income).")

    report_text = "\n".join(report_lines)

    # Save report
    report_path = os.path.join(OUTPUT_DIR, "fairness_report.txt")
    with open(report_path, 'w') as f:
        f.write(report_text)
    print(f"\n  Saved: fairness_report.txt")
    print(f"\n{report_text}")

    return report_text


# ==============================================================================
# VISUALIZATION
# ==============================================================================
def plot_fairness_results(decisions_df):
    """Generate fairness visualization plots."""
    print("\nGenerating fairness plots...")

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # Plot 1: Occupation severity
    ax1 = axes[0, 0]
    occ_severity = decisions_df.groupby('occupation')['severity'].mean().sort_values()
    colors_occ = ['#2ecc71' if v < 1.5 else '#e74c3c' for v in occ_severity]
    ax1.barh(occ_severity.index, occ_severity.values, color=colors_occ, edgecolor='white')
    ax1.set_xlabel('Average Action Severity', fontsize=11)
    ax1.set_title('Severity by Occupation', fontsize=13, fontweight='bold')
    ax1.set_xlim(0, 3.5)

    # Plot 2: Region severity
    ax2 = axes[0, 1]
    region_severity = decisions_df.groupby('region')['severity'].mean().sort_values()
    colors_reg = plt.cm.RdYlGn_r(np.linspace(0, 1, len(region_severity)))
    ax2.barh(region_severity.index, region_severity.values, color=colors_reg, edgecolor='white')
    ax2.set_xlabel('Average Action Severity', fontsize=11)
    ax2.set_title('Severity by Region', fontsize=13, fontweight='bold')
    ax2.set_xlim(0, 3.5)

    # Plot 3: Age vs severity scatter (binned)
    ax3 = axes[1, 0]
    decisions_df_copy = decisions_df.copy()
    decisions_df_copy['age_bin'] = pd.cut(decisions_df_copy['age'], bins=10, labels=False)
    age_sev = decisions_df_copy.groupby('age_bin')['severity'].mean()
    ax3.plot(age_sev.index, age_sev.values, marker='o', color='#3498db', linewidth=2, markersize=8)
    ax3.set_xlabel('Age Bin', fontsize=11)
    ax3.set_ylabel('Average Severity', fontsize=11)
    ax3.set_title('Severity by Age', fontsize=13, fontweight='bold')

    # Plot 4: Income vs severity
    ax4 = axes[1, 1]
    decisions_df_copy['income_bin'] = pd.qcut(decisions_df_copy['income'], q=10, labels=False, duplicates='drop')
    income_sev = decisions_df_copy.groupby('income_bin')['severity'].mean()
    ax4.plot(income_sev.index, income_sev.values, marker='s', color='#e67e22', linewidth=2, markersize=8)
    ax4.set_xlabel('Income Decile', fontsize=11)
    ax4.set_ylabel('Average Severity', fontsize=11)
    ax4.set_title('Severity by Income', fontsize=13, fontweight='bold')

    plt.tight_layout()
    plot_path = os.path.join(OUTPUT_DIR, "fairness_plots.png")
    plt.savefig(plot_path, dpi=200, bbox_inches='tight')
    plt.close()
    print(f"  Saved: fairness_plots.png")


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    model, env, base_env = load_model_and_data()

    # Collect decisions
    decisions_df = collect_agent_decisions(model, env, base_env)

    # Save raw decisions
    decisions_df.to_csv(os.path.join(OUTPUT_DIR, "fairness_decisions.csv"), index=False)

    # Run tests
    all_results = {}
    all_results['occupation'] = test_occupation_bias(decisions_df)
    all_results['region'] = test_region_bias(decisions_df)
    all_results['age'] = test_age_bias(decisions_df)
    all_results['income'] = test_income_bias(decisions_df)
    all_results['graph_features'] = test_graph_feature_bias(decisions_df, base_env)

    # Save results
    # Flatten nested dicts for JSON serialization
    flat_results = {}
    for key, val in all_results.items():
        if isinstance(val, dict):
            for sub_key, sub_val in val.items():
                if isinstance(sub_val, (dict, list, float, int, bool, str)):
                    flat_results[f"{key}_{sub_key}"] = sub_val
                else:
                    flat_results[f"{key}_{sub_key}"] = str(sub_val)
        else:
            flat_results[key] = val

    with open(os.path.join(OUTPUT_DIR, "fairness_audit.json"), 'w') as f:
        json.dump(flat_results, f, indent=2, default=str)

    # Generate report
    generate_fairness_report(all_results, decisions_df)

    # Generate plots
    plot_fairness_results(decisions_df)

    print("\n" + "=" * 70)
    print("FAIRNESS AUDIT COMPLETE")
    print("=" * 70)
    print(f"\n  All outputs saved to: {OUTPUT_DIR}/")
    print(f"  ┣━ fairness_audit.json")
    print(f"  ┣━ fairness_report.txt")
    print(f"  ┣━ fairness_decisions.csv")
    print(f"  ┗━ fairness_plots.png")


if __name__ == "__main__":
    main()
