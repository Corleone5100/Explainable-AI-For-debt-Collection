"""
XAI Visualization Module
========================
Generates stakeholder-friendly visualizations of the RL agent's policy.

Run from project root:
  python xai/xai_visualizations.py
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import warnings
import numpy as np
import pandas as pd
import torch
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from sklearn.manifold import TSNE
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
ATTENTION_PATH = "gat_attention_weights.csv"
OUTPUT_DIR = "xai_outputs"
PLOT_DIR = os.path.join(OUTPUT_DIR, "policy_plots")

os.makedirs(PLOT_DIR, exist_ok=True)

ACTION_COLORS = ['#95a5a6', '#3498db', '#e67e22', '#e74c3c']
RISK_COLORS = {'Very Low': '#2ecc71', 'Low': '#3498db', 'Medium': '#f39c12',
               'High': '#e74c3c', 'Very High': '#8e44ad'}

print("=" * 70)
print("XAI VISUALIZATION MODULE")
print("=" * 70)


def load_model_and_data():
    """Load trained model, environment, and data."""
    print("\nLoading model and data...")

    base_env = DebtCollectionEnv(DATA_PATH)
    env = DummyVecEnv([lambda: base_env])
    env = VecNormalize.load(VEC_NORM_PATH, env)
    env.training = False
    env.norm_reward = False

    model = PPO.load(MODEL_PATH, device="cpu")

    print(f"  Model: {MODEL_PATH}")
    print(f"  Data: {len(base_env.df)} borrowers, {len(base_env.feature_cols)} features")
    return model, env, base_env


def get_preferred_actions(model, env, base_env):
    """Run the agent on all borrowers and collect preferred actions."""
    df = base_env.df
    feature_cols = base_env.feature_cols
    n = len(df)

    all_actions = np.zeros(n, dtype=int)
    all_probs = np.zeros((n, 4), dtype=np.float32)
    all_confidence = np.zeros(n, dtype=np.float32)
    all_entropy = np.zeros(n, dtype=np.float32)

    print("\nRunning agent on all borrowers to collect policy...")
    for i in tqdm(range(n), desc="Collecting actions", ncols=80):
        obs = df.iloc[i][feature_cols].values.astype(np.float32)
        conf = base_env.get_action_probs(model, obs, deterministic=True)
        all_actions[i] = conf['action']
        all_probs[i] = conf['probs']
        all_confidence[i] = conf['confidence']
        all_entropy[i] = conf['entropy']

    return all_actions, all_probs, all_confidence, all_entropy


# ==============================================================================
# PLOT 1: Risk × Action Distribution
# ==============================================================================
def plot_risk_action_distribution(df, actions):
    """Stacked bar chart: action distribution per risk category."""
    print("\n[Plot 1/6] Risk × Action Distribution...")

    df = df.copy()
    df['chosen_action'] = actions

    risk_order = ['Very Low', 'Low', 'Medium', 'High', 'Very High']
    risk_data = [df[df['risk_category'] == r] for r in risk_order]

    action_counts = []
    for r_data in risk_data:
        if len(r_data) == 0:
            action_counts.append([0, 0, 0, 0])
        else:
            counts = r_data['chosen_action'].value_counts().reindex(range(4), fill_value=0)
            action_counts.append((counts / len(r_data) * 100).values.tolist())

    action_counts = np.array(action_counts)

    fig, ax = plt.subplots(figsize=(12, 6))
    x = np.arange(len(risk_order))
    width = 0.5
    bottom = np.zeros(len(risk_order))

    for a_idx in range(4):
        ax.bar(x, action_counts[:, a_idx], width, bottom=bottom,
               color=ACTION_COLORS[a_idx], label=ACTION_NAMES[a_idx], edgecolor='white')
        bottom += action_counts[:, a_idx]

        # Add percentage labels
        for i in range(len(risk_order)):
            if action_counts[i, a_idx] > 5:
                ax.text(i, bottom[i] - action_counts[i, a_idx] / 2,
                        f'{action_counts[i, a_idx]:.0f}%',
                        ha='center', va='center', fontsize=9, fontweight='bold',
                        color='white' if action_counts[i, a_idx] > 20 else 'black')

    ax.set_xticks(x)
    ax.set_xticklabels(risk_order, fontsize=11)
    ax.set_ylabel('Percentage (%)', fontsize=12)
    ax.set_title('Collection Action Distribution by Risk Category', fontsize=14, fontweight='bold')
    ax.legend(loc='upper right', fontsize=10)
    ax.set_ylim(0, 105)

    plt.tight_layout()
    plt.savefig(os.path.join(PLOT_DIR, "risk_action_distribution.png"), dpi=200, bbox_inches='tight')
    plt.close()
    print(f"  Saved: risk_action_distribution.png")


# ==============================================================================
# PLOT 2: Overdue × Income Heatmap
# ==============================================================================
def plot_overdue_income_heatmap(df, actions):
    """2D heatmap: preferred action as a function of overdue months and income."""
    print("\n[Plot 2/6] Overdue × Income Heatmap...")

    df = df.copy()
    df['chosen_action'] = actions

    # Bin overdue and income
    df['overdue_bin'] = pd.cut(df['overdue_months'], bins=12, labels=False)
    df['income_bin'] = pd.cut(df['income'], bins=10, labels=False)

    # Create pivot table: most common action per bin
    pivot = df.groupby(['overdue_bin', 'income_bin'])['chosen_action'].agg(
        lambda x: x.mode().iloc[0] if len(x) > 0 else 0
    ).unstack()

    fig, ax = plt.subplots(figsize=(12, 8))
    cmap = mcolors.ListedColormap(ACTION_COLORS)
    bounds = [-0.5, 0.5, 1.5, 2.5, 3.5]
    norm = mcolors.BoundaryNorm(bounds, cmap.N)

    im = ax.imshow(pivot.values, cmap=cmap, norm=norm, aspect='auto', origin='lower')

    # Labels
    income_bins = sorted(df['income'].quantile(np.linspace(0, 1, 11)).dropna().values)
    overdue_bins = sorted(df['overdue_months'].quantile(np.linspace(0, 1, 13)).dropna().values)

    ax.set_xlabel('Income (quantile bins)', fontsize=12)
    ax.set_ylabel('Overdue Months (quantile bins)', fontsize=12)
    ax.set_title('Preferred Collection Action by Overdue Months × Income',
                 fontsize=14, fontweight='bold')

    # Colorbar
    cbar = plt.colorbar(im, ax=ax, ticks=[0, 1, 2, 3])
    cbar.ax.set_yticklabels(ACTION_NAMES, fontsize=9)

    plt.tight_layout()
    plt.savefig(os.path.join(PLOT_DIR, "overdue_income_heatmap.png"), dpi=200, bbox_inches='tight')
    plt.close()
    print(f"  Saved: overdue_income_heatmap.png")


# ==============================================================================
# PLOT 3: GAT Embedding t-SNE
# ==============================================================================
def plot_gat_embedding_tsne(df, actions):
    """t-SNE projection of 16-dim GAT embeddings, colored by action and risk."""
    print("\n[Plot 3/6] GAT Embedding t-SNE...")

    # Find GAT embedding columns
    gat_cols = [c for c in df.columns if c.startswith('gat_embedding_')]
    if not gat_cols:
        print("  No GAT embedding columns found. Skipping.")
        return

    gat_cols.sort(key=lambda x: int(x.split('_')[-1]))
    embeddings = df[gat_cols].values

    print(f"  Running t-SNE on {len(gat_cols)}-dim embeddings ({len(embeddings)} nodes)...")
    tsne = TSNE(n_components=2, random_state=42, perplexity=30, max_iter=1000)
    embedding_2d = tsne.fit_transform(embeddings)

    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    # Left: colored by action
    ax1 = axes[0]
    scatter1 = ax1.scatter(embedding_2d[:, 0], embedding_2d[:, 1],
                           c=[ACTION_COLORS[a] for a in actions],
                           s=10, alpha=0.6, edgecolors='none')
    ax1.set_title('GAT Embedding Space — Colored by RL Action', fontsize=13, fontweight='bold')
    ax1.set_xlabel('t-SNE dim 1')
    ax1.set_ylabel('t-SNE dim 2')
    legend1 = [plt.Line2D([0], [0], marker='o', color='w',
                          markerfacecolor=ACTION_COLORS[i], markersize=8, label=ACTION_NAMES[i])
               for i in range(4)]
    ax1.legend(handles=legend1, fontsize=8, loc='upper right')

    # Right: colored by risk
    ax2 = axes[1]
    risk_colors_list = [RISK_COLORS.get(df.iloc[i]['risk_category'], '#999999') for i in range(len(df))]
    scatter2 = ax2.scatter(embedding_2d[:, 0], embedding_2d[:, 1],
                           c=risk_colors_list, s=10, alpha=0.6, edgecolors='none')
    ax2.set_title('GAT Embedding Space — Colored by Risk Category', fontsize=13, fontweight='bold')
    ax2.set_xlabel('t-SNE dim 1')
    ax2.set_ylabel('t-SNE dim 2')
    legend2 = [plt.Line2D([0], [0], marker='o', color='w',
                          markerfacecolor=c, markersize=8, label=k)
               for k, c in RISK_COLORS.items()]
    ax2.legend(handles=legend2, fontsize=8, loc='upper right')

    plt.tight_layout()
    plt.savefig(os.path.join(PLOT_DIR, "gat_embedding_tsne.png"), dpi=200, bbox_inches='tight')
    plt.close()
    print(f"  Saved: gat_embedding_tsne.png")


# ==============================================================================
# PLOT 4: Neighborhood Stress → Action Shift
# ==============================================================================
def plot_stress_action_shift(df, actions):
    """Line plot: how action preferences change as neighborhood stress increases."""
    print("\n[Plot 4/6] Neighborhood Stress → Action Shift...")

    # Use 1-hop stress if available, else fallback
    stress_col = 'neighborhood_stress_1hop'
    if stress_col not in df.columns:
        stress_col = 'neighborhood_stress_signal'
    if stress_col not in df.columns:
        print(f"  No neighborhood stress column found. Skipping.")
        return

    df = df.copy()
    df['chosen_action'] = actions

    # Bin stress into 10 groups
    df['stress_bin'] = pd.qcut(df[stress_col], q=10, labels=False, duplicates='drop')

    stress_action_dist = df.groupby('stress_bin')['chosen_action'].apply(
        lambda x: (x.value_counts().reindex(range(4), fill_value=0) / len(x) * 100).values
    )

    stress_bins = sorted(df['stress_bin'].unique())
    dist_matrix = np.array(stress_action_dist.tolist())

    fig, ax = plt.subplots(figsize=(12, 6))
    x = stress_bins

    for a_idx in range(4):
        ax.plot(x, dist_matrix[:, a_idx], marker='o', linewidth=2,
                color=ACTION_COLORS[a_idx], label=ACTION_NAMES[a_idx], markersize=6)

    ax.set_xlabel(f'{stress_col} (decile bins)', fontsize=12)
    ax.set_ylabel('Percentage Choosing Action (%)', fontsize=12)
    ax.set_title(f'Collection Action Preference vs. {stress_col}',
                 fontsize=14, fontweight='bold')
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(os.path.join(PLOT_DIR, "stress_action_shift.png"), dpi=200, bbox_inches='tight')
    plt.close()
    print(f"  Saved: stress_action_shift.png")


# ==============================================================================
# PLOT 5: Confidence Distribution
# ==============================================================================
def plot_confidence_distribution(confidence, entropy):
    """Histogram of decision confidence and entropy."""
    print("\n[Plot 5/6] Confidence Distribution...")

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # Left: Confidence histogram
    ax1 = axes[0]
    ax1.hist(confidence, bins=30, color='#2ecc71', edgecolor='white', alpha=0.8)
    ax1.axvline(x=0.5, color='red', linestyle='--', label='50% threshold', linewidth=2)
    ax1.axvline(x=0.8, color='orange', linestyle='--', label='80% threshold', linewidth=2)
    ax1.set_xlabel('Decision Confidence (max action probability)', fontsize=11)
    ax1.set_ylabel('Count', fontsize=11)
    ax1.set_title('Distribution of Decision Confidence', fontsize=13, fontweight='bold')
    ax1.legend(fontsize=9)

    # Right: Entropy histogram
    ax2 = axes[1]
    ax2.hist(entropy, bins=30, color='#e74c3c', edgecolor='white', alpha=0.8)
    ax2.axvline(x=1.2, color='red', linestyle='--', label='Uncertainty threshold (1.2)', linewidth=2)
    ax2.axvline(x=np.log(4), color='orange', linestyle='--', label='Max entropy (log 4)', linewidth=2)
    ax2.set_xlabel('Policy Entropy', fontsize=11)
    ax2.set_ylabel('Count', fontsize=11)
    ax2.set_title('Distribution of Policy Entropy', fontsize=13, fontweight='bold')
    ax2.legend(fontsize=9)

    plt.tight_layout()
    plt.savefig(os.path.join(PLOT_DIR, "confidence_distribution.png"), dpi=200, bbox_inches='tight')
    plt.close()
    print(f"  Saved: confidence_distribution.png")


# ==============================================================================
# PLOT 6: GAT Attention Network
# ==============================================================================
def plot_gat_attention_network(df):
    """Visualize attention weights for a sample of borrowers."""
    print("\n[Plot 6/6] GAT Attention Network (sample)...")

    if not os.path.exists(ATTENTION_PATH):
        print(f"  Attention weights file not found: {ATTENTION_PATH}. Skipping.")
        return

    attn_df = pd.read_csv(ATTENTION_PATH)

    if len(attn_df) == 0:
        print("  No attention weights found. Skipping.")
        return

    # Sample some target nodes and show their top neighbors
    target_nodes = attn_df['target_node'].unique()
    sample_targets = np.random.choice(target_nodes, size=min(5, len(target_nodes)), replace=False)

    fig, axes = plt.subplots(1, len(sample_targets), figsize=(6 * len(sample_targets), 5))
    if len(sample_targets) == 1:
        axes = [axes]

    for ax_idx, target in enumerate(sample_targets):
        ax = axes[ax_idx]
        target_attn = attn_df[attn_df['target_node'] == target]
        top_neighbors = target_attn.nlargest(8, 'attention_weight')

        if len(top_neighbors) == 0:
            ax.text(0.5, 0.5, 'No attention data', ha='center', va='center', transform=ax.transAxes)
            ax.axis('off')
            continue

        # Get neighbor risk categories
        neighbor_risks = []
        neighbor_weights = []
        for _, row in top_neighbors.iterrows():
            src = int(row['source_node'])
            if src < len(df):
                risk = df.iloc[src].get('risk_category', 'Unknown')
            else:
                risk = 'Unknown'
            neighbor_risks.append(risk)
            neighbor_weights.append(row['attention_weight'])

        colors = [RISK_COLORS.get(r, '#999999') for r in neighbor_risks]

        bars = ax.barh(range(len(neighbor_risks)), neighbor_weights, color=colors, edgecolor='white')
        ax.set_yticks(range(len(neighbor_risks)))
        ax.set_yticklabels([f"Neighbor {i} ({r})" for i, r in enumerate(neighbor_risks)], fontsize=8)
        ax.set_xlabel('Attention Weight', fontsize=10)
        ax.set_title(f'Target Borrower #{int(target)}\n(Risk: {df.iloc[int(target)].get("risk_category", "?") if int(target) < len(df) else "?"})',
                     fontsize=11, fontweight='bold')
        ax.invert_yaxis()

    plt.tight_layout()
    plt.savefig(os.path.join(PLOT_DIR, "gat_attention_network.png"), dpi=200, bbox_inches='tight')
    plt.close()
    print(f"  Saved: gat_attention_network.png")


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    model, env, base_env = load_model_and_data()

    # Collect preferred actions for all borrowers
    actions, probs, confidence, entropy = get_preferred_actions(model, env, base_env)

    # Generate all plots
    plot_risk_action_distribution(base_env.df, actions)
    plot_overdue_income_heatmap(base_env.df, actions)
    plot_gat_embedding_tsne(base_env.df, actions)
    plot_stress_action_shift(base_env.df, actions)
    plot_confidence_distribution(confidence, entropy)
    plot_gat_attention_network(base_env.df)

    print("\n" + "=" * 70)
    print("XAI VISUALIZATION COMPLETE")
    print("=" * 70)
    print(f"\n  All plots saved to: {PLOT_DIR}/")
    print(f"  ┣━ risk_action_distribution.png")
    print(f"  ┣━ overdue_income_heatmap.png")
    print(f"  ┣━ gat_embedding_tsne.png")
    print(f"  ┣━ stress_action_shift.png")
    print(f"  ┣━ confidence_distribution.png")
    print(f"  ┗━ gat_attention_network.png")


if __name__ == "__main__":
    main()
