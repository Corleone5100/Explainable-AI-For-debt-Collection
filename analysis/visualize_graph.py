"""
Graph Visualization — 6-panel visualization of the borrower contagion graph.

Run from project root:
  python analysis/visualize_graph.py
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import networkx as nx
from sqlalchemy import create_engine

from config import cfg

# -- 1. Database Connection --
engine = create_engine(cfg.db_url)

# -- 2. Load a small subset for visualization --
SAMPLE_SIZE = 100

print("--- Loading Data from Postgres ---")
query = f"SELECT * FROM master_env_data LIMIT {SAMPLE_SIZE}"
df = pd.read_sql(query, engine)

df['occ_idx'] = df['occupation'].astype('category').cat.codes
df['reg_idx'] = df['region'].astype('category').cat.codes

print(f"--- Sampled {len(df)} borrowers for visualization ---")

# -- 3. Build weighted edges --
print("--- Creating Weighted Edges ---")
income_min, income_max = df['income'].min(), df['income'].max()
age_min, age_max = df['age'].min(), df['age'].max()
df['_income_norm'] = (df['income'] - income_min) / (income_max - income_min + 1e-8)
df['_age_norm'] = (df['age'] - age_min) / (age_max - age_min + 1e-8)

edge_list = []
edge_weights = []
grouped = df.groupby(['region', 'occupation'])

for name, group in grouped:
    indices = group.index.values
    if len(indices) > 1:
        for i in range(len(indices)):
            n_neighbors = min(len(indices) - 1, 5)
            neighbors = np.random.choice(
                [idx for idx in indices if idx != indices[i]],
                size=n_neighbors, replace=False
            )
            for nb in neighbors:
                income_sim = 1.0 - abs(df.loc[indices[i], '_income_norm'] - df.loc[nb, '_income_norm'])
                age_sim = 1.0 - abs(df.loc[indices[i], '_age_norm'] - df.loc[nb, '_age_norm'])
                same_risk = 1.0 if df.loc[indices[i], 'risk_category'] == df.loc[nb, 'risk_category'] else 0.0
                w = 0.5 * income_sim + 0.3 * age_sim + 0.2 * same_risk
                edge_list.append([indices[i], nb])
                edge_weights.append(w)

# Deduplicate
edge_dict = {}
for (u, v), w in zip(edge_list, edge_weights):
    key = tuple(sorted([u, v]))
    if key not in edge_dict:
        edge_dict[key] = w
    else:
        edge_dict[key] = max(edge_dict[key], w)

unique_edges = list(edge_dict.keys())
unique_weights = list(edge_dict.values())

print(f"--- Created {len(unique_edges)} unique weighted edges ---")

df.drop(columns=['_income_norm', '_age_norm'], inplace=True, errors='ignore')

# -- 4. Build NetworkX Graph --
G = nx.Graph()

for idx, row in df.iterrows():
    G.add_node(
        idx,
        risk=row['risk_category'],
        income=row['income'],
        cibil=row['cibil_score'],
        overdue=row['overdue_months'],
        occupation=row['occupation'],
        region=row['region']
    )

for (u, v), w in zip(unique_edges, unique_weights):
    G.add_edge(u, v, weight=w)

print(f"--- Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges ---")

# -- 5. Visualization --
fig, axes = plt.subplots(2, 3, figsize=(20, 12))
fig.suptitle("Enhanced Debt Collection Borrower Contagion Graph", fontsize=16, fontweight='bold')

print("--- Computing layout ---")
pos = nx.spring_layout(G, k=0.8, iterations=50, seed=42)

# -- Plot 1: Risk Category --
ax1 = axes[0, 0]
risk_colors = {
    'Very Low': '#2ecc71', 'Low': '#3498db',
    'Medium': '#f39c12', 'High': '#e74c3c', 'Very High': '#8e44ad'
}
node_colors = [risk_colors[G.nodes[n]['risk']] for n in G.nodes()]
edge_alphas = [min(0.6, G[u][v].get('weight', 0.5)) for u, v in G.edges()]

nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=60, alpha=0.85, ax=ax1)
nx.draw_networkx_edges(G, pos, alpha=edge_alphas, width=1.0, ax=ax1)

legend_elements = [
    plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=c, markersize=10, label=k)
    for k, c in risk_colors.items()
]
ax1.legend(handles=legend_elements, fontsize=7, loc='upper right')
ax1.set_title("Nodes: Risk Category | Edges: Weighted", fontweight='bold')
ax1.axis('off')

# -- Plot 2: Edge Weight Heatmap --
ax2 = axes[0, 1]
weights = [G[u][v].get('weight', 0.5) for u, v in G.edges()]
norm = plt.Normalize(vmin=0, vmax=1)
edge_colors = plt.cm.RdYlGn(weights)

nx.draw_networkx_nodes(G, pos, node_color='#cccccc', node_size=40, alpha=0.6, ax=ax2)
nx.draw_networkx_edges(G, pos, edge_color=edge_colors, width=2.0, ax=ax2)

sm = plt.cm.ScalarMappable(cmap='RdYlGn', norm=norm)
sm.set_array([])
plt.colorbar(sm, ax=ax2, label='Edge Weight', fraction=0.046, pad=0.04)
ax2.set_title("Edge Weight Heatmap\n(Green = Similar, Red = Dissimilar)", fontweight='bold')
ax2.axis('off')

# -- Plot 3: Node Degree (size) --
ax3 = axes[0, 2]
degrees = dict(G.degree())
node_sizes = [degrees[n] * 15 + 20 for n in G.nodes()]

nx.draw_networkx_nodes(G, pos, node_size=node_sizes, node_color=node_colors,
                       alpha=0.85, ax=ax3)
nx.draw_networkx_edges(G, pos, alpha=0.2, width=0.5, ax=ax3)
ax3.set_title(f"Node Size = Degree\n(range: {min(degrees.values())}-{max(degrees.values())})",
              fontweight='bold')
ax3.axis('off')

# -- Plot 4: Community Detection --
ax4 = axes[1, 0]
try:
    import community as community_louvain
    partition = community_louvain.best_partition(G, random_state=42)
    n_communities = len(set(partition.values()))
    cmap = plt.cm.get_cmap('tab20', n_communities)
    comm_colors = [cmap(partition[n]) for n in G.nodes()]

    nx.draw_networkx_nodes(G, pos, node_color=comm_colors, node_size=50, alpha=0.85, ax=ax4)
    nx.draw_networkx_edges(G, pos, alpha=0.15, width=0.5, ax=ax4)
    ax4.set_title(f"Communities Detected ({n_communities})", fontweight='bold')
except ImportError:
    nx.draw_networkx_nodes(G, pos, node_color='#999999', node_size=50, ax=ax4)
    ax4.set_title("Community Detection\n(python-louvain not installed)", fontweight='bold')
ax4.axis('off')

# -- Plot 5: Overdue Months (contagion signal) --
ax5 = axes[1, 1]
overdues = [G.nodes[n]['overdue'] for n in G.nodes()]
norm_overdue = plt.Normalize(vmin=min(overdues), vmax=max(overdues))
overdue_colors = plt.cm.RdYlGn_r([norm_overdue(o) for o in overdues])

nx.draw_networkx_nodes(G, pos, node_color=overdue_colors, node_size=60, alpha=0.85, ax=ax5)
nx.draw_networkx_edges(G, pos, alpha=0.15, width=0.5, ax=ax5)

sm2 = plt.cm.ScalarMappable(cmap='RdYlGn_r', norm=norm_overdue)
sm2.set_array([])
plt.colorbar(sm2, ax=ax5, label='Overdue Months', fraction=0.046, pad=0.04)
ax5.set_title("Node Color = Overdue Months\n(Red = High Risk)", fontweight='bold')
ax5.axis('off')

# -- Plot 6: Graph Statistics --
ax6 = axes[1, 2]
ax6.axis('off')

# Compute stats
n_components = nx.number_connected_components(G)
clustering = nx.average_clustering(G)
try:
    assortativity = nx.degree_pearson_correlation_coefficient(G)
except:
    assortativity = 0.0

same_risk_edges = sum(1 for u, v in G.edges() if G.nodes[u]['risk'] == G.nodes[v]['risk'])
total_edges = G.number_of_edges()
risk_ratio = same_risk_edges / total_edges if total_edges > 0 else 0

stats_text = f"""
GRAPH STATISTICS
{'='*35}
Nodes:              {G.number_of_nodes()}
Edges:              {G.number_of_edges()}
Avg Degree:         {np.mean(list(degrees.values())):.2f}
Connected Comps:    {n_components}
Clustering Coeff:   {clustering:.4f}
Degree Assort.:     {assortativity:.4f}
Same-Risk Edges:    {same_risk_edges}/{total_edges} ({risk_ratio*100:.1f}%)

EDGE WEIGHT STATS
{'='*35}
Mean Weight:        {np.mean(unique_weights):.4f}
Std Weight:         {np.std(unique_weights):.4f}
Min Weight:         {min(unique_weights):.4f}
Max Weight:         {max(unique_weights):.4f}

RISK DISTRIBUTION
{'='*35}
"""
for risk in ['Very Low', 'Low', 'Medium', 'High', 'Very High']:
    count = sum(1 for n in G.nodes() if G.nodes[n]['risk'] == risk)
    stats_text += f"{risk:15s}: {count}\n"

ax6.text(0.05, 0.98, stats_text, transform=ax6.transAxes, fontsize=8,
         verticalalignment='top', fontfamily='monospace',
         bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.8))
ax6.set_title("Graph & Edge Statistics", fontweight='bold')

plt.tight_layout()
plt.savefig("graph_visualization_enhanced.png", dpi=300, bbox_inches='tight')
print("--- Saved: graph_visualization_enhanced.png ---")
plt.show()
