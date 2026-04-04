import pandas as pd
import numpy as np
import torch
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import networkx as nx
from sqlalchemy import create_engine

# ── 1. Database Connection ──
engine = create_engine("postgresql://postgres:Bishal5100#@localhost:5432/debt_market_db")

# ── 2. Load a small subset for visualization ──
# Full graph is 200k nodes – we sample 300 for a readable visualization
SAMPLE_SIZE = 300

print("--- Loading Data from Postgres ---")
query = f"SELECT * FROM master_env_data LIMIT {SAMPLE_SIZE}"
df = pd.read_sql(query, engine)

# Encode categoricals
df['occ_idx'] = df['occupation'].astype('category').cat.codes
df['reg_idx'] = df['region'].astype('category').cat.codes

print(f"--- Sampled {len(df)} borrowers for visualization ---")

# ── 3. Build edges (same logic as graph_builder.py) ──
print("--- Creating Edges ---")
edge_list = []
grouped = df.groupby(['region', 'occupation'])

for name, group in grouped:
    indices = group.index.values
    if len(indices) > 1:
        for i in range(len(indices)):
            neighbors = np.random.choice(
                indices, size=min(len(indices), 5), replace=False
            )
            for nb in neighbors:
                if i != nb:
                    edge_list.append([indices[i], nb])

# Remove duplicate edges
edge_list = list(set(tuple(sorted(e)) for e in edge_list))
print(f"--- Created {len(edge_list)} unique edges ---")

# ── 4. Build NetworkX Graph ──
G = nx.Graph()

# Add nodes with attributes
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

# Add edges
for u, v in edge_list:
    G.add_edge(u, v)

print(f"--- Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges ---")

# ── 5. Visualization ──
fig, axes = plt.subplots(2, 2, figsize=(16, 14))
fig.suptitle("Debt Collection Borrower Contagion Graph", fontsize=16, fontweight='bold')

# Layout algorithm (force-directed)
print("--- Computing layout (this may take a moment) ---")
pos = nx.spring_layout(G, k=0.5, iterations=100, seed=42)

# ── Plot 1: Colored by Risk Category ──
ax1 = axes[0, 0]
risk_colors = {
    'Very Low': '#2ecc71',
    'Low': '#3498db',
    'Medium': '#f39c12',
    'High': '#e74c3c',
    'Very High': '#8e44ad'
}
node_colors = [risk_colors[G.nodes[n]['risk']] for n in G.nodes()]

nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=50, alpha=0.8, ax=ax1)
nx.draw_networkx_edges(G, pos, alpha=0.15, width=0.5, ax=ax1)

legend_elements = [
    plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=c, markersize=10, label=k)
    for k, c in risk_colors.items()
]
ax1.legend(handles=legend_elements, fontsize=7, loc='upper right')
ax1.set_title("Nodes Colored by Risk Category", fontweight='bold')
ax1.axis('off')

# ── Plot 2: Colored by Occupation ──
ax2 = axes[0, 1]
occ_colors = {
    'Salaried': '#1abc9c',
    'Self-employed': '#e67e22',
    'Agriculturalist': '#27ae60',
    'Daily wage worker': '#95a5a6'
}
node_colors_occ = [occ_colors.get(G.nodes[n]['occupation'], '#999999') for n in G.nodes()]

nx.draw_networkx_nodes(G, pos, node_color=node_colors_occ, node_size=50, alpha=0.8, ax=ax2)
nx.draw_networkx_edges(G, pos, alpha=0.15, width=0.5, ax=ax2)

legend_occ = [
    plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=c, markersize=10, label=k)
    for k, c in occ_colors.items()
]
ax2.legend(handles=legend_occ, fontsize=7, loc='upper right')
ax2.set_title("Nodes Colored by Occupation", fontweight='bold')
ax2.axis('off')

# ── Plot 3: Node Size by Income ──
ax3 = axes[1, 0]
incomes = [G.nodes[n]['income'] for n in G.nodes()]
node_sizes = np.interp(incomes, (min(incomes), max(incomes)), (30, 150))

norm = plt.Normalize(vmin=min(incomes), vmax=max(incomes))
sm = plt.cm.ScalarMappable(cmap='YlOrRd', norm=norm)
sm.set_array([])

nx.draw_networkx_nodes(G, pos, node_size=node_sizes, node_color=incomes,
                       cmap='YlOrRd', alpha=0.8, ax=ax3)
nx.draw_networkx_edges(G, pos, alpha=0.15, width=0.5, ax=ax3)
plt.colorbar(sm, ax=ax3, label='Income', fraction=0.046, pad=0.04)
ax3.set_title("Node Size & Color = Income", fontweight='bold')
ax3.axis('off')

# ── Plot 4: Highlight High-Risk Clusters ──
ax4 = axes[1, 1]

# Find connected components
components = list(nx.connected_components(G))
components_sorted = sorted(components, key=len, reverse=True)[:10]

# Color each major component differently
cmap = plt.cm.get_cmap('tab10', len(components_sorted))
for i, comp in enumerate(components_sorted):
    subgraph = G.subgraph(comp)
    nx.draw_networkx_nodes(G, pos, nodelist=list(comp), node_color=[cmap(i)],
                           node_size=50, alpha=0.8, ax=ax4)

# Draw edges only within components
for i, comp in enumerate(components_sorted):
    sub_edges = [(u, v) for u, v in G.edges() if u in comp and v in comp]
    nx.draw_networkx_edges(G, pos, edgelist=sub_edges, edge_color=[cmap(i)],
                           alpha=0.3, width=0.5, ax=ax4)

legend_comp = [
    plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=cmap(i),
               markersize=8, label=f"Cluster {i+1} ({len(c)} nodes)")
    for i, c in enumerate(components_sorted)
]
ax4.legend(handles=legend_comp, fontsize=6, loc='upper right')
ax4.set_title("Top 10 Connected Components (Clusters)", fontweight='bold')
ax4.axis('off')

plt.tight_layout()
plt.savefig("graph_visualization.png", dpi=300, bbox_inches='tight')
print("--- Saved: graph_visualization.png ---")
plt.show()

# ── 6. Graph Statistics ──
print("\n" + "="*60)
print("GRAPH STATISTICS")
print("="*60)
print(f"Number of nodes:      {G.number_of_nodes()}")
print(f"Number of edges:      {G.number_of_edges()}")
print(f"Connected components: {nx.number_connected_components(G)}")

if G.number_of_nodes() < 1000:
    avg_degree = sum(dict(G.degree()).values()) / G.number_of_nodes()
    print(f"Average degree:       {avg_degree:.2f}")
    
    clustering_coeff = nx.average_clustering(G)
    print(f"Clustering coefficient: {clustering_coeff:.4f}")
    
    # Risk distribution
    print("\n--- Risk Category Distribution ---")
    risk_counts = {}
    for n in G.nodes():
        risk = G.nodes[n]['risk']
        risk_counts[risk] = risk_counts.get(risk, 0) + 1
    for risk, count in sorted(risk_counts.items()):
        print(f"  {risk}: {count}")
    
    # Edge density within risk groups
    print("\n--- Edges Within Same Risk Category ---")
    same_risk_edges = sum(1 for u, v in G.edges() if G.nodes[u]['risk'] == G.nodes[v]['risk'])
    print(f"  Same-risk edges: {same_risk_edges}/{G.number_of_edges()} ({same_risk_edges/G.number_of_edges()*100:.1f}%)")

print("="*60)
