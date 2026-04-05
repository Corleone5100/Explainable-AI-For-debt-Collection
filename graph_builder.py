import pandas as pd
import numpy as np
import torch
import torch.nn.functional as F
from torch_geometric.data import Data
from torch_geometric.nn import GATConv
from torch_geometric.utils import degree
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sqlalchemy import create_engine
import networkx as nx
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

device = torch.device('cuda') if torch.cuda.is_available() else torch.device('cpu')
print(f"Using device: {device}")

# ── Database Connection ──
engine = create_engine("postgresql://postgres:Bishal5100#@localhost:5432/debt_market_db")

# ==============================================================================
# STEP 1: LOAD DATA
# ==============================================================================
def load_data(limit=200000):
    print("=" * 60)
    print("STEP 1: Loading Data from Postgres")
    print("=" * 60)
    query = f"SELECT * FROM master_env_data LIMIT {limit}"
    df = pd.read_sql(query, engine)

    # Encode categoricals
    df['occ_idx'] = df['occupation'].astype('category').cat.codes
    df['reg_idx'] = df['region'].astype('category').cat.codes

    # Encode risk_category as numeric labels for GNN training
    le = LabelEncoder()
    df['risk_label'] = le.fit_transform(df['risk_category'])

    print(f"  Loaded {len(df)} borrowers")
    print(f"  Risk distribution:\n{df['risk_category'].value_counts().to_string()}")
    return df, le


# ==============================================================================
# STEP 2: BUILD WEIGHTED GRAPH
# ==============================================================================
def build_weighted_edges(df):
    print("\n" + "=" * 60)
    print("STEP 2: Building Weighted Edges (Contagion Links)")
    print("=" * 60)

    # Normalize features for weight computation
    income_min, income_max = df['income'].min(), df['income'].max()
    age_min, age_max = df['age'].min(), df['age'].max()

    df['_income_norm'] = (df['income'] - income_min) / (income_max - income_min + 1e-8)
    df['_age_norm'] = (df['age'] - age_min) / (age_max - age_min + 1e-8)

    edge_list = []
    edge_weights = []

    grouped = df.groupby(['region', 'occupation'])
    total_groups = len(grouped)

    for gidx, (name, group) in enumerate(tqdm(grouped, desc="Building edges", ncols=80)):
        indices = group.index.values
        if len(indices) > 1:
            for i in range(len(indices)):
                n_neighbors = min(len(indices) - 1, 5)
                neighbors = np.random.choice(
                    [idx for idx in indices if idx != indices[i]],
                    size=n_neighbors, replace=False
                )
                for nb in neighbors:
                    # Compute edge weight
                    income_sim = 1.0 - abs(
                        df.loc[indices[i], '_income_norm'] - df.loc[nb, '_income_norm']
                    )
                    age_sim = 1.0 - abs(
                        df.loc[indices[i], '_age_norm'] - df.loc[nb, '_age_norm']
                    )
                    same_risk = 1.0 if (
                        df.loc[indices[i], 'risk_category'] == df.loc[nb, 'risk_category']
                    ) else 0.0

                    # Weighted combination
                    w = 0.5 * income_sim + 0.3 * age_sim + 0.2 * same_risk
                    edge_list.append([indices[i], nb])
                    edge_weights.append(w)

        if (gidx + 1) % 100 == 0:
            print(f"    Processed {gidx + 1}/{total_groups} groups...")

    # Remove duplicate edges while preserving weights
    edge_dict = {}
    for (u, v), w in zip(edge_list, edge_weights):
        key = tuple(sorted([u, v]))
        if key not in edge_dict:
            edge_dict[key] = w
        else:
            edge_dict[key] = max(edge_dict[key], w)  # Keep max weight for duplicates

    unique_edges = list(edge_dict.keys())
    unique_weights = list(edge_dict.values())

    edge_index = torch.tensor(unique_edges, dtype=torch.long).t().contiguous()
    edge_attr = torch.tensor(unique_weights, dtype=torch.float).unsqueeze(1)

    print(f"  Created {len(unique_edges)} unique edges with weights")
    print(f"  Weight range: [{min(unique_weights):.4f}, {max(unique_weights):.4f}]")
    print(f"  Mean weight: {np.mean(unique_weights):.4f}")

    # Clean up temp columns
    df.drop(columns=['_income_norm', '_age_norm'], inplace=True, errors='ignore')

    return edge_index, edge_attr


# ==============================================================================
# STEP 3: COMPUTE NODE STRUCTURAL FEATURES
# ==============================================================================
def compute_structural_features(df, edge_index):
    print("\n" + "=" * 60)
    print("STEP 3: Computing Node Structural Features")
    print("=" * 60)

    num_nodes = len(df)
    row, col = edge_index

    # ── Node Degree ──
    deg = degree(row, num_nodes=num_nodes, dtype=torch.float)
    df['node_degree'] = deg.numpy()

    # ── For NetworkX-based metrics, build sparse graph ──
    print("  Building NetworkX graph for centrality metrics...")
    G = nx.Graph()
    G.add_nodes_from(range(num_nodes))
    edge_pairs = edge_index.t().numpy()
    G.add_edges_from(edge_pairs)

    # ── PageRank ──
    print("  Computing PageRank...")
    pagerank_dict = nx.pagerank(G, max_iter=100)
    df['pagerank'] = pd.Series(pagerank_dict).values

    # ── Betweenness Centrality (sample for large graphs) ──
    if num_nodes > 5000:
        print(f"  Computing betweenness centrality (sampled for {num_nodes} nodes)...")
        # Use approximation with k shortest paths for large graphs
        betweenness = nx.betweenness_centrality(G, k=min(1000, num_nodes))
    else:
        print("  Computing betweenness centrality...")
        betweenness = nx.betweenness_centrality(G)
    df['betweenness'] = pd.Series(betweenness).values

    print(f"  Degree:  [{df['node_degree'].min():.0f}, {df['node_degree'].max():.0f}] "
          f"(mean={df['node_degree'].mean():.2f})")
    print(f"  PageRank: [{df['pagerank'].min():.6f}, {df['pagerank'].max():.6f}]")
    print(f"  Betweenness: [{df['betweenness'].min():.6f}, {df['betweenness'].max():.6f}]")

    return df, G


# ==============================================================================
# STEP 4: GRAPH VALIDATION METRICS
# ==============================================================================
def validate_graph_structure(df, G, edge_index):
    print("\n" + "=" * 60)
    print("STEP 4: Graph Validation Metrics")
    print("=" * 60)

    num_nodes = len(df)
    num_edges = edge_index.shape[1]

    print(f"  Nodes: {num_nodes}")
    print(f"  Edges: {num_edges}")
    print(f"  Density: {2 * num_edges / (num_nodes * (num_nodes - 1)):.6f}")

    # ── Connected Components ──
    n_components = nx.number_connected_components(G)
    print(f"  Connected components: {n_components}")

    # ── Average Clustering Coefficient ──
    print("  Computing average clustering coefficient...")
    if num_nodes > 10000:
        # Sample for large graphs
        sample_nodes = list(np.random.choice(G.nodes(), 5000, replace=False))
        clustering = nx.average_clustering(G, nodes=sample_nodes)
    else:
        clustering = nx.average_clustering(G)
    print(f"  Clustering coefficient: {clustering:.4f}")

    # ── Degree Assortativity ──
    try:
        assortativity = nx.degree_pearson_correlation_coefficient(G)
        print(f"  Degree assortativity: {assortativity:.4f}")
    except:
        print("  Degree assortativity: could not compute (graph too small or disconnected)")

    # ── Risk Assortativity ──
    print("  Computing risk assortativity...")
    same_risk_edges = 0
    total_edges = 0
    for u, v in G.edges():
        if u < len(df) and v < len(df):
            total_edges += 1
            if df.iloc[u]['risk_category'] == df.iloc[v]['risk_category']:
                same_risk_edges += 1
    risk_assort = same_risk_edges / total_edges if total_edges > 0 else 0
    print(f"  Same-risk edge ratio: {risk_assort:.4f} ({same_risk_edges}/{total_edges})")

    # ── Degree Distribution Stats ──
    degrees = [d for n, d in G.degree()]
    print(f"  Degree distribution: min={min(degrees)}, max={max(degrees)}, "
          f"mean={np.mean(degrees):.2f}, std={np.std(degrees):.2f}")

    return {
        'num_nodes': num_nodes,
        'num_edges': num_edges,
        'n_components': n_components,
        'clustering': clustering,
        'risk_assortativity': risk_assort,
    }


# ==============================================================================
# STEP 5: COMMUNITY DETECTION
# ==============================================================================
def detect_communities(df, G):
    print("\n" + "=" * 60)
    print("STEP 5: Community Detection (Louvain)")
    print("=" * 60)

    try:
        import community as community_louvain
        partition = community_louvain.best_partition(G, random_state=42)
        df['community_id'] = pd.Series(partition).values

        n_communities = df['community_id'].nunique()
        print(f"  Detected {n_communities} communities")

        # Compute community-level risk metrics
        community_risk = df.groupby('community_id').agg(
            community_risk_pct=('risk_category', lambda x: (
                x.isin(['High', 'Very High']).sum() / len(x) * 100
            )),
            community_avg_overdue=('overdue_months', 'mean'),
            community_total_demand=('total_demand', 'sum'),
            community_size=('customer_id', 'count'),
        )

        df = df.join(community_risk, on='community_id')

        print(f"  Community risk %: [{df['community_risk_pct'].min():.1f}, "
              f"{df['community_risk_pct'].max():.1f}]")
        print(f"  Community sizes: [{df['community_size'].min()}, "
              f"{df['community_size'].max()}]")

    except ImportError:
        print("  python-louvain not installed. Skipping community detection.")
        print("  Install with: pip install python-louvain")
        df['community_id'] = -1
        df['community_risk_pct'] = 0.0
        df['community_avg_overdue'] = df['overdue_months'].mean()
        df['community_total_demand'] = 0.0
        df['community_size'] = 1

    return df


# ==============================================================================
# STEP 6: MULTI-HOP NEIGHBORHOOD SIGNALS
# ==============================================================================
def compute_multihop_signals(df, edge_index):
    print("\n" + "=" * 60)
    print("STEP 6: Multi-Hop Neighborhood Signals")
    print("=" * 60)

    from torch_scatter import scatter_mean, scatter_max

    row, col = edge_index
    num_nodes = len(df)
    overdue_tensor = torch.tensor(df['overdue_months'].values, dtype=torch.float)

    # ── 1-hop signal ──
    signal_1hop = scatter_mean(overdue_tensor[row], col, dim=0, dim_size=num_nodes)
    df['neighborhood_stress_1hop'] = signal_1hop.numpy()
    print(f"  1-hop stress: mean={df['neighborhood_stress_1hop'].mean():.2f}, "
          f"max={df['neighborhood_stress_1hop'].max():.2f}")

    # ── 2-hop signal ──
    # Build 2-hop adjacency: (row, col) @ (row, col)
    num_nodes = df.shape[0]
    adj = torch.sparse_coo_tensor(edge_index, torch.ones(edge_index.shape[1]),
                                   size=(num_nodes, num_nodes))
    adj_2 = torch.sparse.mm(adj, adj)

    # Get 2-hop edges (non-zero entries)
    adj_2_indices = adj_2.coalesce().indices()
    adj_2_values = adj_2.coalesce().values()

    # For each node, find 2-hop neighbors and aggregate
    # Use sparse matrix-vector multiplication
    signal_2hop = torch.sparse.mm(adj_2, overdue_tensor.unsqueeze(1)).squeeze(1)

    # Normalize by number of 2-hop neighbors
    deg_2 = torch.sparse.mm(adj_2, torch.ones(num_nodes, 1)).squeeze(1)
    deg_2[deg_2 == 0] = 1  # Avoid division by zero
    signal_2hop = signal_2hop / deg_2

    df['neighborhood_stress_2hop'] = signal_2hop.numpy()
    print(f"  2-hop stress: mean={df['neighborhood_stress_2hop'].mean():.2f}, "
          f"max={df['neighborhood_stress_2hop'].max():.2f}")

    # ── 3-hop signal ──
    adj_3 = torch.sparse.mm(adj_2, adj)
    signal_3hop = torch.sparse.mm(adj_3, overdue_tensor.unsqueeze(1)).squeeze(1)
    deg_3 = torch.sparse.mm(adj_3, torch.ones(num_nodes, 1)).squeeze(1)
    deg_3[deg_3 == 0] = 1
    signal_3hop = signal_3hop / deg_3

    df['neighborhood_stress_3hop'] = signal_3hop.numpy()
    print(f"  3-hop stress: mean={df['neighborhood_stress_3hop'].mean():.2f}, "
          f"max={df['neighborhood_stress_3hop'].max():.2f}")

    return df


# ==============================================================================
# STEP 7: GAT MODEL DEFINITION & TRAINING
# ==============================================================================
class GATNet(torch.nn.Module):
    """
    2-layer Graph Attention Network for node classification.

    Architecture:
      Input(8) -> GATConv(8 -> 32, heads=4) -> ReLU -> Dropout -> GATConv(128 -> 16) -> Output(5)

    Why GAT over GCN/GraphSAGE:
      - GAT learns attention weights per neighbor, so risky neighbors get higher weight
      - GCN treats all neighbors equally (bad for contagion modeling)
      - GraphSAGE samples neighbors (unnecessary since our graph is already sparse: ~5 edges/node)
      - Our graph is transductive (all nodes known at training time) -> GAT is ideal
    """
    def __init__(self, in_channels, hidden_channels, out_channels, num_classes,
                 num_heads=4, dropout=0.3):
        super(GATNet, self).__init__()

        self.conv1 = GATConv(in_channels, hidden_channels, heads=num_heads,
                             dropout=dropout, concat=True)
        # After concatenation: hidden_channels * num_heads
        self.conv2 = GATConv(hidden_channels * num_heads, out_channels, heads=1,
                             dropout=dropout, concat=False)

        self.dropout = torch.nn.Dropout(dropout)
        self.bn1 = torch.nn.BatchNorm1d(hidden_channels * num_heads)

        self.num_heads = num_heads

    def forward(self, x, edge_index, edge_attr=None):
        # Layer 1: GAT with multi-head attention
        x = self.conv1(x, edge_index, edge_attr=edge_attr.view(-1, 1) if edge_attr is not None else None)
        x = self.bn1(x)
        x = F.relu(x)
        x = self.dropout(x)

        # Layer 2: GAT with single attention head (for final embedding)
        x = self.conv2(x, edge_index)

        return x

    def get_embeddings(self, x, edge_index, edge_attr=None):
        """Forward pass returning the final embeddings (before classification)."""
        x = self.conv1(x, edge_index, edge_attr=edge_attr.view(-1, 1) if edge_attr is not None else None)
        x = self.bn1(x)
        x = F.relu(x)

        # Get embeddings from conv2 (before the final linear projection to classes)
        # We need to hook into conv2's output
        emb = self.conv2(x, edge_index, return_attention_weights=False)
        return emb


class GATWithEmbedding(GATNet):
    """Extended GAT that returns both class logits and embeddings."""
    def forward(self, x, edge_index, edge_attr=None):
        # Layer 1
        x = self.conv1(x, edge_index, edge_attr=edge_attr.view(-1, 1) if edge_attr is not None else None)
        x = self.bn1(x)
        x = F.relu(x)
        x = self.dropout(x)

        # Store embeddings before classification
        embeddings = x.clone()

        # Layer 2: produce class logits
        logits = self.conv2(x, edge_index)

        return logits, embeddings


def train_gnn(data, num_classes, epochs=100, lr=0.005):
    print("\n" + "=" * 60)
    print("STEP 7: Training GAT (Graph Attention Network)")
    print("=" * 60)
    print(f"  Architecture: GATConv(8->32, 4 heads) -> ReLU -> GATConv(128->16)")
    print(f"  Epochs: {epochs}, LR: {lr}, Device: {device}")

    model = GATWithEmbedding(
        in_channels=data.x.shape[1],
        hidden_channels=32,
        out_channels=16,  # embedding dimension
        num_classes=num_classes,
        num_heads=4,
        dropout=0.3,
    ).to(device)

    optimizer = torch.optim.Adam(model.parameters(), lr=lr, weight_decay=1e-5)
    criterion = torch.nn.CrossEntropyLoss()

    data = data.to(device)

    model.train()
    best_loss = float('inf')
    best_embeddings = None

    for epoch in range(epochs):
        optimizer.zero_grad()
        out, embeddings = model(data.x, data.edge_index,
                                edge_attr=data.edge_attr if hasattr(data, 'edge_attr') else None)

        loss = criterion(out, data.y)
        loss.backward()
        optimizer.step()

        if loss.item() < best_loss:
            best_loss = loss.item()
            best_embeddings = embeddings.cpu().detach().clone()

        if (epoch + 1) % 10 == 0:
            # Compute accuracy
            pred = out.argmax(dim=1)
            acc = (pred == data.y).sum().item() / data.y.shape[0]
            print(f"  Epoch {epoch+1}/{epochs} | Loss: {loss.item():.4f} | Acc: {acc:.4f}")

    print(f"  Training complete. Best loss: {best_loss:.4f}")
    return model, best_embeddings


# ==============================================================================
# MAIN PIPELINE
# ==============================================================================
def main():
    # ── 1. Load Data ──
    df, label_encoder = load_data(limit=200000)

    # ── 2. Build Weighted Edges ──
    edge_index, edge_attr = build_weighted_edges(df)

    # ── 3. Scale Features ──
    feature_cols = [
        'age', 'income', 'cibil_score', 'overdue_months',
        'bounce_count', 'coll_success_rate', 'occ_idx', 'reg_idx'
    ]
    scaler = StandardScaler()
    x_scaled = scaler.fit_transform(df[feature_cols])
    x = torch.tensor(x_scaled, dtype=torch.float)

    # ── 4. Compute Structural Features ──
    df, G = compute_structural_features(df, edge_index)

    # ── 5. Validate Graph ──
    validation_metrics = validate_graph_structure(df, G, edge_index)

    # ── 6. Community Detection ──
    df = detect_communities(df, G)

    # ── 7. Multi-Hop Signals ──
    df = compute_multihop_signals(df, edge_index)

    # ── 8. Prepare PyG Data for GNN ──
    y = torch.tensor(df['risk_label'].values, dtype=torch.long)

    graph_data = Data(
        x=x,
        edge_index=edge_index,
        edge_attr=edge_attr,
        y=y,
    )

    # ── 9. Train GNN ──
    num_classes = len(label_encoder.classes_)
    model, gat_embeddings = train_gnn(graph_data, num_classes, epochs=100, lr=0.005)

    # ── 10. Attach GNN Embeddings to DataFrame ──
    embedding_dim = gat_embeddings.shape[1]
    for d in range(embedding_dim):
        df[f'gat_embedding_{d}'] = gat_embeddings[:, d].numpy()

    print(f"\n  GNN embeddings attached: {embedding_dim} dimensions")

    # ── 11. Final Feature Summary ──
    print("\n" + "=" * 60)
    print("FINAL FEATURE SUMMARY")
    print("=" * 60)

    original_features = feature_cols
    structural_features = ['node_degree', 'pagerank', 'betweenness']
    community_features = ['community_risk_pct', 'community_avg_overdue',
                          'community_total_demand', 'community_size']
    multihop_features = ['neighborhood_stress_1hop', 'neighborhood_stress_2hop',
                         'neighborhood_stress_3hop']
    gat_feature_names = [f'gat_embedding_{d}' for d in range(embedding_dim)]

    all_new_features = structural_features + community_features + multihop_features + gat_feature_names

    print(f"  Original features:      {len(original_features)}")
    print(f"  Structural features:    {len(structural_features)}")
    print(f"  Community features:     {len(community_features)}")
    print(f"  Multi-hop signals:      {len(multihop_features)}")
    print(f"  GAT embeddings:         {len(gat_feature_names)}")
    print(f"  ─────────────────────────────────")
    print(f"  Total RL observation:   {len(original_features) + len(all_new_features)} dimensions")

    # ── 12. Save ──
    output_path = "rl_ready_with_graph_features.csv"
    df.to_csv(output_path, index=False)
    print(f"\n  Saved enhanced dataset to: {output_path}")
    print(f"  Total columns: {len(df.columns)}")

    # ── 13. Save Graph Validation Report ──
    report_path = "graph_validation_report.csv"
    pd.DataFrame([validation_metrics]).to_csv(report_path, index=False)
    print(f"  Saved graph validation to: {report_path}")

    print("\n" + "=" * 60)
    print("GRAPH BUILDING COMPLETE")
    print("=" * 60)

    return df, graph_data, model


if __name__ == "__main__":
    df, graph_data, model = main()
