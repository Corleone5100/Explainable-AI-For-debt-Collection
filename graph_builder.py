import pandas as pd
import numpy as np
import torch
from sqlalchemy import create_engine
from torch_geometric.data import Data
from sklearn.preprocessing import StandardScaler

device = torch.device('cuda') if torch.cuda.is_available() else torch.device('cpu')
# 1. Database Connection
engine = create_engine("postgresql://postgres:Bishal5100#@localhost:5432/debt_market_db")

def build_contagion_graph():
    print("--- Step 1: Loading Data from Postgres ---")
    # Load 50k for testing first, then scale to 200k
    query = "SELECT * FROM master_env_data LIMIT 200000" 
    df = pd.read_sql(query, engine)

    # 2. Node Feature Selection (S1, S2, S3)
    # We convert categorical to numeric for the GNN
    df['occ_idx'] = df['occupation'].astype('category').cat.codes
    df['reg_idx'] = df['region'].astype('category').cat.codes
    
    features = [
        'age', 'income', 'cibil_score', 'overdue_months', 
        'bounce_count', 'coll_success_rate', 'occ_idx', 'reg_idx'
    ]
    
    # Scale features
    scaler = StandardScaler()
    x = torch.tensor(scaler.fit_transform(df[features]), dtype=torch.float)

    print("--- Step 2: Creating Edges (Contagion Links) ---")
    # Logic: Connect if (Region == Region) AND (Occupation == Occupation)
    # To optimize for 200k, we use grouping
    edge_list = []
    grouped = df.groupby(['region', 'occupation'])

    for name, group in grouped:
        indices = group.index.values
        # Create edges between all members of the group (Cliques)
        # Note: For very large groups, we sample edges to save memory
        if len(indices) > 1:
            for i in range(len(indices)):
                # Connect to a few random neighbors in the same group to keep it sparse
                neighbors = np.random.choice(indices, size=min(len(indices), 5), replace=False)
                for nb in neighbors:
                    if i != nb:
                        edge_list.append([indices[i], nb])

    edge_index = torch.tensor(edge_list, dtype=torch.long).t().contiguous()

    # 3. Create PyG Data Object
    graph_data = Data(x=x, edge_index=edge_index)
    
    print(f"Graph Created with {graph_data.num_nodes} nodes and {graph_data.num_edges} edges.")
    return graph_data, df

# Execute
borrower_graph, borrower_df = build_contagion_graph()

# --- Step 3: Simple Graph Feature Extraction (Pre-RL) ---
# Before we train the full GNN, let's calculate the 'Neighborhood Default Rate'
# which acts as the 'Contagion Signal' for the RL agent.

def calculate_neighborhood_signal(data, df):
    # This simulates a single pass of a GNN
    row, col = data.edge_index
    overdue_tensor = torch.tensor(df['overdue_months'].values, dtype=torch.float)
    
    # Aggregate: Calculate average overdue months of neighbors
    neighborhood_overdue = torch.zeros(data.num_nodes)
    # Use scatter_mean to find avg neighbor risk
    from torch_scatter import scatter_mean
    neighborhood_overdue = scatter_mean(overdue_tensor[row], col, dim=0, dim_size=data.num_nodes)
    
    df['neighborhood_stress_signal'] = neighborhood_overdue.numpy()
    return df

final_df = calculate_neighborhood_signal(borrower_graph, borrower_df)
final_df.to_csv("rl_ready_with_graph_features.csv", index=False)
print("Saved 'rl_ready_with_graph_features.csv' with Network Intelligence.")