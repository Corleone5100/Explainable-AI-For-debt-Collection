"""
Testing Script — Run the trained model on sample borrowers.

Run from project root:
  python scripts/testing.py
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import pandas as pd
import numpy as np
import torch
from stable_baselines3 import PPO
from stable_baselines3.common.vec_env import DummyVecEnv, VecNormalize
from scripts.debt_env import DebtCollectionEnv, ACTION_NAMES

# 1. Load the Model and the Data
df = pd.read_csv("rl_ready_with_graph_features.csv")

# 2. Recreate the environment and load the Normalization Stats
raw_env = DebtCollectionEnv("rl_ready_with_graph_features.csv")
env = DummyVecEnv([lambda: raw_env])
env = VecNormalize.load("vec_normalize.pkl", env)

# Turn off training mode for normalization so it doesn't update stats during testing
env.training = False
env.norm_reward = False

# 3. Load the Model
model = PPO.load("graph_rl_debt_model")

# Collect all results
results = []

def get_recommendation(borrower_index):
    row = df.iloc[borrower_index]

    # Use the environment's _get_obs() to ensure feature alignment
    raw_env.current_row = borrower_index % raw_env.n_rows
    obs_raw = raw_env._get_obs()

    # Normalize the observation using the saved VecNormalize stats
    obs_normalized = env.normalize_obs(obs_raw)

    # Get the Action and the Confidence
    action, _states = model.predict(obs_normalized, deterministic=True)
    action = int(action)

    # Get Action Probabilities (How 'sure' is the agent?)
    obs_tensor = torch.tensor(obs_normalized).unsqueeze(0).to(model.device)
    with torch.no_grad():
        dist = model.policy.get_distribution(obs_tensor)
        probs = dist.distribution.probs.cpu().numpy()[0]

    # Determine neighborhood stress column (new or legacy)
    if 'neighborhood_stress_1hop' in row:
        stress_val = float(row['neighborhood_stress_1hop'])
        stress_label = "Neighborhood Stress (1-hop)"
    elif 'neighborhood_stress_signal' in row:
        stress_val = float(row['neighborhood_stress_signal'])
        stress_label = "Neighborhood Stress"
    else:
        stress_val = 0.0
        stress_label = "Neighborhood Stress (N/A)"

    # Build result dict
    result = {
        "borrower_index": borrower_index,
        "customer_id": str(row.get('customer_id', 'unknown')),
        "risk_category": row['risk_category'],
        "cibil_score": float(row['cibil_score']),
        "overdue_months": float(row['overdue_months']),
        "total_demand": float(row.get('total_demand', 0)),
        "coll_success_rate": float(row['coll_success_rate']),
        "collector_tier": str(row.get('coll_tier', 'N/A')),
        "neighborhood_stress": {
            "label": stress_label,
            "value": round(stress_val, 2),
        },
        "recommended_action": ACTION_NAMES[action],
        "confidence_pct": round(float(probs[action]) * 100, 1),
        "all_action_probabilities": {
            ACTION_NAMES[i]: round(float(p) * 100, 1) for i, p in enumerate(probs)
        },
    }
    results.append(result)

    # Print to console
    print(f"--- Recommendation for Borrower {result['customer_id']} ---")
    print(f"Risk: {result['risk_category']} | CIBIL: {result['cibil_score']}")
    print(f"{stress_label}: {stress_val:.2f}")
    print(f"Collector Tier: {result['collector_tier']} ({result['coll_success_rate']})")
    print(f"Resulting Action: {ACTION_NAMES[action]}")
    print(f"Confidence: {result['confidence_pct']}% | All probs: {[f'{ACTION_NAMES[i]}={p*100:.0f}%' for i, p in enumerate(probs)]}")
    print("-" * 40)

# Test on borrowers (every 5th up to 10000, capped by data size)
max_index = min(10000, len(df))
print(f"\nTesting on {len(range(0, max_index, 5))} borrowers...\n")
for i in range(0, max_index, 5):
    get_recommendation(i)

# Save results to JSON
output_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "evaluation_outputs", "testing_results.json")
os.makedirs(os.path.dirname(output_path), exist_ok=True)

output = {
    "model": "graph_rl_debt_model",
    "data_source": "rl_ready_with_graph_features.csv",
    "total_borrowers_tested": len(results),
    "recommendations": results,
}

with open(output_path, 'w') as f:
    json.dump(output, f, indent=2, default=str)

print(f"\n✅ Results saved to: {output_path}")
print(f"   Total borrowers tested: {len(results)}")
