import pandas as pd
import numpy as np
import torch
from stable_baselines3 import PPO
from stable_baselines3.common.vec_env import DummyVecEnv, VecNormalize
from debt_env import DebtCollectionEnv # Ensure your env class is in a file named debt_env.py

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
def get_recommendation(borrower_index):
    row = df.iloc[borrower_index]
    
    # Prepare the observation (Must match your Env's _get_obs logic)
    obs = np.array([
        row['income'], row['cibil_score'], row['overdue_months'], 
        row['bounce_count'], row['coll_success_rate'], 
        row['neighborhood_stress_signal'],
        row['age'], row['occ_idx'], row['reg_idx'], 0.0
    ], dtype=np.float32)

    # NORMALIZE the observation using the saved VecNormalize stats
    obs_normalized = env.normalize_obs(obs)

    # 2. Get the Action and the Confidence (Probability)
    # action: 0=None, 1=SMS, 2=Visit, 3=Legal
    action, _states = model.predict(obs_normalized, deterministic=True)
    action = int(action)  # Ensure it's an integer for indexing
    # 3. Get Action Probabilities (How 'sure' is the agent?)
    # This helps you see if it's struggling between two choices
    obs_tensor = torch.tensor(obs_normalized).unsqueeze(0).to(model.device)
    with torch.no_grad():
        dist = model.policy.get_distribution(obs_tensor)
        probs = dist.distribution.probs.cpu().numpy()[0]

    action_map = {0: "No Action", 1: "SMS/Call", 2: "Field Visit", 3: "Legal Notice"}
    
    print(f"--- Recommendation for Borrower {row['customer_id']} ---")
    print(f"Risk: {row['risk_category']} | CIBIL: {row['cibil_score']}")
    print(f"Neighborhood Stress: {row['neighborhood_stress_signal']:.2f} (Contagion Source)")
    print(f"Collector Tier: {row['coll_tier']} ({row['coll_success_rate']})")
    print(f"Resulting Action: {action_map[action]}")
    print(f"Confidence: {probs[action]*100:.1f}%")
    print("-" * 40)

# Test on 5 different borrowers
for i in range(0, 10000, 5):
    get_recommendation(i)