from turtle import done

import gymnasium as gym
from gymnasium import spaces
import pandas as pd
import numpy as np
from stable_baselines3 import PPO
from stable_baselines3.common.vec_env import DummyVecEnv, VecNormalize


class DebtCollectionEnv(gym.Env):
    def __init__(self, df_path):
        super(DebtCollectionEnv, self).__init__()

        # 1. Load the Graph-Ready Data
        self.df = pd.read_csv(df_path)
        self.n_rows = len(self.df)
        self.current_row = 0
        self.steps_taken = 0
        self.max_steps = 100 # Process 100 borrowers per episode

        # 2. Action Space: [No Action, SMS/Call, Field Visit, Legal Notice]
        self.action_space = spaces.Discrete(4)

        # 3. Observation Space: 10 Features (S1, S2, S3 + GNN Signal)
        # Features: [income, cibil, overdue, bounce, coll_success, neighborhood_stress, etc.]
        self.observation_space = spaces.Box(
            low=-np.inf, high=np.inf, shape=(10,), dtype=np.float32
        )

    def _get_obs(self):
        # Extract features for the current borrower
        row = self.df.iloc[self.current_row]
        # Make sure this matches your 10 features exactly
        obs = np.array(
            [
                row["income"],
                row["cibil_score"],
                row["overdue_months"],
                row["bounce_count"],
                row["coll_success_rate"],
                row["neighborhood_stress_signal"],  # This is your Graph contribution!
                row["age"],
                row["occ_idx"],
                row["reg_idx"],
                0.0,  # padding
            ],
            dtype=np.float32,
        )
        return obs

    def reset(self, seed=None, options=None):
        super().reset(seed=seed)
        self.current_row = np.random.randint(0, self.n_rows)
        return self._get_obs(), {}

    def step(self, action):
        row = self.df.iloc[self.current_row]

        reward = 0
        done = True

        risk = row["risk_category"]

        # Cost and Base Recovery by Risk Category
        # Lower risk = cheaper actions should work, higher risk needs aggressive action
        if risk in ["Very Low", "Low"]:
            cost_map = {
                0: 0,
                1: 30,
                2: 300,
                3: 800,
            }  # Legal is very costly for low risk
            base_recovery = {
                0: 0.0,
                1: 0.4,
                2: 0.7,
                3: 0.3,
            }  # Low risk responds well to SMS
        elif risk == "Medium":
            cost_map = {0: 0, 1: 50, 2: 400, 3: 500}
            base_recovery = {0: 0.0, 1: 0.3, 2: 0.6, 3: 0.5}
        else:  # High, Very High
            cost_map = {0: 0, 1: 80, 2: 500, 3: 250}
            base_recovery = {0: 0.0, 1: 0.15, 2: 0.65, 3: 0.7}

        # 1. Action Cost
        reward -= cost_map[action]

        # 2. Recovery Probability
        success_prob = (
            base_recovery[action]
            * row["coll_success_rate"]
            * (1.0 - (row["neighborhood_stress_signal"] / 12.0))
        )

        if np.random.random() < success_prob:
            # Recovery amount varies by action type
            if action == 1:  # SMS - lower recovery
                recovered_amt = row["total_demand"] * np.random.uniform(0.05, 0.15)
            elif action == 2:  # Field Visit - higher recovery
                recovered_amt = row["total_demand"] * np.random.uniform(0.15, 0.35)
            elif action == 3:  # Legal - highest recovery
                recovered_amt = row["total_demand"] * np.random.uniform(0.20, 0.40)
            else:
                recovered_amt = 0
            reward += recovered_amt

        # 3. Penalty for wrong action on risk profile
        if risk == "Very Low" and action == 2:  # Field visit on very low risk
            reward -= (row["total_demand"] * 0.35)
        elif risk == "Very Low" and action == 3:  # Legal on very low risk
            reward -= (row["total_demand"] * 0.5)
        elif risk == "Low" and action == 3:  # Legal on low risk
            reward -= (row["total_demand"] * 0.35)
        elif risk == "Low" and action == 2:  # Field visit on low risk
            reward -= (row["total_demand"] * 0.25)
        elif risk in ["High", "Very High"] and action == 0:  # No action on high risk
            reward -= (row["total_demand"] * 0.5)
        elif risk in ["High", "Very High"] and action == 1:  # Only SMS on high risk
            reward -= (row["total_demand"] * 0.4)

        self.current_row = (self.current_row + 1) % self.n_rows
    
        self.steps_taken += 1
        done = self.steps_taken >= self.max_steps
        if done: self.steps_taken = 0
        return self._get_obs(), reward, done, False, {}


if __name__ == "__main__":
    from stable_baselines3.common.vec_env import DummyVecEnv, VecNormalize
    
    # 1. Instantiate the raw environment first
    base_env = DebtCollectionEnv("rl_ready_with_graph_features.csv")
    
    # Grab the n_rows BEFORE wrapping it to fix the AttributeError
    num_rows = base_env.n_rows
    print(f"Training on {num_rows} data points...")
    
    # 2. Wrap it in DummyVecEnv (requires a callable/lambda)
    env = DummyVecEnv([lambda: base_env])
    
    # 3. Wrap it in VecNormalize to fix the feature scale differences (Income vs CIBIL vs Graph Signal)
    env = VecNormalize(env, norm_obs=True, norm_reward=True, clip_obs=10.0)

    # 4. Initialize the PPO Model (Forcing CUDA as discussed)
    model = PPO("MlpPolicy", env, verbose=1, learning_rate=0.0003, n_steps=2048, device="cuda")

    print("Training the Graph-Based RL Agent...")
    model.learn(total_timesteps=500000)

    # 5. Save BOTH the model and the Normalization statistics
    model.save("graph_rl_debt_model")
    env.save("vec_normalize.pkl")  # <--- CRUCIAL: Save the scaling stats
    print("Model and Normalization Stats Saved Successfully!")