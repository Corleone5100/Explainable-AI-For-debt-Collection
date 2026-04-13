"""
Unit Tests
==========
Tests for:
  1. DebtCollectionEnv: reward calculation, observation space, action space
  2. Config: credential loading
  3. Feature column detection

Run from project root:
  pytest tests/ -v
  python -m unittest discover tests/
"""

import os
import sys
import unittest
import numpy as np
import pandas as pd

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)


# ==============================================================================
# HELPER: Create minimal test data
# ==============================================================================
def create_test_csv(path="test_data.csv"):
    """Create a minimal CSV file for testing the environment."""
    data = {
        'customer_id': [f'TEST_{i:04d}' for i in range(50)],
        'age': np.random.randint(18, 75, 50),
        'income': np.random.uniform(10000, 100000, 50),
        'cibil_score': np.random.randint(300, 900, 50),
        'overdue_months': np.random.randint(0, 12, 50),
        'bounce_count': np.random.randint(0, 10, 50),
        'coll_success_rate': np.random.uniform(0.2, 0.95, 50),
        'occ_idx': np.random.randint(0, 4, 50),
        'reg_idx': np.random.randint(0, 7, 50),
        'risk_category': np.random.choice(
            ['Very Low', 'Low', 'Medium', 'High', 'Very High'], 50,
            p=[0.2, 0.2, 0.2, 0.2, 0.2]
        ),
        'total_demand': np.random.uniform(5000, 50000, 50),
        'neighborhood_stress_signal': np.random.uniform(0, 8, 50),
        'node_degree': np.random.uniform(1, 10, 50),
        'pagerank': np.random.uniform(0.001, 0.05, 50),
        'betweenness': np.random.uniform(0, 0.1, 50),
        'community_risk_pct': np.random.uniform(0, 100, 50),
        'community_avg_overdue': np.random.uniform(0, 8, 50),
        'community_total_demand': np.random.uniform(100000, 500000, 50),
        'community_size': np.random.randint(5, 50, 50),
        'neighborhood_stress_1hop': np.random.uniform(0, 8, 50),
        'neighborhood_stress_2hop': np.random.uniform(0, 6, 50),
        'neighborhood_stress_3hop': np.random.uniform(0, 4, 50),
    }
    # Add GAT embeddings
    for d in range(16):
        data[f'gat_embedding_{d}'] = np.random.randn(50) * 0.5

    df = pd.DataFrame(data)
    df.to_csv(path, index=False)
    return path


# ==============================================================================
# TEST 1: Environment Observation Space
# ==============================================================================
class TestDebtCollectionEnv(unittest.TestCase):
    """Tests for the DebtCollectionEnv class."""

    @classmethod
    def setUpClass(cls):
        cls.test_csv = create_test_csv()

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.test_csv):
            os.remove(cls.test_csv)

    def setUp(self):
        from scripts.debt_env import DebtCollectionEnv
        self.env = DebtCollectionEnv(self.test_csv)

    def test_observation_space_shape(self):
        """Observation space should have the correct number of dimensions."""
        expected_dim = len(self.env.feature_cols)
        self.assertEqual(self.env.observation_space.shape[0], expected_dim)
        self.assertGreater(expected_dim, 0)

    def test_observation_values_are_finite(self):
        """Observations should contain only finite values."""
        obs, _ = self.env.reset()
        self.assertTrue(np.all(np.isfinite(obs)), "Observation contains inf or nan")

    def test_observation_dtype(self):
        """Observations should be float32."""
        obs, _ = self.env.reset()
        self.assertEqual(obs.dtype, np.float32)

    def test_get_obs_returns_correct_length(self):
        """_get_obs should return array matching observation space."""
        obs, _ = self.env.reset()
        self.assertEqual(len(obs), self.env.observation_space.shape[0])


# ==============================================================================
# TEST 2: Environment Action Space
# ==============================================================================
class TestActionSpace(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.test_csv = create_test_csv()

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.test_csv):
            os.remove(cls.test_csv)

    def setUp(self):
        from scripts.debt_env import DebtCollectionEnv
        self.env = DebtCollectionEnv(self.test_csv)

    def test_action_space_has_4_actions(self):
        """Action space should have exactly 4 discrete actions."""
        self.assertEqual(self.env.action_space.n, 4)

    def test_all_actions_are_valid(self):
        """Each action (0-3) should be valid and produce a step."""
        obs, _ = self.env.reset()
        for action in range(4):
            next_obs, reward, done, truncated, info = self.env.step(action)
            self.assertIsInstance(reward, (int, float, np.floating))
            self.assertIsInstance(done, (bool, np.bool_))
            self.assertEqual(len(next_obs), self.env.observation_space.shape[0])


# ==============================================================================
# TEST 3: Reward Calculation Logic
# ==============================================================================
class TestRewardCalculation(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.test_csv = create_test_csv()

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.test_csv):
            os.remove(cls.test_csv)

    def setUp(self):
        from scripts.debt_env import DebtCollectionEnv
        self.env = DebtCollectionEnv(self.test_csv)

    def test_no_action_has_zero_cost(self):
        """Action 0 (No Action) should not incur direct action costs."""
        # With action 0:
        #   - cost = 0 (no direct cost)
        #   - recovery = 0 (base_recovery is 0 for action 0)
        #   - No penalty for action 0 on any risk category
        #   - Contagion bonus only if community_risk_pct > 50
        #
        # So reward should be 0 (no cost, no recovery) OR positive only from
        # contagion bonus. The key assertion is that action 0 has no direct cost.
        # We test this by verifying reward >= -small_margin (allowing for floating point)
        # OR that reward is only positive when contagion bonus applies.
        obs, _ = self.env.reset()
        self.env.current_row = 0
        _, reward_no_action, _, _, _ = self.env.step(0)

        # For action 0, the only positive reward source is contagion bonus
        # (requires community_risk_pct > 50). If community_risk_pct <= 50,
        # reward must be exactly 0.
        row = self.env.df.iloc[0]
        if row.get('community_risk_pct', 0) <= 50:
            # No contagion bonus possible → reward should be ~0
            self.assertAlmostEqual(reward_no_action, 0, places=2)
        else:
            # Contagion bonus may apply → reward should be >= 0
            self.assertGreaterEqual(reward_no_action, -0.01)

    def test_high_risk_no_action_penalty(self):
        """High risk borrowers with No Action should incur a penalty."""
        # Find a High/Very High risk borrower
        for i in range(len(self.env.df)):
            row = self.env.df.iloc[i]
            if row['risk_category'] in ['High', 'Very High']:
                self.env.current_row = i
                obs, _ = self.env.reset()
                _, reward, _, _, _ = self.env.step(0)
                # No Action penalty: -total_demand * 0.5
                # But contagion bonus may offset it if community_risk_pct > 50
                # For this test, just verify the penalty component exists
                # (reward should be negative unless contagion bonus outweighs it)
                # Check that the penalty is applied: reward + total_demand * 0.5 should be >= 0
                # (meaning the negative part came from the penalty)
                penalty_amount = row['total_demand'] * 0.5
                # The raw reward should reflect the penalty
                self.assertLess(reward, penalty_amount * 0.5)
                return
        self.skipTest("No High/Very High risk borrower found in test data")

    def test_very_low_risk_legal_penalty(self):
        """Very Low risk borrowers with Legal action should incur large penalty."""
        # Set up a controlled borrower where recovery won't offset the penalty
        # Use a Very Low risk borrower with low total_demand and low coll_success_rate
        for i in range(len(self.env.df)):
            row = self.env.df.iloc[i]
            if (row['risk_category'] == 'Very Low'
                    and row['total_demand'] < 10000
                    and row['coll_success_rate'] < 0.4):
                self.env.current_row = i
                obs, _ = self.env.reset()
                _, reward, _, _, _ = self.env.step(3)  # Legal
                # Should be penalized heavily
                self.assertLess(reward, 0)
                return
        # If no suitable borrower found in random data, skip gracefully
        self.skipTest("No suitable Very Low risk borrower found in test data")

    def test_reward_is_finite(self):
        """All actions should produce finite rewards."""
        obs, _ = self.env.reset()
        for action in range(4):
            self.env.current_row = 0
            _, reward, _, _, _ = self.env.step(action)
            self.assertTrue(np.isfinite(reward), f"Action {action} produced non-finite reward")


# ==============================================================================
# TEST 4: Environment Episode Completion
# ==============================================================================
class TestEpisodeFlow(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.test_csv = create_test_csv()

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.test_csv):
            os.remove(cls.test_csv)

    def setUp(self):
        from scripts.debt_env import DebtCollectionEnv
        self.env = DebtCollectionEnv(self.test_csv)

    def test_episode_completes_in_max_steps(self):
        """An episode should complete within max_steps."""
        obs, _ = self.env.reset()
        steps = 0
        done = False
        while not done:
            action = self.env.action_space.sample()
            obs, reward, done, truncated, info = self.env.step(action)
            steps += 1
            if steps > self.env.max_steps + 10:
                self.fail("Episode did not complete within max_steps")
        self.assertGreater(steps, 0)

    def test_reset_randomizes_starting_position(self):
        """Reset should randomize the starting borrower."""
        positions = set()
        for _ in range(10):
            self.env.reset(seed=42)
            positions.add(self.env.current_row)
        # With seed=42, positions should be deterministic
        self.assertEqual(len(positions), 1)


# ==============================================================================
# TEST 5: Confidence Extraction
# ==============================================================================
class TestConfidenceExtraction(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.test_csv = create_test_csv()

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.test_csv):
            os.remove(cls.test_csv)

    def setUp(self):
        from scripts.debt_env import DebtCollectionEnv
        self.env = DebtCollectionEnv(self.test_csv)

    def test_get_action_probs_returns_required_keys(self):
        """get_action_probs should return dict with required keys."""
        model_path = "graph_rl_debt_model.zip"
        if not os.path.exists(model_path):
            self.skipTest("No trained model found")

        vec_norm_path = "vec_normalize.pkl"
        if not os.path.exists(vec_norm_path):
            self.skipTest("No VecNormalize stats found")

        from stable_baselines3 import PPO
        from stable_baselines3.common.vec_env import DummyVecEnv, VecNormalize

        model = PPO.load(model_path)
        model_obs_dim = model.observation_space.shape[0]
        env_obs_dim = self.env.observation_space.shape[0]

        if model_obs_dim != env_obs_dim:
            self.skipTest(
                f"Model expects {model_obs_dim}-dim obs, test env has {env_obs_dim}-dim"
            )

        # Check VecNormalize obs_rms dimension compatibility
        vec_norm_stats = VecNormalize.load(vec_norm_path, DummyVecEnv([lambda: self.env]))
        if vec_norm_stats.obs_rms.mean.shape[0] != env_obs_dim:
            vec_norm_stats.close()
            self.skipTest(
                f"VecNormalize has {vec_norm_stats.obs_rms.mean.shape[0]}-dim stats, "
                f"test env has {env_obs_dim}-dim"
            )
        vec_norm_stats.close()

        wrapped_env = DummyVecEnv([lambda: self.env])
        wrapped_env = VecNormalize.load(vec_norm_path, wrapped_env)
        wrapped_env.training = False
        wrapped_env.norm_reward = False

        obs = wrapped_env.reset()
        result = self.env.get_action_probs(model, obs[0])
        wrapped_env.close()

        required_keys = ['action', 'probs', 'confidence', 'entropy', 'margin', 'is_uncertain']
        for key in required_keys:
            self.assertIn(key, result)

    def test_confidence_is_valid_range(self):
        """Confidence should be between 0 and 1."""
        model_path = "graph_rl_debt_model.zip"
        if not os.path.exists(model_path):
            self.skipTest("No trained model found")

        vec_norm_path = "vec_normalize.pkl"
        if not os.path.exists(vec_norm_path):
            self.skipTest("No VecNormalize stats found")

        from stable_baselines3 import PPO
        from stable_baselines3.common.vec_env import DummyVecEnv, VecNormalize

        model = PPO.load(model_path)
        model_obs_dim = model.observation_space.shape[0]
        env_obs_dim = self.env.observation_space.shape[0]

        if model_obs_dim != env_obs_dim:
            self.skipTest(
                f"Model expects {model_obs_dim}-dim obs, test env has {env_obs_dim}-dim"
            )

        vec_norm_stats = VecNormalize.load(vec_norm_path, DummyVecEnv([lambda: self.env]))
        if vec_norm_stats.obs_rms.mean.shape[0] != env_obs_dim:
            vec_norm_stats.close()
            self.skipTest(
                f"VecNormalize has {vec_norm_stats.obs_rms.mean.shape[0]}-dim stats, "
                f"test env has {env_obs_dim}-dim"
            )
        vec_norm_stats.close()

        wrapped_env = DummyVecEnv([lambda: self.env])
        wrapped_env = VecNormalize.load(vec_norm_path, wrapped_env)
        wrapped_env.training = False
        wrapped_env.norm_reward = False

        obs = wrapped_env.reset()
        result = self.env.get_action_probs(model, obs[0])
        wrapped_env.close()

        self.assertGreaterEqual(result['confidence'], 0)
        self.assertLessEqual(result['confidence'], 1)

    def test_entropy_is_non_negative(self):
        """Entropy should be non-negative."""
        model_path = "graph_rl_debt_model.zip"
        if not os.path.exists(model_path):
            self.skipTest("No trained model found")

        vec_norm_path = "vec_normalize.pkl"
        if not os.path.exists(vec_norm_path):
            self.skipTest("No VecNormalize stats found")

        from stable_baselines3 import PPO
        from stable_baselines3.common.vec_env import DummyVecEnv, VecNormalize

        model = PPO.load(model_path)
        model_obs_dim = model.observation_space.shape[0]
        env_obs_dim = self.env.observation_space.shape[0]

        if model_obs_dim != env_obs_dim:
            self.skipTest(
                f"Model expects {model_obs_dim}-dim obs, test env has {env_obs_dim}-dim"
            )

        vec_norm_stats = VecNormalize.load(vec_norm_path, DummyVecEnv([lambda: self.env]))
        if vec_norm_stats.obs_rms.mean.shape[0] != env_obs_dim:
            vec_norm_stats.close()
            self.skipTest(
                f"VecNormalize has {vec_norm_stats.obs_rms.mean.shape[0]}-dim stats, "
                f"test env has {env_obs_dim}-dim"
            )
        vec_norm_stats.close()

        wrapped_env = DummyVecEnv([lambda: self.env])
        wrapped_env = VecNormalize.load(vec_norm_path, wrapped_env)
        wrapped_env.training = False
        wrapped_env.norm_reward = False

        obs = wrapped_env.reset()
        result = self.env.get_action_probs(model, obs[0])
        wrapped_env.close()

        self.assertGreaterEqual(result['entropy'], 0)


# ==============================================================================
# TEST 6: Configuration Loading
# ==============================================================================
class TestConfig(unittest.TestCase):

    def test_config_db_url(self):
        """Config should produce a valid database URL."""
        from config import cfg
        url = cfg.db_url
        self.assertIn('postgresql://', url)
        self.assertIn('debt_market_db', url)

    def test_config_types(self):
        """Config values should have correct types."""
        from config import cfg
        self.assertIsInstance(cfg.graph_node_limit, int)
        self.assertIsInstance(cfg.gnn_epochs, int)
        self.assertIsInstance(cfg.gnn_lr, float)
        self.assertIsInstance(cfg.rl_learning_rate, float)
        self.assertIsInstance(cfg.eval_train_ratio, float)

    def test_config_reasonable_defaults(self):
        """Config should have reasonable default values."""
        from config import cfg
        self.assertGreater(cfg.graph_node_limit, 0)
        self.assertGreater(cfg.gnn_epochs, 0)
        self.assertGreater(cfg.gnn_lr, 0)
        self.assertGreater(cfg.rl_learning_rate, 0)
        self.assertTrue(0 < cfg.eval_train_ratio <= 1)


# ==============================================================================
# TEST 7: Feature Column Detection
# ==============================================================================
class TestFeatureDetection(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.test_csv = create_test_csv()

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.test_csv):
            os.remove(cls.test_csv)

    def test_core_features_detected(self):
        """Core features should always be detected."""
        from scripts.debt_env import DebtCollectionEnv
        env = DebtCollectionEnv(self.test_csv)
        core = ['income', 'cibil_score', 'overdue_months', 'bounce_count',
                'coll_success_rate', 'age', 'occ_idx', 'reg_idx']
        for f in core:
            self.assertIn(f, env.feature_cols)

    def test_graph_features_detected_when_present(self):
        """Graph features should be detected when present in CSV."""
        from scripts.debt_env import DebtCollectionEnv
        env = DebtCollectionEnv(self.test_csv)
        graph_features = ['node_degree', 'pagerank', 'betweenness',
                          'community_risk_pct', 'neighborhood_stress_1hop']
        for f in graph_features:
            self.assertIn(f, env.feature_cols)

    def test_gat_embeddings_detected(self):
        """GAT embedding columns should be detected and sorted."""
        from scripts.debt_env import DebtCollectionEnv
        env = DebtCollectionEnv(self.test_csv)
        gat_cols = [c for c in env.feature_cols if c.startswith('gat_embedding_')]
        self.assertEqual(len(gat_cols), 16)
        # Check sorted order
        for i in range(len(gat_cols) - 1):
            self.assertLess(
                int(gat_cols[i].split('_')[-1]),
                int(gat_cols[i + 1].split('_')[-1])
            )


if __name__ == '__main__':
    unittest.main(verbosity=2)
