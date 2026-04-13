"""
Configuration Module
====================
Loads settings from .env file and provides typed access to all tunable parameters.

Usage:
    from config import cfg
    print(cfg.db_url)
    print(cfg.gnn_epochs)
"""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv

# -- Load .env file --
_env_path = Path(__file__).parent / '.env'
if _env_path.exists():
    load_dotenv(_env_path)
else:
    # Fallback to environment variables
    pass

# -- Setup Logging --
LOG_DIR = Path(__file__).parent / 'logs'
LOG_DIR.mkdir(exist_ok=True)

# Use UTF-8 encoding for log files to avoid Windows cp1252 encoding errors
file_handler = logging.FileHandler(LOG_DIR / 'pipeline.log', encoding='utf-8')
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
))

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(
    '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
))

logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, console_handler],
)

logger = logging.getLogger('debt_collection')


class Config:
    """Centralized configuration loaded from .env file."""

    # -- Database --
    @property
    def db_host(self):
        return os.getenv('DB_HOST', 'localhost')

    @property
    def db_port(self):
        return os.getenv('DB_PORT', '5432')

    @property
    def db_name(self):
        return os.getenv('DB_NAME', 'debt_market_db')

    @property
    def db_user(self):
        return os.getenv('DB_USER', 'postgres')

    @property
    def db_password(self):
        return os.getenv('DB_PASSWORD', '')

    @property
    def db_url(self):
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

    # -- Graph Builder --
    @property
    def graph_node_limit(self):
        return int(os.getenv('GRAPH_NODE_LIMIT', '200000'))

    @property
    def gnn_epochs(self):
        return int(os.getenv('GNN_EPOCHS', '100'))

    @property
    def gnn_lr(self):
        return float(os.getenv('GNN_LR', '0.005'))

    @property
    def gnn_hidden(self):
        return int(os.getenv('GNN_HIDDEN', '32'))

    @property
    def gnn_heads(self):
        return int(os.getenv('GNN_HEADS', '4'))

    @property
    def gnn_dropout(self):
        return float(os.getenv('GNN_DROPOUT', '0.3'))

    # -- RL Training --
    @property
    def rl_learning_rate(self):
        return float(os.getenv('RL_LEARNING_RATE', '0.0003'))

    @property
    def rl_n_steps(self):
        return int(os.getenv('RL_N_STEPS', '2048'))

    @property
    def rl_total_timesteps(self):
        return int(os.getenv('RL_TOTAL_TIMESTEPS', '500000'))

    @property
    def rl_device(self):
        return os.getenv('RL_DEVICE', 'cuda')

    # -- Evaluation --
    @property
    def eval_train_ratio(self):
        return float(os.getenv('EVAL_TRAIN_RATIO', '0.8'))

    @property
    def eval_n_episodes(self):
        return int(os.getenv('EVAL_N_EPISODES', '50'))

    @property
    def eval_n_seeds(self):
        return int(os.getenv('EVAL_N_SEEDS', '3'))

    @property
    def eval_ablation_features(self):
        return os.getenv('EVAL_ABLATION_FEATURES', 'graph,structural,community,multihop').split(',')

    # -- Feature Set Selection --
    @property
    def feature_set(self):
        """
        Feature set to use for training.

        Options:
          - 'optimal':  +all_graph (16 features: core + community + 1-hop + GAT embeddings)
                        Recommended for production. Fast training, best performance.
          - 'full':     All 150 features including multi-dimensional GAT embeddings.
                        Requires 500K+ timesteps (5M recommended).
          - 'ablation': Run all feature subsets for analysis (evaluation only).

        Per the ablation study:
          - Community features are the single most important addition (+Rs. 0.99M alone)
          - Structural features (degree, pagerank, betweenness) HURT performance
          - Multi-hop signals (2-hop, 3-hop) add noise; 1-hop is sufficient
          - Full 150-feature model fails catastrophically with only 50K timesteps
        """
        return os.getenv('FEATURE_SET', 'optimal')

    @property
    def curriculum_learning(self):
        """
        Enable curriculum learning for the RL agent.

        When True, training starts with only High/Very High risk borrowers
        (where the agent performs well) and gradually introduces Medium,
        Low, and Very Low risk borrowers. This helps the agent learn
        good policies for the most profitable cases first, then generalize.

        Phases (each phase = fraction of total timesteps):
          Phase 1 (0-25%):   High + Very High only
          Phase 2 (25-50%):  High + Very High + Medium
          Phase 3 (50-100%): All risk categories
        """
        return os.getenv('CURRICULUM_LEARNING', 'true').lower() == 'true'

    # -- Visualization --
    @property
    def viz_sample_size(self):
        return int(os.getenv('VIZ_SAMPLE_SIZE', '100'))


cfg = Config()
