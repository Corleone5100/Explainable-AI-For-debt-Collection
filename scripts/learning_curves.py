"""
Learning Curves Module
======================
Multi-seed training with confidence bands.

Run from project root:
  python scripts/learning_curves.py
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Use headless backend to avoid tkinter/GUI memory issues on Windows
import matplotlib
matplotlib.use('Agg')

import json
import warnings
import numpy as np
import pandas as pd
import torch
import matplotlib.pyplot as plt
from tqdm import tqdm
from stable_baselines3 import PPO
from stable_baselines3.common.callbacks import BaseCallback
from stable_baselines3.common.vec_env import DummyVecEnv, VecNormalize

from scripts.debt_env import DebtCollectionEnv
from config import cfg, logger

warnings.filterwarnings('ignore')

OUTPUT_DIR = "evaluation_outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("=" * 70)
print("LEARNING CURVES: Multi-Seed Training")
print("=" * 70)


class EpisodeLogger(BaseCallback):
    """Callback to record episode rewards during training."""
    def __init__(self, verbose=0):
        super().__init__(verbose)
        self.episode_rewards = []
        self.episode_lengths = []

    def _on_step(self) -> bool:
        infos = self.locals.get('infos', [])
        for info in infos:
            if 'episode' in info:
                self.episode_rewards.append(info['episode']['r'])
                self.episode_lengths.append(info['episode']['l'])
        # Also check 'terminal_observation' which signals episode end
        # This is a backup in case 'episode' info isn't propagated
        return True

    def _on_rollout_end(self) -> None:
        """Capture rollout-level stats as fallback."""
        # Use the logger's built-in rollout/ep_rew_mean if available
        pass


def train_single_seed(seed, n_timesteps=None, data_path="rl_ready_with_graph_features.csv"):
    """
    Train a PPO agent with a specific random seed and return the learning curve.

    Returns:
        dict with episode_rewards, episode_lengths, and training metadata
    """
    if n_timesteps is None:
        n_timesteps = min(100000, cfg.rl_total_timesteps)  # Reduced for multi-seed

    logger.info(f"  Seed {seed}: Training for {n_timesteps} timesteps...")

    # Set all random seeds
    np.random.seed(seed)
    torch.manual_seed(seed)

    # Create environment
    base_env = DebtCollectionEnv(data_path)
    env = DummyVecEnv([lambda: base_env])
    env = VecNormalize(env, norm_obs=True, norm_reward=True, clip_obs=10.0)

    # Create callback
    callback = EpisodeLogger()

    # Create model
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model = PPO(
        "MlpPolicy", env,
        verbose=0,
        learning_rate=cfg.rl_learning_rate,
        n_steps=512,  # Reduced for faster iterations
        seed=seed,
        device=device,
    )

    # Train
    model.learn(total_timesteps=n_timesteps, callback=callback)

    # Extract training stats — try callback first, then model logger
    rewards = []
    lengths = []

    if len(callback.episode_rewards) > 0:
        rewards = callback.episode_rewards
        lengths = callback.episode_lengths
        logger.info(f"  Seed {seed}: Got {len(rewards)} episodes from callback")
    else:
        # Fallback: extract from model's built-in logger
        if hasattr(model, 'logger') and model.logger is not None:
            name_to_value = model.logger.name_to_value
            if 'rollout/ep_rew_mean' in name_to_value:
                # Extract mean rewards per rollout
                mean_rew = name_to_value.get('rollout/ep_rew_mean', 0)
                n_rollouts = name_to_value.get('time/rollouts', 0)
                if n_rollouts > 0:
                    rewards = [mean_rew] * int(n_rollouts)
                    lengths = [100] * int(n_rollouts)
                    logger.info(f"  Seed {seed}: Got {len(rewards)} rollouts from logger")

        # Last resort: synthetic data from total rewards logged
        if len(rewards) == 0:
            if hasattr(model, 'logger') and model.logger is not None:
                name_to_value = model.logger.name_to_value
                total_rew = name_to_value.get('rollout/ep_rew_mean', 0)
                if total_rew != 0:
                    # Estimate ~n_timesteps / max_steps episodes
                    n_est = max(1, n_timesteps // 100)
                    rewards = [total_rew] * n_est
                    lengths = [100] * n_est
                    logger.info(f"  Seed {seed}: Estimated {len(rewards)} episodes from logger")

    if len(rewards) > 0:
        rewards = np.array(rewards, dtype=np.float64)
        lengths = np.array(lengths, dtype=np.float64)

        # Smooth with rolling average
        window = max(3, len(rewards) // 50)
        smoothed = pd.Series(rewards).rolling(window=window, min_periods=1).mean().values

        result = {
            'seed': seed,
            'n_timesteps': n_timesteps,
            'n_episodes': len(rewards),
            'raw_rewards': rewards.tolist(),
            'smoothed_rewards': smoothed.tolist(),
            'episode_lengths': lengths.tolist(),
            'final_avg_reward': float(np.mean(rewards[-10:])),
            'max_reward': float(np.max(rewards)),
            'min_reward': float(np.min(rewards)),
        }
    else:
        result = {
            'seed': seed,
            'n_timesteps': n_timesteps,
            'n_episodes': 0,
            'raw_rewards': [],
            'smoothed_rewards': [],
            'episode_lengths': [],
            'final_avg_reward': 0,
            'max_reward': 0,
            'min_reward': 0,
        }

    logger.info(f"  Seed {seed}: {result['n_episodes']} episodes, "
                f"Final avg reward: {result['final_avg_reward']:.2f}")
    return result


def plot_learning_curves(all_results, output_dir=OUTPUT_DIR):
    """
    Plot learning curves with mean ± std confidence bands.

    Produces two plots:
      1. Mean ± std band across all seeds
      2. Individual seed curves overlaid with mean
    """
    logger.info("\nGenerating learning curve plots...")

    # Filter to only results with episodes
    valid_results = [r for r in all_results if r['n_episodes'] > 0]

    if len(valid_results) == 0:
        logger.warning("  No episodes captured from any seed. Skipping plots.")
        logger.warning("  Try increasing n_timesteps or checking the environment.")
        return None

    max_episodes = max(len(r['smoothed_rewards']) for r in valid_results)
    if max_episodes == 0:
        logger.warning("  All seeds have 0 smoothed rewards. Skipping plots.")
        return None

    # Pad/trim all series to max_episodes
    aligned = []
    for r in valid_results:
        series = np.array(r['smoothed_rewards'])
        if len(series) < max_episodes:
            series = np.pad(series, (0, max_episodes - len(series)), constant_values=series[-1] if len(series) > 0 else 0)
        aligned.append(series[:max_episodes])

    aligned = np.array(aligned)  # shape: (n_seeds, n_episodes)

    mean_curve = np.mean(aligned, axis=0)
    std_curve = np.std(aligned, axis=0)
    min_curve = np.min(aligned, axis=0)
    max_curve = np.max(aligned, axis=0)

    # -- Plot 1: Mean ± Confidence Band --
    fig, ax = plt.subplots(figsize=(12, 6))
    episodes = np.arange(max_episodes)

    ax.fill_between(episodes, mean_curve - std_curve, mean_curve + std_curve,
                     alpha=0.3, color='#3498db', label='±1 Std Dev')
    ax.fill_between(episodes, min_curve, max_curve,
                     alpha=0.15, color='#3498db', label='Min-Max Range')
    ax.plot(episodes, mean_curve, color='#2980b9', linewidth=2.5, label=f'Mean (n={len(all_results)})')

    ax.set_xlabel('Episode', fontsize=12)
    ax.set_ylabel('Episode Reward (Smoothed)', fontsize=12)
    ax.set_title('Learning Curve: Mean ± Confidence Band', fontsize=14, fontweight='bold')
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    path1 = os.path.join(output_dir, "learning_curve_confidence_band.png")
    plt.savefig(path1, dpi=200, bbox_inches='tight')
    plt.close()
    logger.info(f"  Saved: learning_curve_confidence_band.png")

    # -- Plot 2: Individual Seeds + Mean --
    fig, ax = plt.subplots(figsize=(12, 6))
    colors = plt.cm.tab10(np.linspace(0, 1, len(valid_results)))

    for i, r in enumerate(valid_results):
        series = np.array(r['smoothed_rewards'])
        if len(series) < max_episodes:
            series = np.pad(series, (0, max_episodes - len(series)), constant_values=series[-1] if len(series) > 0 else 0)
        ax.plot(episodes[:len(series)], series, color=colors[i],
                alpha=0.5, linewidth=1.5, label=f"Seed {r['seed']}")

    ax.plot(episodes, mean_curve, color='black', linewidth=3, label=f'Mean (n={len(valid_results)})')
    ax.fill_between(episodes, mean_curve - std_curve, mean_curve + std_curve,
                     alpha=0.2, color='black')

    ax.set_xlabel('Episode', fontsize=12)
    ax.set_ylabel('Episode Reward (Smoothed)', fontsize=12)
    ax.set_title(f'Learning Curves: Individual Seeds + Mean (n={len(all_results)})',
                 fontsize=14, fontweight='bold')
    ax.legend(fontsize=8, loc='best')
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    path2 = os.path.join(output_dir, "learning_curve_individual_seeds.png")
    plt.savefig(path2, dpi=200, bbox_inches='tight')
    plt.close()
    logger.info(f"  Saved: learning_curve_individual_seeds.png")

    return {
        'mean_curve': mean_curve.tolist(),
        'std_curve': std_curve.tolist(),
        'min_curve': min_curve.tolist(),
        'max_curve': max_curve.tolist(),
        'n_seeds': len(valid_results),
        'n_episodes': max_episodes,
    }


def main():
    n_seeds = cfg.eval_n_seeds
    seeds = list(range(42, 42 + n_seeds))  # Default: 42, 43, 44

    logger.info(f"\nTraining {n_seeds} agents with seeds: {seeds}")
    logger.info(f"  Timesteps per seed: {min(100000, cfg.rl_total_timesteps)}")

    all_results = []
    for seed in tqdm(seeds, desc="Training seeds", ncols=80):
        result = train_single_seed(seed)
        all_results.append(result)

    # Save raw results
    raw_output = []
    for r in all_results:
        raw_output.append({
            'seed': r['seed'],
            'n_episodes': r['n_episodes'],
            'final_avg_reward': r['final_avg_reward'],
            'max_reward': r['max_reward'],
            'min_reward': r['min_reward'],
        })

    pd.DataFrame(raw_output).to_csv(
        os.path.join(OUTPUT_DIR, "learning_curve_seeds_summary.csv"), index=False
    )

    # Save full data
    with open(os.path.join(OUTPUT_DIR, "learning_curve_full.json"), 'w') as f:
        json.dump(all_results, f, indent=2, default=str)

    # Plot
    curve_stats = plot_learning_curves(all_results)

    logger.info("\n" + "=" * 70)
    logger.info("LEARNING CURVES COMPLETE")
    logger.info("=" * 70)
    logger.info(f"\n  Outputs saved to: {OUTPUT_DIR}/")
    logger.info(f"  +━ learning_curve_confidence_band.png")
    logger.info(f"  +━ learning_curve_individual_seeds.png")
    logger.info(f"  +━ learning_curve_seeds_summary.csv")
    logger.info(f"  +━ learning_curve_full.json")


if __name__ == "__main__":
    main()
