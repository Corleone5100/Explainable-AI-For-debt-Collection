"""
Feature Selector — Prepare optimal feature set for training.

Per the ablation study in evaluation_report.txt:
  - Community features are the single most important addition (+Rs. 0.99M alone)
  - Structural features (degree, pagerank, betweenness) HURT performance
  - Multi-hop signals (2-hop, 3-hop) add noise; 1-hop is sufficient
  - Full 150-feature model fails catastrophically with only 50K timesteps
  - Optimal: +all_graph (16 features: core + community + 1-hop + GAT embeddings)

Usage:
    python scripts/feature_selector.py                  # Create optimal CSV (default)
    python scripts/feature_selector.py --set optimal    # Optimal 16-feature set
    python scripts/feature_selector.py --set full       # All features
    python scripts/feature_selector.py --set baseline   # Core features only
    python scripts/feature_selector.py --set community  # Core + community features
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import pandas as pd
from config import cfg, logger

# Feature set definitions
FEATURE_SETS = {
    "baseline": {
        "description": "8 core borrower features only (no graph)",
        "include": [
            "income", "cibil_score", "overdue_months", "bounce_count",
            "coll_success_rate", "age", "occ_idx", "reg_idx",
        ],
        "exclude_graph": True,
    },
    "community": {
        "description": "Core + community features (first profitable config: +Rs. 0.99M)",
        "include": [
            "income", "cibil_score", "overdue_months", "bounce_count",
            "coll_success_rate", "age", "occ_idx", "reg_idx",
            "community_risk_pct", "community_avg_overdue",
        ],
        "exclude_graph": False,
        "exclude_structural": True,
        "exclude_multihop": True,
        "exclude_gat": True,
    },
    "optimal": {
        "description": "Core + community + 1-hop + GAT embeddings (+Rs. 1.74M, recommended)",
        "include": [
            "income", "cibil_score", "overdue_months", "bounce_count",
            "coll_success_rate", "age", "occ_idx", "reg_idx",
            "community_risk_pct", "community_avg_overdue",
            "community_total_demand", "community_size",
            "neighborhood_stress_1hop",
        ],
        "include_prefix": ["gat_embedding_"],
        "exclude_structural": True,
        "exclude_multihop": True,
    },
    "full": {
        "description": "All features (requires 500K+ timesteps)",
        "include": None,  # Include everything
    },
}


def select_features(input_csv, output_csv, feature_set_name):
    """Filter CSV to the specified feature set and save."""
    logger.info(f"Loading data from {input_csv}...")
    df = pd.read_csv(input_csv)

    if feature_set_name not in FEATURE_SETS:
        logger.error(f"Unknown feature set: {feature_set_name}")
        logger.info(f"Available: {list(FEATURE_SETS.keys())}")
        return

    config = FEATURE_SETS[feature_set_name]
    logger.info(f"Feature set: {feature_set_name} — {config['description']}")

    # Start with always-required columns (non-feature columns)
    always_keep = [
        "risk_category", "total_demand", "customer_id"
    ]

    if config.get("include") is None:
        # Full mode: keep everything
        selected_cols = list(df.columns)
    else:
        selected_cols = list(config["include"])

        # Add GAT embeddings if not excluded
        if not config.get("exclude_gat", False):
            gat_cols = [c for c in df.columns if c.startswith("gat_embedding_")]
            if gat_cols:
                gat_cols.sort(key=lambda x: int(x.split("_")[-1]))
                selected_cols.extend(gat_cols)
        elif "include_prefix" in config:
            for prefix in config.get("include_prefix", []):
                matching = [c for c in df.columns if c.startswith(prefix)]
                selected_cols.extend(matching)

        # Exclude structural features if requested
        if config.get("exclude_structural", False):
            for col in ["node_degree", "pagerank", "betweenness"]:
                if col in selected_cols:
                    selected_cols.remove(col)

        # Exclude multi-hop (2-hop, 3-hop) if requested
        if config.get("exclude_multihop", False):
            for col in ["neighborhood_stress_2hop", "neighborhood_stress_3hop"]:
                if col in selected_cols:
                    selected_cols.remove(col)

        # Exclude all graph features if requested
        if config.get("exclude_graph", False):
            graph_cols = [
                "node_degree", "pagerank", "betweenness",
                "community_risk_pct", "community_avg_overdue",
                "community_total_demand", "community_size",
                "neighborhood_stress_1hop", "neighborhood_stress_2hop",
                "neighborhood_stress_3hop", "neighborhood_stress_signal",
            ]
            graph_cols += [c for c in df.columns if c.startswith("gat_embedding_")]
            selected_cols = [c for c in selected_cols if c not in graph_cols]

    # Filter to only columns that exist in the DataFrame
    final_cols = [c for c in selected_cols if c in df.columns]

    # Add always-required columns that exist
    for col in always_keep:
        if col in df.columns and col not in final_cols:
            final_cols.append(col)

    missing = [c for c in final_cols if c not in df.columns]
    if missing:
        logger.warning(f"Columns not found in data (skipping): {missing}")
        final_cols = [c for c in final_cols if c in df.columns]

    df_out = df[final_cols]

    logger.info(f"Selected {len(final_cols)} features: {final_cols}")
    logger.info(f"Saving to {output_csv}...")
    df_out.to_csv(output_csv, index=False)
    logger.info(f"Done! Output shape: {df_out.shape}")

    return df_out


def main():
    parser = argparse.ArgumentParser(description="Select optimal feature set for RL training")
    parser.add_argument(
        "--set",
        type=str,
        default=cfg.feature_set,
        choices=list(FEATURE_SETS.keys()),
        help=f"Feature set to use (default from config: {cfg.feature_set})",
    )
    parser.add_argument(
        "--input",
        type=str,
        default="rl_ready_with_graph_features.csv",
        help="Input CSV with all features",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output CSV path (default: rl_ready_<set_name>.csv)",
    )
    args = parser.parse_args()

    if args.output is None:
        base = os.path.splitext(os.path.basename(args.input))[0]
        args.output = f"{base}_{args.set}.csv"

    logger.info(f"=== Feature Selector ===")
    logger.info(f"Input:  {args.input}")
    logger.info(f"Output: {args.output}")
    logger.info(f"Set:    {args.set}")

    select_features(args.input, args.output, args.set)


if __name__ == "__main__":
    main()
