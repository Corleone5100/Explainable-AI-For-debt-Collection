# Explainable AI for Debt Collection — Run Guide

## Project Structure

```
Explainable AI For debt Collection/
├── .env                         # Database credentials + tunable params (DO NOT COMMIT)
├── .env.example                 # Template for .env
├── config.py                    # Centralized config loader
├── logs/                        # Auto-created log files
│
├── data_generation/             # Step 1-2: Synthetic data + collector assignment
│   ├── Sample_Data_Generaton.py    # Generate borrower profiles → PostgreSQL
│   ├── loan_collector.py           # Create collectors + merge into master table
│   └── Synthetic_Data.py           # Alternative/earlier data generation
│
├── analysis/                    # Step 3-4: Exploratory data analysis & plots
│   ├── correlation.py              # Correlation matrix heatmap
│   ├── risk_analysis.py            # CIBIL vs risk, bounce vs overdue plots
│   ├── socioeconomic_analysis.py   # Occupation, income, region distributions
│   └── visualize_graph.py          # 6-panel graph visualization
│
├── scripts/                     # Step 5-8: Core ML pipeline
│   ├── graph_builder.py            # Build contagion graph + train GAT + extract embeddings
│   ├── debt_env.py                 # RL environment with graph-enhanced features
│   ├── testing.py                  # Run trained model on sample borrowers
│   ├── evaluation.py               # Train/test split, metrics, ablation, confusion matrix
│   └── learning_curves.py          # Multi-seed training with confidence bands
│
├── xai/                         # Step 9-12: Explainable AI analysis
│   ├── xai_analysis.py             # SHAP, counterfactuals, feature importance, trajectories
│   ├── xai_visualizations.py       # Policy plots, embedding t-SNE, heatmaps
│   ├── fairness_audit.py           # Demographic bias detection (occupation, region, age, income)
│   └── xai_inference.py            # Real-time explainable inference demo
│
├── tests/                       # Unit tests
│   └── test_debt_collection.py     # 20+ tests for env, config, features
│
├── docs/                        # Documentation
│   ├── project_summary.txt         # Overall repo summary
│   ├── graph_enhancement_report.txt # Graph construction detailed report
│   └── xai_enhancement_report.txt  # XAI features detailed report
│
├── figures/                     # Analysis plots (auto-generated)
├── xai_outputs/                 # XAI analysis outputs (auto-generated)
├── evaluation_outputs/          # Evaluation metrics & plots (auto-generated)
│
└── RUN.md                       # ← You are here
```

---

## Prerequisites

### 1. Install Dependencies

```bash
pip install pandas numpy sqlalchemy psycopg2-binary
pip install scikit-learn torch torch-geometric torch-scatter torch-sparse
pip install gymnasium stable-baselines3
pip install matplotlib seaborn networkx
pip install shap tqdm
pip install python-louvain python-dotenv
pip install pytest

# Optional: for large graph operations
pip install torch-scatter torch-sparse -f https://data.pyg.org/whl/torch-<version>.html
```

### 2. Configure Database

Copy the example and fill in your credentials:

```bash
cp .env.example .env
```

Edit `.env` and set your actual database password:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=debt_market_db
DB_USER=postgres
DB_PASSWORD=your_actual_password
```

> ⚠️ **Never commit `.env` to git.** It's already in `.gitignore`.

---

## Full Pipeline (Start to Finish)

### Step 1: Generate Synthetic Borrower Data

```bash
python data_generation/Sample_Data_Generaton.py
```

**What it does:**
- Creates PostgreSQL database `debt_market_db`
- Creates `customer_profiles` table with S1 (demographics) and S2 (financial) features
- Generates 20,000 synthetic borrower records with realistic socioeconomic logic

**Output:** `customer_profiles` table in PostgreSQL

---

### Step 2: Create Collectors + Merge Master Table

```bash
python data_generation/loan_collector.py
```

**What it does:**
- Creates 500 debt collector profiles with varying capabilities
- Matches borrowers ↔ collectors by region + risk specialization
- Creates `master_env_data` table (merged borrower + collector data)

**Output:** `master_env_data` table in PostgreSQL

---

### Step 3: Exploratory Data Analysis

```bash
python analysis/socioeconomic_analysis.py
python analysis/correlation.py
python analysis/risk_analysis.py
```

**What it does:**
- Generates distribution plots, correlation matrix, risk analysis charts
- All plots saved to `figures/`

---

### Step 4: Visualize the Borrower Graph (small sample)

```bash
python analysis/visualize_graph.py
```

**Output:** `graph_visualization_enhanced.png` — 6-panel visualization

---

### Step 5: Build Graph + Train GAT + Extract Embeddings

```bash
python scripts/graph_builder.py
```

**What it does (full pipeline):**
1. Loads up to 200,000 borrowers from `master_env_data`
2. Builds weighted contagion graph (region + occupation edges with income/age similarity weights)
3. Computes structural features (degree, PageRank, betweenness)
4. Detects communities (Louvain) → community risk metrics
5. Computes multi-hop signals (1-hop, 2-hop, 3-hop neighborhood stress)
6. Trains 2-layer GAT model on risk classification (100 epochs)
7. Extracts 16-dim embeddings + attention weights
8. Saves everything to `rl_ready_with_graph_features.csv`

**Outputs:**
- `rl_ready_with_graph_features.csv` — Enhanced dataset (~45 columns)
- `gat_attention_weights.csv` — GAT attention coefficients
- `graph_validation_report.csv` — Graph quality metrics

**Time:** ~10-30 minutes depending on data size and GPU

---

### Step 6: Train RL Agent (PPO)

```bash
python scripts/debt_env.py
```

**What it does:**
- Loads the enhanced CSV (34-dim observations)
- Wraps in VecNormalize for feature scaling
- Trains PPO agent for 500,000 timesteps (configurable in `.env`)
- Saves trained model and normalization stats

**Outputs:**
- `graph_rl_debt_model.zip` — Trained PPO model
- `vec_normalize.pkl` — Observation/reward scaling statistics

**Time:** ~30-60 minutes on GPU

---

### Step 7: Test the Trained Model

```bash
python scripts/testing.py
```

**What it does:**
- Loads trained model + normalization stats
- Runs inference on 5 sample borrowers
- Prints recommended action + confidence + neighborhood stress

---

### Step 8: Comprehensive Evaluation

```bash
python scripts/evaluation.py
```

**What it does:**
1. Creates stratified train/test split (80/20)
2. Evaluates agent on test set with full metrics (ROI, % resolved, cost/recovery)
3. Confusion matrix: agent actions vs domain-optimal actions per risk category
4. Baseline comparison: random policy vs rule-based heuristic
5. Ablation study: trains 7 models with different feature subsets

**Outputs in `evaluation_outputs/`:**
- `evaluation_results.json` — Full metrics
- `confusion_matrix.png` — Action distribution by risk category
- `ablation_study.png` — Performance by feature subset
- `baseline_comparison.csv` — Random vs rule-based vs PPO

**Time:** ~15-30 minutes (ablation trains takes longest)

---

### Step 9: Multi-Seed Learning Curves

```bash
python scripts/learning_curves.py
```

**What it does:**
- Trains 3 agents with different random seeds (configurable)
- Plots mean ± std confidence band + individual seed curves

**Outputs in `evaluation_outputs/`:**
- `learning_curve_confidence_band.png`
- `learning_curve_individual_seeds.png`

**Time:** ~20-40 minutes

---

### Step 10: XAI — SHAP, Counterfactuals, Feature Importance

```bash
python xai/xai_analysis.py
```

**What it does:**
1. SHAP analysis on 200 samples → global + per-decision explanations
2. Counterfactual explanations for 50 borrowers
3. Permutation feature importance (shuffle each feature, measure reward drop)
4. Trajectory analysis: 5 episodes with per-step SHAP explanations

**Outputs in `xai_outputs/`:**
- `shap_summary_beeswarm.png`, `shap_summary_bar.png`
- `shap_per_decision.json`
- `counterfactuals.json`, `counterfactuals_summary.csv`
- `feature_importance.csv`, `feature_importance_plot.png`
- `trajectory_reports/episode_01.csv` + summary

**Time:** ~10-20 minutes

---

### Step 11: XAI — Policy Visualizations

```bash
python xai/xai_visualizations.py
```

**What it does:**
1. Risk × Action distribution (stacked bar chart)
2. Overdue × Income heatmap (2D grid colored by preferred action)
3. GAT embedding t-SNE (colored by action and risk)
4. Neighborhood stress → action shift (line plot)
5. Confidence distribution histogram
6. GAT attention network (neighbor influence)

**Outputs in `xai_outputs/policy_plots/`:**
- 6 PNG files

**Time:** ~5 minutes

---

### Step 12: XAI — Fairness Audit

```bash
python xai/fairness_audit.py
```

**What it does:**
- Chi-squared tests: occupation bias, region bias
- ANOVA: reward differences across regions
- Spearman correlations: age bias, income bias, graph feature bias
- Generates fairness report + plots

**Outputs in `xai_outputs/`:**
- `fairness_report.txt`
- `fairness_audit.json`
- `fairness_plots.png`

**Time:** ~5 minutes

---

### Step 13: XAI — Interactive Inference Demo

```bash
python xai/xai_inference.py
```

**What it does:**
- Picks 5 diverse borrowers (one per risk category)
- For each, generates a full explanation card:
  - Recommended action + confidence
  - SHAP feature contributions
  - Top influential neighbors (GAT attention)
  - Counterfactual scenario
  - Human-review flag

**Outputs in `xai_outputs/borrower_explanations/`:**
- Individual JSON files per borrower
- `borrower_explanations_all.json` — all combined

**Time:** ~2 minutes

---

### Step 14: Run Unit Tests

```bash
# Option 1: pytest (preferred)
pytest tests/ -v

# Option 2: unittest
python -m unittest discover tests/

# Option 3: direct run
python tests/test_debt_collection.py
```

**What it tests:**
- Observation space shape and dtype
- Action space validity
- Reward calculation logic (penalties for mismatched actions)
- Episode flow (completes within max_steps)
- Configuration loading and types
- Feature column detection (core, graph, GAT embeddings)

---

## Quick Start (Minimum Viable Demo)

If you want to go from **zero to results** as fast as possible:

```bash
# 1. Ensure data exists in PostgreSQL (run once)
python data_generation/Sample_Data_Generaton.py
python data_generation/loan_collector.py

# 2. Build graph + extract features (the big step)
python scripts/graph_builder.py

# 3. Train RL agent
python scripts/debt_env.py

# 4. Test on samples
python scripts/testing.py

# 5. Run XAI analysis
python xai/xai_analysis.py
python xai/xai_visualizations.py
python xai/xai_inference.py

# 6. Run tests
pytest tests/ -v
```

---

## Configuration

All tunable parameters are in `.env`. Key settings:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `GRAPH_NODE_LIMIT` | 200000 | Max borrowers to load |
| `GNN_EPOCHS` | 100 | GAT training epochs |
| `GNN_LR` | 0.005 | GAT learning rate |
| `GNN_HIDDEN` | 32 | GAT hidden layer size |
| `GNN_HEADS` | 4 | Number of attention heads |
| `RL_LEARNING_RATE` | 0.0003 | PPO learning rate |
| `RL_TOTAL_TIMESTEPS` | 500000 | Total PPO training steps |
| `EVAL_TRAIN_RATIO` | 0.8 | Train/test split ratio |
| `EVAL_N_EPISODES` | 50 | Episodes for evaluation |
| `EVAL_N_SEEDS` | 3 | Seeds for learning curves |

Change these values in `.env` and re-run — no code changes needed.

---

## Logs

All scripts log to `logs/pipeline.log`. Check this file for detailed execution history, errors, and timing.

```bash
# View logs in real-time
tail -f logs/pipeline.log

# Search for errors
grep ERROR logs/pipeline.log
```

---

## Troubleshooting

### Database connection fails
- Verify `.env` has correct `DB_PASSWORD`
- Test: `psql -U postgres -d debt_market_db -c "SELECT 1"`

### `torch_geometric` import errors
- Install matching versions: `pip install torch-geometric torch-scatter torch-sparse -f https://data.pyg.org/whl/torch-<your-version>.html`

### SHAP analysis is slow
- Reduce `N_SHAP_SAMPLES` in `xai/xai_analysis.py` (default: 200)
- Reduce `N_BACKGROUND` (default: 100)

### GAT training out of memory
- Reduce `GRAPH_NODE_LIMIT` in `.env` (try 50000)
- Reduce `GNN_HIDDEN` to 16

### VecNormalize file not found
- Make sure you ran `scripts/debt_env.py` first (it saves `vec_normalize.pkl`)

---

## File Outputs Summary

| File | Generated By | Location |
|------|-------------|----------|
| `rl_ready_with_graph_features.csv` | `graph_builder.py` | Project root |
| `gat_attention_weights.csv` | `graph_builder.py` | Project root |
| `graph_rl_debt_model.zip` | `debt_env.py` | Project root |
| `vec_normalize.pkl` | `debt_env.py` | Project root |
| `*.png` (analysis) | `analysis/*.py` | `figures/` |
| XAI outputs | `xai/*.py` | `xai_outputs/` |
| Evaluation outputs | `evaluation.py` | `evaluation_outputs/` |
| Logs | All scripts | `logs/pipeline.log` |
