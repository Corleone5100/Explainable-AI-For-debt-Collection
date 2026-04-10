"""
Correlation Matrix Analysis.

Run from project root:
  python analysis/correlation.py
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

from config import cfg

engine = create_engine(cfg.db_url)
df = pd.read_sql_table("synthetic_profiles_gan", engine)

# Create figures directory in root
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
figures_dir = os.path.join(root_dir, "figures")
os.makedirs(figures_dir, exist_ok=True)

# Set visual style
sns.set(style="whitegrid")
plt.figure(figsize=(15, 10))

# Convert Categorical to Numeric for Correlation
corr_df = df.copy()
risk_map = {"Very Low": 1, "Low": 2, "Medium": 3, "High": 4, "Very High": 5}
corr_df["risk_numeric"] = corr_df["risk_category"].map(risk_map)

# Select numerical columns
cols = [
    "age",
    "income",
    "cibil_score",
    "overdue_months",
    "bounce_count",
    "current_demand",
    "total_demand",
    "risk_numeric",
]

plt.figure(figsize=(12, 10))
mask = np.triu(np.ones_like(corr_df[cols].corr(), dtype=bool))
sns.heatmap(corr_df[cols].corr(), mask=mask, annot=True, cmap="coolwarm", fmt=".2f")
plt.title("Correlation Matrix: Identifying Key State Drivers")
plt.tight_layout()
plt.savefig(os.path.join(figures_dir, "correlation_matrix.png"), dpi=300)
plt.close()

# Print out basic stats for the XAI Baseline
print("--- SUMMARY STATISTICS ---")
print(df[["income", "cibil_score", "overdue_months", "total_demand"]].describe())
print("\n--- RISK CATEGORY COUNTS ---")
print(df["risk_category"].value_counts())
