import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# Database Connection
engine = create_engine(
    "postgresql://postgres:Bishal5100#@localhost:5432/debt_market_db"
)
df = pd.read_sql_table("synthetic_profiles_gan", engine)

# Create figures directory in root
root_dir = r"C:\Users\Bishal\Documents\GitHub\Explainable AI For debt Collection"
figures_dir = os.path.join(root_dir, "figures")
os.makedirs(figures_dir, exist_ok=True)

# Set visual style
sns.set(style="whitegrid")

# 1. Risk Category vs. CIBIL Score
plt.figure(figsize=(12, 6))
sns.violinplot(
    data=df,
    x="risk_category",
    y="cibil_score",
    order=["Very Low", "Low", "Medium", "High", "Very High"],
)
plt.title("CIBIL Score Spread per Risk Category")
plt.tight_layout()
plt.savefig(os.path.join(figures_dir, "risk_vs_cibil.png"), dpi=300)
plt.close()

# 2. Bounce Count vs. Overdue Months (Financial Health)
plt.figure(figsize=(10, 6))
sns.scatterplot(
    data=df, x="bounce_count", y="overdue_months", hue="risk_category", alpha=0.5
)
plt.title("Relationship: Bounces vs. Overdue Months")
plt.tight_layout()
plt.savefig(os.path.join(figures_dir, "bounces_vs_overdue.png"), dpi=300)
plt.close()

# 3. Last Call Status vs. Risk Category (Behavioral Analysis)
# This shows how different risk levels respond to communication
pivot_df = df.groupby(["risk_category", "last_call_status"]).size().unstack()
plt.figure(figsize=(12, 7))
pivot_df.plot(kind="bar", stacked=True, figsize=(12, 7))
plt.title("Last Call Response by Risk Category")
plt.ylabel("Number of Customers")
plt.tight_layout()
plt.savefig(os.path.join(figures_dir, "call_status_by_risk.png"), dpi=300)
plt.close()

# 4. Total Demand Distribution
plt.figure(figsize=(10, 5))
sns.histplot(df["total_demand"], bins=50, kde=True, color="red")
plt.title("Distribution of Total Debt Demand")
plt.tight_layout()
plt.savefig(os.path.join(figures_dir, "total_demand_distribution.png"), dpi=300)
plt.close()

print(f"Plots saved to: {figures_dir}")
