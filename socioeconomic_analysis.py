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

# 1. Distribution of Occupations and Qualifications
plt.figure(figsize=(10, 6))
sns.countplot(
    data=df, x="occupation", hue="occupation", palette="viridis", legend=False
)
plt.title("Distribution of Occupations")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(os.path.join(figures_dir, "occupation_distribution.png"), dpi=300)
plt.close()

# 2. Income Distribution by Occupation (Validating the Multiplier Logic)
plt.figure(figsize=(10, 6))
sns.boxplot(data=df, x="occupation", y="income")
plt.title("Income Range by Occupation")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(os.path.join(figures_dir, "income_by_occupation.png"), dpi=300)
plt.close()

# 3. Age vs. Income (Validating the Age Factor Trend)
plt.figure(figsize=(10, 6))
sns.lineplot(data=df, x="age", y="income", errorbar=None)
plt.title("Age vs. Average Income (Expect Peak 45-55)")
plt.tight_layout()
plt.savefig(os.path.join(figures_dir, "age_vs_income.png"), dpi=300)
plt.close()

# 4. Region-wise Distribution
plt.figure(figsize=(10, 6))
sns.countplot(data=df, x="region", order=[f"R{i}" for i in range(1, 8)])
plt.title("Customer Distribution by Region")
plt.tight_layout()
plt.savefig(os.path.join(figures_dir, "region_distribution.png"), dpi=300)
plt.close()

print(f"Plots saved to: {figures_dir}")
