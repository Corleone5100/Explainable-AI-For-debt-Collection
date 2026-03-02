import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# Database Connection
engine = create_engine('postgresql://postgres:Bishal5100#@localhost:5432/debt_market_db')
df = pd.read_sql_table('customer_profiles', engine)

# Set visual style
sns.set(style="whitegrid")
plt.figure(figsize=(15, 10))

# 1. Risk Category vs. CIBIL Score
plt.figure(figsize=(12, 6))
sns.violinplot(data=df, x='risk_category', y='cibil_score', 
               order=["Very Low", "Low", "Medium", "High", "Very High"])
plt.title('CIBIL Score Spread per Risk Category')
plt.show()

# 2. Bounce Count vs. Overdue Months (Financial Health)
plt.figure(figsize=(10, 6))
sns.scatterplot(data=df, x='bounce_count', y='overdue_months', hue='risk_category', alpha=0.5)
plt.title('Relationship: Bounces vs. Overdue Months')
plt.show()

# 3. Last Call Status vs. Risk Category (Behavioral Analysis)
# This shows how different risk levels respond to communication
pivot_df = df.groupby(['risk_category', 'last_call_status']).size().unstack()
pivot_df.plot(kind='bar', stacked=True, figsize=(12, 7))
plt.title('Last Call Response by Risk Category')
plt.ylabel('Number of Customers')
plt.show()

# 4. Total Demand Distribution
plt.figure(figsize=(10, 5))
sns.histplot(df['total_demand'], bins=50, kde=True, color='red')
plt.title('Distribution of Total Debt Demand')
plt.show()