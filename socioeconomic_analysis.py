import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# Database Connection
engine = create_engine('postgresql://postgres:Bishal5100#@localhost:5432/debt_market_db')
df = pd.read_sql_table('synthetic_profiles_gan', engine)

# Set visual style
sns.set(style="whitegrid")
plt.figure(figsize=(15, 10))

# 1. Distribution of Occupations and Qualifications
plt.subplot(2, 2, 1)
sns.countplot(data=df, x='occupation', palette='viridis')
plt.title('Distribution of Occupations')
plt.xticks(rotation=45)

# 2. Income Distribution by Occupation (Validating the Multiplier Logic)
plt.subplot(2, 2, 2)
sns.boxplot(data=df, x='occupation', y='income')
plt.title('Income Range by Occupation')
plt.xticks(rotation=45)

# 3. Age vs. Income (Validating the Age Factor Trend)
plt.subplot(2, 2, 3)
sns.lineplot(data=df, x='age', y='income', ci=None)
plt.title('Age vs. Average Income (Expect Peak 45-55)')

# 4. Region-wise Distribution
plt.subplot(2, 2, 4)
sns.countplot(data=df, x='region', order=[f'R{i}' for i in range(1, 8)])
plt.title('Customer Distribution by Region')

plt.tight_layout()
plt.show()