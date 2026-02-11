import psycopg2
from psycopg2.extras import execute_values
import numpy as np
import random
from tqdm import tqdm # For progress bar

# --- DATABASE CONFIGURATION ---
DB_CONFIG = {
    "host": "localhost",
    "database": "debt_market_db",
    "user": "postgres",
    "password": "Bishal5100#"
}
def create_database_if_not_exists():
    # Connect to the default 'postgres' database first
    conn = psycopg2.connect(
        host=DB_CONFIG["host"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database="postgres" # Connect to system default
    )
    conn.autocommit = True # Required for CREATE DATABASE commands
    cur = conn.cursor()
    
    # Check if our database exists
    cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'debt_market_db'")
    exists = cur.fetchone()
    
    if not exists:
        print("Database does not exist. Creating...")
        cur.execute("CREATE DATABASE debt_market_db")
    else:
        print("Database already exists.")
        
    cur.close()
    conn.close()

def create_table():
    """
    Creates the customer profile table in Postgres with the exact S1 and S2 parameters.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Clean start: drop the old table if it exists
    cur.execute("DROP TABLE IF EXISTS customer_profiles;")
    
    cur.execute("""
        CREATE TABLE customer_profiles (
            -- S1: Customer Profile Data (Socioeconomic Details)
            customer_id VARCHAR(20) PRIMARY KEY,
            age INT,
            occupation VARCHAR(50),
            income DECIMAL(12,2),
            family_size INT,
            region VARCHAR(10),
            qualification VARCHAR(50),
            cibil_score INT,
            cibil_hit INT, -- 0 or 1

            -- S2: Debt & History Data (Financial State)
            overdue_months INT,
            bounce_count INT,
            emi_month INT,
            current_demand DECIMAL(12,2),
            total_demand DECIMAL(12,2),
            pending_status VARCHAR(10), -- 'Yes' or 'No'
            last_call_status VARCHAR(50),
            risk_category VARCHAR(20)
        );
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("Table 'customer_profiles' created successfully with S1 and S2 parameters.")

def generate_batch(batch_size, start_id):
    """
    Generates synthetic borrower data based on socioeconomic hierarchies.
    
    Logic Flow:
    1. Randomly pick Age, Occupation, Region.
    2. Qualification is chosen based on Occupation probabilities.
    3. Income is derived from Occupation + Qualification + Age factor.
    4. CIBIL & Risk are derived from Income and Qualification trends.
    5. Debt stats (Bounces/Overdue) are inversely related to Income/CIBIL.
    """
    
    occupations = ['Salaried', 'Self-employed', 'Agriculturalist', 'Daily wage worker']
    regions = [f'R{i}' for i in range(1, 8)]
    qualifications_list = ['Post-Graduate', 'Graduate', 'High School', 'Illiterate']
    call_statuses = ['PTP', 'No Response', 'Refuse to Pay', 'Wrong Number']
    
    batch_data = []

    for i in range(batch_size):
        # --- 1. BASE DEMOGRAPHICS ---
        cust_id = f"ACC_{start_id + i}"
        age = random.randint(18, 75)
        region = random.choice(regions)
        occ = random.choice(occupations)
        
        # --- 2. QUALIFICATION BASED ON OCCUPATION (Probabilistic) ---
        if occ == 'Salaried':
            qual = np.random.choice(qualifications_list, p=[0.4, 0.4, 0.15, 0.05])
        elif occ == 'Self-employed':
            qual = np.random.choice(qualifications_list, p=[0.2, 0.4, 0.3, 0.1])
        elif occ == 'Agriculturalist':
            qual = np.random.choice(qualifications_list, p=[0.05, 0.15, 0.4, 0.4])
        else: # Daily wage worker
            qual = np.random.choice(qualifications_list, p=[0.01, 0.09, 0.4, 0.5])

        # --- 3. INCOME LOGIC (Trend: Higher Qual + Higher Age + Occ -> Higher Income) ---
        # Base income by occupation
        base_income_map = {
            'Salaried': 30000, 
            'Self-employed': 35000, 
            'Agriculturalist': 15000, 
            'Daily wage worker': 8000
        }
        # Multiplier by qualification
        qual_mult_map = {
            'Post-Graduate': 2.0, 
            'Graduate': 1.5, 
            'High School': 1.0, 
            'Illiterate': 0.7
        }
        
        income_base = base_income_map[occ] * qual_mult_map[qual]
        # Age trend: Income peaks around 45-55
        age_factor = 1 + (max(0, age - 18) / 40) 
        income = income_base * age_factor * random.uniform(0.8, 1.2)
        income = max(10000, min(200000, income)) # Constraints

        # --- 4. CIBIL & FINANCIAL BEHAVIOR ---
        # Trend: High Income + High Qual -> High CIBIL
        cibil_base = 400 + (income / 200000 * 400) + (qual_mult_map[qual] * 50)
        cibil = int(max(300, min(900, cibil_base + random.randint(-50, 50))))
        cibil_hit = 1 if random.random() > 0.7 else 0
        
        family_size = random.randint(1, 8)
        
        # --- 5. DEBT & HISTORY (S2 - Financial State) ---
        # Trend: Higher CIBIL/Income -> Lower Bounces and Overdue
        if cibil > 750:
            risk_cat = "Very Low"
            overdue_months = random.randint(0, 1)
            bounce_count = random.randint(0, 1)
            last_call = "PTP"
        elif cibil > 650:
            risk_cat = random.choice(["Low", "Medium"])
            overdue_months = random.randint(1, 3)
            bounce_count = random.randint(0, 3)
            last_call = random.choice(["PTP", "No Response"])
        elif cibil > 500:
            risk_cat = "High"
            overdue_months = random.randint(3, 6)
            bounce_count = random.randint(2, 6)
            last_call = random.choice(["No Response", "Refuse to Pay"])
        else:
            risk_cat = "Very High"
            overdue_months = random.randint(6, 12)
            bounce_count = random.randint(5, 10)
            last_call = random.choice(["Refuse to Pay", "Wrong Number"])

        emi_month = random.randint(1, 60)
        current_demand = random.uniform(2000, 15000)
        total_demand = current_demand * (overdue_months + random.uniform(1, 2))
        pending_status = "Yes" if overdue_months > 0 else "No"

        # Construct Row
        batch_data.append({
            # S1 - Customer Profile
            "Customer_ID": cust_id,
            "Age": age,
            "Occupation": occ,
            "Income": round(income, 2),
            "Family_Size": family_size,
            "Region": region,
            "Qualification": qual,
            "CIBIL_Score": cibil,
            "CIBIL_Hit": cibil_hit,
            
            # S2 - Financial State
            "Overdue_Months": overdue_months,
            "Bounce_Count": bounce_count,
            "EMI_Month": emi_month,
            "Current_Demand": round(current_demand, 2),
            "Total_Demand": round(total_demand, 2),
            "Pending_Status": pending_status,
            "Last_Call_Status": last_call,
            "Risk_Category": risk_cat
        })
        
    return batch_data

def insert_data(total_records=20000, batch_size=1000):
<<<<<<< HEAD
    """
    Inserts synthetic borrower data into Postgres.
    Matches the exact S1 and S2 parameters.
    """
    # Database configuration (Ensure this is defined in your script)
    # DB_CONFIG = { ... } 
    
=======
    """Inserts data into Postgres using bulk execution."""
>>>>>>> 8ae0e9c9762849f7ac87192bc41c8a729e3c0f2e
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    print(f"Starting insertion of {total_records} records...")
    
    try:
        for start_id in tqdm(range(0, total_records, batch_size)):
            # 1. Generate the DataFrame using the logic-heavy function
            df_batch = generate_batch(batch_size, start_id)
            
            # 2. Convert DataFrame to list of tuples for psycopg2
            # We explicitly define the column order to match the SQL query
            data_to_insert = [tuple(d.values()) for d in df_batch]
            
            # 3. SQL Query including all S1 and S2 parameters
            query = """
                INSERT INTO customer_profiles(
                    customer_id, age, occupation, income, family_size, 
                    region, qualification, cibil_score, cibil_hit,
                    overdue_months, bounce_count, emi_month, current_demand,
                    total_demand, pending_status, last_call_status, risk_category
                ) VALUES %s
            """
            
            # 4. Bulk Execution
            execute_values(cur, query, data_to_insert)
            conn.commit()
            
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()
        
    print("Data insertion complete.")

if __name__ == "__main__":
    create_database_if_not_exists()
    create_table()
<<<<<<< HEAD
    insert_data()
=======
    insert_data(20000)
>>>>>>> 8ae0e9c9762849f7ac87192bc41c8a729e3c0f2e
