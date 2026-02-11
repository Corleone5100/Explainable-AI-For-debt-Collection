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
    """Creates the customer profile table in Postgres."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS customer_profiles;")
    cur.execute("""
        CREATE TABLE customer_profiles (
            customer_id VARCHAR(20) PRIMARY KEY,
            age INT,
            occupation VARCHAR(50),
            income DECIMAL(12,2),
            region VARCHAR(10),
            qualification VARCHAR(50),
            cibil_score INT,
            overdue_months INT,
            bounce_count INT,
            current_demand DECIMAL(12,2),
            total_demand DECIMAL(12,2),
            risk_category VARCHAR(20),
            profile_type VARCHAR(10)
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Table created successfully.")

def generate_batch(batch_size, start_id):
    """Generates a batch of synthetic data with Good, Average, and Bad profiles."""
    occupations = ['Salaried', 'Self-employed', 'Agriculturalist', 'Daily wage worker']
    regions = ['R1', 'R2', 'R3', 'R4', 'R5', 'R6', 'R7']
    qualifications = ['Graduate', 'Post-Graduate', 'High School', 'Illiterate']
    
    batch_data = []
    
    for i in range(batch_size):
        cust_id = f"ACC_{start_id + i}"
        age = random.randint(18, 70)
        region = random.choice(regions)
        qual = random.choice(qualifications)
        occ = random.choice(occupations)
        
        # Determine Profile Type (Distribution: 30% Good, 40% Average, 30% Bad)
        rand_val = random.random()
        if rand_val < 0.30:
            profile_type = "Good"
        elif rand_val < 0.70:
            profile_type = "Average"
        else:
            profile_type = "Bad"

        # --- LOGIC PER PROFILE ---
        if profile_type == "Good":
            income = np.random.normal(70000, 15000)
            cibil = random.randint(750, 900)
            overdue = 0
            bounces = 0
            risk = "Very Low"
            demand = random.uniform(1000, 5000)
            
        elif profile_type == "Average":
            income = np.random.normal(40000, 10000)
            cibil = random.randint(600, 749)
            overdue = random.randint(1, 3)
            bounces = random.randint(0, 2)
            risk = random.choice(["Low", "Medium"])
            demand = random.uniform(5000, 15000)
            
        else: # "Bad" Profile
            income = np.random.normal(15000, 5000)
            cibil = random.randint(300, 599)
            overdue = random.randint(4, 12)
            bounces = random.randint(3, 10)
            risk = random.choice(["High", "Very High"])
            demand = random.uniform(15000, 50000)

        income = max(8000, income) # Salary floor
        total_demand = demand * random.uniform(1.1, 2.0)

        batch_data.append((
            cust_id, age, occ, round(income, 2), region, qual,
            cibil, overdue, bounces, round(demand, 2), 
            round(total_demand, 2), risk, profile_type
        ))
        
    return batch_data

def insert_data(total_records=20000, batch_size=1000):
    """Inserts data into Postgres using bulk execution."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    print(f"Starting insertion of {total_records} records...")
    
    for start_id in tqdm(range(0, total_records, batch_size)):
        batch = generate_batch(batch_size, start_id)
        
        query = """
            INSERT INTO customer_profiles (
                customer_id, age, occupation, income, region, qualification,
                cibil_score, overdue_months, bounce_count, current_demand,
                total_demand, risk_category, profile_type
            ) VALUES %s
        """
        execute_values(cur, query, batch)
        conn.commit()

    cur.close()
    conn.close()
    print("Data insertion complete.")

if __name__ == "__main__":
    create_database_if_not_exists()
    create_table()
    insert_data(20000)