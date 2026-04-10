"""
Loan Collector Generation + Master Table Merge.

Run from project root:
  python data_generation/loan_collector.py
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from psycopg2.extras import execute_values
import random
import numpy as np

from config import cfg

# 1. Database Configuration
DB_CONFIG = {
    "host": cfg.db_host,
    "database": cfg.db_name,
    "user": cfg.db_user,
    "password": cfg.db_password,
}

def setup_collector_table():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    print("--- Step 1: Creating Collectors Table ---")
    cur.execute("DROP TABLE IF EXISTS collectors CASCADE;")
    cur.execute("""
        CREATE TABLE collectors (
            collector_id VARCHAR(20) PRIMARY KEY,
            performance_tier VARCHAR(20), -- Excellent, Average, Irregular
            experience_level VARCHAR(20),
            assigned_region VARCHAR(10),  -- R1 to R7
            specialization_risk VARCHAR(20), -- Tier they handle
            historical_success_rate DECIMAL(3,2)
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

def insert_collector_data(n_collectors=500):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    print(f"--- Step 2: Generating {n_collectors} Collector Profiles ---")
    
    tiers = ['Excellent', 'Average', 'Irregular']
    regions = [f'R{i}' for i in range(1, 8)]
    risk_buckets = ['Very Low', 'Low', 'Medium', 'High', 'Very High']
    exp_levels = ['Junior', 'Mid-level', 'Senior']
    
    collector_data = []
    for i in range(n_collectors):
        col_id = f"COL_{i:04d}"
        tier = np.random.choice(tiers, p=[0.2, 0.5, 0.3])
        region = random.choice(regions)
        exp = random.choice(exp_levels)
        
        # Logic from PPT: Mapping Collector capability to Risk specialization
        if tier == 'Excellent':
            spec_risk = random.choice(['High', 'Very High'])
            success = random.uniform(0.75, 0.95)
        elif tier == 'Average':
            spec_risk = random.choice(['Low', 'Medium', 'High'])
            success = random.uniform(0.50, 0.74)
        else: # Irregular
            spec_risk = random.choice(['Very Low', 'Low'])
            success = random.uniform(0.20, 0.49)
            
        collector_data.append((col_id, tier, exp, region, spec_risk, round(success, 2)))

    query = "INSERT INTO collectors VALUES %s"
    execute_values(cur, query, collector_data)
    conn.commit()
    cur.close()
    conn.close()
    print("Collectors inserted successfully.")

def create_master_merged_table():
    """
    Creates the final dataset in Postgres. 
    Matches Borrowers to Collectors using SQL logic.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    print("--- Step 3: Merging Borrowers and Collectors into Final Dataset ---")
    
    # We create a new table 'master_env_data'
    # Use DISTINCT ON to ensure each borrower gets exactly ONE collector from the pool
    cur.execute("DROP TABLE IF EXISTS master_env_data;")
    cur.execute("""
        CREATE TABLE master_env_data AS
        SELECT DISTINCT ON (b.customer_id)
            b.*,
            c.collector_id,
            c.performance_tier AS coll_tier,
            c.historical_success_rate AS coll_success_rate
        FROM synthetic_profiles_gan b
        LEFT JOIN collectors c ON b.region = c.assigned_region 
                               AND b.risk_category = c.specialization_risk
        ORDER BY b.customer_id, RANDOM(); 
    """)
    # Note: RANDOM() ensures that if multiple collectors fit the region/risk, 
    # the assignment is distributed among them.

    conn.commit()
    print("--- SUCCESS: 'master_env_data' table created in PostgreSQL ---")
    
    # Check for unassigned borrowers (where no collector matched both region and risk)
    cur.execute("SELECT COUNT(*) FROM master_env_data WHERE collector_id IS NULL;")
    missing = cur.fetchone()[0]
    if missing > 0:
        print(f"Warning: {missing} borrowers had no matching collector. Running fallback...")
        # Fallback logic: Assign any collector from the same region
        cur.execute("""
            UPDATE master_env_data m
            SET collector_id = c.collector_id,
                coll_tier = c.performance_tier,
                coll_success_rate = c.historical_success_rate
            FROM (SELECT DISTINCT ON (assigned_region) * FROM collectors ORDER BY assigned_region, RANDOM()) c
            WHERE m.collector_id IS NULL AND m.region = c.assigned_region;
        """)
        conn.commit()

    cur.close()
    conn.close()

if __name__ == "__main__":
    setup_collector_table()
    insert_collector_data(500)
    create_master_merged_table()
