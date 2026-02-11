import pandas as pd
import psycopg2
from sdv.metadata import Metadata
from sdv.single_table import CTGANSynthesizer
from psycopg2.extras import execute_values

# 1. Database Configuration
DB_CONFIG = {
    "host": "localhost",
    "database": "debt_market_db",
    "user": "postgres",
    "password": "Bishal5100#"
}

def fetch_sample(limit=20000):
<<<<<<< HEAD
    """
    Fetches the generated data from Postgres to be used as the 
    Observation Space for the RL Agent.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    
    # Selecting all relevant S1 and S2 features
    query = f"""
        SELECT 
            age, 
            occupation, 
            income, 
            family_size, 
            region, 
            qualification, 
            cibil_score, 
            cibil_hit,
            overdue_months, 
            bounce_count, 
            emi_month, 
            current_demand, 
            total_demand, 
            pending_status, 
            last_call_status, 
            risk_category 
        FROM customer_profiles 
        LIMIT {limit}
    """
    
    try:
        # Load into DataFrame
        df = pd.read_sql(query, conn)
    except Exception as e:
        print(f"Error fetching data: {e}")
        df = pd.DataFrame() # Return empty DF on error
    finally:
        conn.close()
        
=======
    conn = psycopg2.connect(**DB_CONFIG)
    # We select specific columns to avoid issues with ID generation
    query = f"SELECT age, occupation, income, region, qualification, cibil_score, overdue_months, bounce_count, current_demand, total_demand, risk_category, profile_type FROM customer_profiles LIMIT {limit}"
    df = pd.read_sql(query, conn)
    conn.close()
>>>>>>> 8ae0e9c9762849f7ac87192bc41c8a729e3c0f2e
    return df

def train_and_generate_modern(df, num_to_generate=200000):
    print("--- Step 1: Detecting Metadata ---")
    # New SDV requires a Metadata object to understand your columns
    metadata = Metadata.detect_from_dataframe(data=df, table_name='customer_profiles')
    
    print("--- Step 2: Initializing CTGAN Synthesizer (GPU Enabled) ---")
    # 'cuda' is for NVIDIA GPUs. SDV will handle the move to GPU.
    synthesizer = CTGANSynthesizer(
        metadata, 
        enforce_rounding=False,
<<<<<<< HEAD
        epochs=300,           # Set to 100 for high quality
=======
        epochs=100,           # Set to 100 for high quality
>>>>>>> 8ae0e9c9762849f7ac87192bc41c8a729e3c0f2e
        verbose=True,
        cuda=True             # THIS ENABLES YOUR GPU
    )

    print("--- Step 3: Training (Watch your GPU usage!) ---")
    synthesizer.fit(df)

    print(f"--- Step 4: Generating {num_to_generate} records ---")
    synthetic_data = synthesizer.sample(num_rows=num_to_generate)
    
    # Add a new ID column for the DB
    synthetic_data.insert(0, 'customer_id', [f"GAN_{i}" for i in range(len(synthetic_data))])
    
    return synthetic_data

def save_to_postgres(df, table_name="synthetic_profiles_gan"):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Create the table based on the existing structure
    cur.execute(f"DROP TABLE IF EXISTS {table_name};")
    cur.execute(f"""
<<<<<<< HEAD
         CREATE TABLE synthetic_profiles_gan(
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
=======
        CREATE TABLE {table_name} (
            customer_id VARCHAR(20), age INT, occupation VARCHAR(50), income DECIMAL(12,2),
            region VARCHAR(10), qualification VARCHAR(50), cibil_score INT,
            overdue_months INT, bounce_count INT, current_demand DECIMAL(12,2),
            total_demand DECIMAL(12,2), risk_category VARCHAR(20), profile_type VARCHAR(10)
>>>>>>> 8ae0e9c9762849f7ac87192bc41c8a729e3c0f2e
        );
    """)
    
    # Bulk insert
    query = f"INSERT INTO {table_name} VALUES %s"
    execute_values(cur, query, df.values.tolist())
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"Done! Synthetic data saved to {table_name}")

if __name__ == "__main__":
    # Pull 10k rows from your existing Postgres table
    real_data = fetch_sample(20000)
    
    # Train and Generate
    synthetic_df = train_and_generate_modern(real_data, num_to_generate=200000)
    
    # Save back to Postgres
    save_to_postgres(synthetic_df)