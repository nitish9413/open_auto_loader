import os

import polars as pl

from open_auto_loader import OpenAutoLoader

# 1. Setup local environment
DATA_DIR = "./data/landing_zone"
os.makedirs(DATA_DIR, exist_ok=True)

# 2. Generate mock data using Polars
df = pl.DataFrame(
    {
        "id": [1, 2, 3],
        "user": ["nitish", "dev_user", "tester"],
        "status": ["active", "pending", "active"],
    }
)

# Write two files to simulate a multi-file ingestion
df.write_csv(f"{DATA_DIR}/batch_1.csv")
df.slice(2, 1).write_csv(f"{DATA_DIR}/batch_2.csv")

print(f"✅ Created mock data in {DATA_DIR}")

# 3. Initialize the AutoLoader (Local Mode)
loader = OpenAutoLoader(
    source=DATA_DIR,
    target="./delta",
    check_point="./check_point",
    schema_path="./schema",
    format_type="csv",
    table_type="delta",
)

# 4. Execute
# 'test123' acts as the job/batch identifier
loader.run("test123")

print("🚀 Local Polars ingestion complete!")
