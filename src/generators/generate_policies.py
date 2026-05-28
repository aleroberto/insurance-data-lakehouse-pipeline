from faker import Faker
import pandas as pd
import random
from pathlib import Path
from datetime import datetime, timedelta


date_ingestion = datetime.now().strftime("%Y-%m-%d")

fake = Faker("pt_BR")
BASE_DIR = Path(__file__).resolve().parents[2]

def log_info(message):
    print(f"{datetime.now().strftime('%y/%m/%d %H:%M:%S')} INFO GeneratePolicies: {message}")

OUTPUT_PATH = Path(
    f"{BASE_DIR}/data/raw/policies/ingestion_date={date_ingestion}"
)
output_file = OUTPUT_PATH / f"policies_{date_ingestion}.csv"

OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

PRODUCT_TYPES = [
    "AUTO",
    "HOME",
    "LIFE",
    "HEALTH",
    "TRAVEL"
]

POLICY_STATUS = [
    "ACTIVE",
    "CANCELLED",
    "EXPIRED",
    "PENDING"
]

INSURERS = [
    "INS001",
    "INS002",
    "INS003"
]

records = []

TOTAL_RECORDS = 30

for i in range(TOTAL_RECORDS):

    start_date = fake.date_between(
        start_date="-2y",
        end_date="today"
    )

    end_date = start_date + timedelta(
        days=random.randint(30, 3650)
    )

    premium_amount = round(
        random.uniform(300, 15000),
        2
    )

    policy = {
        "policy_id": f"POL{i:06}",
        "customer_id": f"CUST{random.randint(1, 300):05}",
        "insurer_id": random.choice(INSURERS),
        "product_type": random.choice(PRODUCT_TYPES),
        "premium_amount": premium_amount,
        "policy_start_date": start_date,
        "policy_end_date": end_date,
        "status": random.choice(POLICY_STATUS),
        "created_at": datetime.now().isoformat()
    }

    # -----------------------------
    # INJECTING INVALID RECORDS
    # -----------------------------

    random_error = random.random()

    # policy_id nulo
    if random_error < 0.02:
        policy["policy_id"] = None

    # premium negativo
    elif random_error < 0.04:
        policy["premium_amount"] = -1000

    # end_date menor que start_date
    elif random_error < 0.06:
        policy["policy_end_date"] = (
            start_date - timedelta(days=10)
        )

    # status inválido
    elif random_error < 0.08:
        policy["status"] = "INVALID_STATUS"

    records.append(policy)

df = pd.DataFrame(records)
df.to_csv(output_file, index=False)

log_info("Policies dataset generated successfully")
log_info(f"File saved at: {output_file}")
log_info(f"Total records: {len(df)}")