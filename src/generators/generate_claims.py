from faker import Faker
import pandas as pd
import random
from pathlib import Path
from datetime import datetime, timedelta

fake = Faker("pt_BR")
random.seed(42)

BASE_DIR = Path(__file__).resolve().parent.parent

date_ingestion = datetime.now().strftime("%Y-%m-%d")

output_dir = Path(
    f"{BASE_DIR}/data/raw/claims/ingestion_date={date_ingestion}"
)

output_dir.mkdir(parents=True, exist_ok=True)

output_file = output_dir / f"claims_{date_ingestion}.csv"

TOTAL_RECORDS = 3

claim_statuses = ["OPEN", "APPROVED", "REJECTED", "PAID", "CANCELLED"]
claim_types = ["COLLISION", "THEFT", "FIRE", "FLOOD", "THIRD_PARTY", "GLASS_DAMAGE"]

claims = []

for i in range(1, TOTAL_RECORDS + 1):
    occurrence_date = fake.date_between(start_date="-2y", end_date="today")
    reported_date = occurrence_date + timedelta(days=random.randint(0, 30))

    claim_amount = round(random.uniform(500, 80000), 2)
    approved_amount = claim_amount if random.random() > 0.25 else round(claim_amount * random.uniform(0.2, 0.9), 2)

    status = random.choice(claim_statuses)

    if status in ["REJECTED", "CANCELLED"]:
        approved_amount = 0

    claims.append({
        "claim_id": f"CLM-{i:08d}",
        "policy_id": f"POL-{random.randint(1, 500):08d}",
        "claim_type": random.choice(claim_types),
        "claim_status": status,
        "occurrence_date": occurrence_date.strftime("%Y-%m-%d"),
        "reported_date": reported_date.strftime("%Y-%m-%d"),
        "claim_amount": claim_amount,
        "approved_amount": approved_amount,
        "city": fake.city(),
        "state": fake.estado_sigla(),
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

df = pd.DataFrame(claims)

df.to_csv(output_file, index=False, encoding="utf-8")

print(f"Arquivo gerado com sucesso: {output_file}")
print(f"Total de registros: {len(df)}")