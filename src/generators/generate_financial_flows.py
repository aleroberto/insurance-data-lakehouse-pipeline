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
    f"{BASE_DIR}/data/raw/financial_flows/ingestion_date={date_ingestion}"
)

output_dir.mkdir(parents=True, exist_ok=True)
output_file = output_dir / f"financial_flows_{date_ingestion}.csv"

TOTAL_RECORDS = 5000

flow_types = [
    "PREMIUM_PAYMENT",
    "CLAIM_PAYMENT",
    "COMMISSION",
    "REFUND",
    "REINSURANCE",
    "CANCELLATION"
]

payment_methods = [
    "PIX",
    "BANK_SLIP",
    "CREDIT_CARD",
    "DEBIT",
    "BANK_TRANSFER"
]

flow_statuses = [
    "PENDING",
    "PROCESSED",
    "FAILED",
    "CANCELLED"
]

financial_flows = []

for i in range(1, TOTAL_RECORDS + 1):

    transaction_date = fake.date_between(
        start_date="-2y",
        end_date="today"
    )

    processing_date = transaction_date + timedelta(
        days=random.randint(0, 5)
    )

    amount = round(random.uniform(50, 25000), 2)

    flow_type = random.choice(flow_types)
    status = random.choice(flow_statuses)

    if status == "FAILED":
        processed_amount = 0
    else:
        processed_amount = amount

    financial_flows.append({
        "flow_id": f"FLOW-{i:010d}",
        "policy_id": f"POL-{random.randint(1, 500):08d}",
        "claim_id": f"CLM-{random.randint(1, 1000):08d}",
        "flow_type": flow_type,
        "payment_method": random.choice(payment_methods),
        "flow_status": status,
        "transaction_date": transaction_date.strftime("%Y-%m-%d"),
        "processing_date": processing_date.strftime("%Y-%m-%d"),
        "amount": amount,
        "processed_amount": processed_amount,
        "currency": "BRL",
        "customer_document": fake.cpf(),
        "bank_name": fake.company(),
        "city": fake.city(),
        "state": fake.estado_sigla(),
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

df = pd.DataFrame(financial_flows)

df.to_csv(output_file, index=False, encoding="utf-8")

print(f"Arquivo gerado com sucesso: {output_file}")
print(f"Total de registros: {len(df)}")