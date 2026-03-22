import pandas as pd
import random
from pathlib import Path
from datetime import datetime, timedelta


def generate_bank_data(n_rows: int = 3000, seed: int = 42):
    """
    Generate a more realistic synthetic bank transaction dataset.

    Improvements:
    - ambiguous merchants across categories
    - merchant variants
    - payment processor prefixes
    - random numeric noise
    - noisy location / suffix tokens
    """

    random.seed(seed)

    merchant_catalog = {
        "amazon": [
            {"description": "AMZN Mktp", "category": "equipment"},
            {"description": "Amazon Marketplace", "category": "equipment"},
            {"description": "Amazon EU", "category": "equipment"},
            {"description": "AMAZON AWS", "category": "software"},
            {"description": "Amazon Web Services", "category": "software"},
        ],
        "uber": [
            {"description": "UBER TRIP", "category": "travel"},
            {"description": "UBER BV", "category": "travel"},
            {"description": "UBER *TRIP", "category": "travel"},
            {"description": "UBER EATS", "category": "meals"},
        ],
        "google": [
            {"description": "GOOGLE ADS", "category": "marketing"},
            {"description": "GOOGLE *ADS", "category": "marketing"},
            {"description": "GOOGLE CLOUD", "category": "software"},
        ],
        "apple": [
            {"description": "APPLE STORE", "category": "equipment"},
            {"description": "APPLE.COM BILL", "category": "software"},
        ],
        "stripe": [
            {"description": "STRIPE PAYMENTS", "category": "software"},
            {"description": "STRIPE FEE", "category": "software"},
        ],
        "notion": [
            {"description": "NOTION LABS", "category": "software"},
        ],
        "github": [
            {"description": "GITHUB", "category": "software"},
        ],
        "datadog": [
            {"description": "DATADOG", "category": "software"},
        ],
        "cloudflare": [
            {"description": "CLOUDFLARE", "category": "software"},
        ],
        "figma": [
            {"description": "FIGMA", "category": "software"},
        ],
        "trainline": [
            {"description": "TRAINLINE UK", "category": "travel"},
            {"description": "TRAINLINE TICKET", "category": "travel"},
        ],
        "ryanair": [
            {"description": "RYANAIR FLIGHT", "category": "travel"},
            {"description": "RYANAIR", "category": "travel"},
        ],
        "wework": [
            {"description": "WEWORK RENT", "category": "rent"},
        ],
        "office_rent": [
            {"description": "OFFICE RENT LTD", "category": "rent"},
        ],
        "pret": [
            {"description": "PRET A MANGER", "category": "meals"},
        ],
        "starbucks": [
            {"description": "STARBUCKS", "category": "meals"},
            {"description": "STARBUCKS LONDON", "category": "meals"},
        ],
        "facebook": [
            {"description": "FACEBOOK ADS", "category": "marketing"},
            {"description": "META ADS", "category": "marketing"},
        ],
    }

    payment_processors = ["STRIPE", "PAYPAL", "SQUARE"]
    location_tokens = ["LONDON", "UK", "EU", "GB", "INC", "LTD", "BV", "HELP"]
    channels = ["card", "transfer", "direct_debit"]

    rows = []
    start_date = datetime(2025, 1, 1)

    merchant_keys = list(merchant_catalog.keys())

    for _ in range(n_rows):
        merchant = random.choice(merchant_keys)
        merchant_variant = random.choice(merchant_catalog[merchant])

        description = merchant_variant["description"]
        category = merchant_variant["category"]

        # Add a payment processor prefix to obscure the merchant sometimes
        if random.random() < 0.15:
            processor = random.choice(payment_processors)
            description = f"{processor} {description}"

        # Add a noisy location / legal suffix token sometimes
        if random.random() < 0.35:
            suffix = random.choice(location_tokens)
            description = f"{description} {suffix}"

        # Add numeric noise often
        if random.random() < 0.60:
            description = f"{description} {random.randint(1000, 9999)}"

        # Amount logic by category
        if category == "rent":
            amount = round(random.uniform(500, 2000), 2)
        elif category == "marketing":
            amount = round(random.uniform(50, 500), 2)
        elif category == "software":
            amount = round(random.uniform(10, 300), 2)
        elif category == "travel":
            amount = round(random.uniform(8, 250), 2)
        elif category == "meals":
            amount = round(random.uniform(4, 40), 2)
        elif category == "equipment":
            amount = round(random.uniform(20, 1000), 2)
        else:
            amount = round(random.uniform(5, 200), 2)

        rows.append(
            {
                "date": start_date + timedelta(days=random.randint(0, 180)),
                "description": description,
                "amount": -amount,
                "currency": "GBP",
                "channel": random.choice(channels),
                "category": category,
            }
        )

    df = pd.DataFrame(rows)

    project_root = Path(__file__).resolve().parent.parent.parent
    output_path = project_root / "data" / "raw" / "synthetic_bank_transactions.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(output_path, index=False)

    print(f"Synthetic bank dataset created at: {output_path}")
    print("\nSample rows:")
    print(df.head(10))


if __name__ == "__main__":
    generate_bank_data()