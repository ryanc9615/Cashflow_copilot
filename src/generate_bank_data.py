import pandas as pd
import random
from pathlib import Path
from datetime import datetime, timedelta

# Generate categories and merchants in categories
categories = {
    "software": ["OPEN API", "NOTION LABS", "AWS CLOUD", "GITHUB"],
    "travel": ["UBER TRIP", "TRAINLINE UK", "RYANAIR FLIGHT"],
    "rent": ["WEWORK RENT", "OFFICE RENT LTD"],
    "marketing": ["GOOGLE ADS", "FACEBOOK ADS"],
    "meals": ["PRET A MANGER", "STARBUCKS"],
    "equipment": ["AMAZON", "APPLE STORE"]
}

# initilise a blank list
rows = []

# initilise bank statement start date
start_date = datetime(2025,1,1)

# randomly generate 3000 lines of bank data 
for i in range(3000):

    category = random.choice(list(categories.keys()))
    description = random.choice(categories[category])

    amount = round(random.uniform(5,200),2)

    if category == "rent":
        amount = round(random.uniform(500,2000),2)

    rows.append({
        "date": start_date + timedelta(days=random.randint(0,180)),
        "description": description,
        "amount": -amount,
        "currency": "GBP",
        "channel": random.choice(["card","transfer","direct_debit"]),
        "category": category
    })

# load randomly generated data into dataframe
df = pd.DataFrame(rows)

# define path directory
project_root = Path(__file__).resolve().parent.parent
output_path = project_root / "data" / "raw" / "synthetic_bank_transactions.csv"

output_path.parent.mkdir(parents=True, exist_ok=True)

# send file to path directory as csv
df.to_csv(output_path, index=False)

print(f"Synthetic bank dataset created at: {output_path}")
