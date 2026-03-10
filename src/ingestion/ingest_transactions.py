import pandas as pd
import uuid
from pathlib import Path

from src.database.connection import get_connection

def ingest_transactions():

    # locate project root
    project_root = Path(__file__).resolve().parent.parent.parent

    #path to raw csv
    data_path = project_root / "data" / "raw" / "synthetic_bank_transactions.csv"

    # load csv
    df = pd.read_csv(data_path)

    print(f"Loaded {len(df)} rows from CSV")

    # generate transaction ids
    df["transaction_id"] = [str(uuid.uuid4()) for _ in range(len(df))]

    # merchant extraction
    df["merchant_key"] = (
        df["description"]
        .str.lower()
        .str.replace(r"[^a-zA-Z\s]", "", regex=True)
        .str.split()
        .str[0]
    )

    #reorder columns
    df = df[
        [
            "transaction_id",
            "date",
            "amount",
            "currency",
            "channel",
            "description",
            "merchant_key",
            "category"
        ]
    ]

    # connect to database
    con = get_connection()

    # register dataframe as SQL table
    con.register("transactions_df", df)

    # replace transactions table
    con.execute("""
        CREATE OR REPLACE TABLE transactions AS
        SELECT * FROM transactions_df
    """)

    print("Transactions table rebuilt successfully")

if __name__ == "__main__":
    ingest_transactions()