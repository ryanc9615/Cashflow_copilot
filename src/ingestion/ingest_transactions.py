import pandas as pd
import uuid
from pathlib import Path

from src.database.connection import get_connection
from src.features.text_preprocessing import normalise_transaction_text

def ingest_transactions():

    # locate project root
    project_root = Path(__file__).resolve().parent.parent.parent

    #path to raw csv
    data_path = project_root / "data" / "raw" / "synthetic_bank_transactions.csv"

    # load csv
    df = pd.read_csv(data_path)

    print(f"Loaded {len(df)} rows from CSV")

    # basic data validation
    df = df.dropna(subset=["description", "amount"])
    df = df.reset_index(drop=True)

    # generate transaction ids
    df["transaction_id"] = [str(uuid.uuid4()) for _ in range(len(df))]

    # normalise transaction ids
    df["normalised_description"] = df["description"].apply(normalise_transaction_text)

    # merchant extraction
    df["merchant_key"] = (
        df["normalised_description"]
        .str.split()
        .str[:2].str.join("_")
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
            "normalised_description",
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

if __name__ == "__main__":
    ingest_transactions()