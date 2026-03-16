from src.database.connection import get_connection

def create_training_dataset(con):
    """
    Create a clean training dataset from confirmed labels.
    """

    con.execute("""
        CREATE OR REPLACE TABLE training_transactions AS
        SELECT
            t.transaction_id,
            t.normalised_description,
            t.merchant_key,
            l.category
        FROM transactions t
        JOIN transaction_labels l
            ON t.transaction_id = l.transaction_id
    """)

def preview_training_dataset(con):

    return con.execute("""
        SELECT *
        FROM training_transactions
        LIMIT 20
    """).df()

def dataset_summary(con):

    return con.execute("""
        SELECT
            category,
            COUNT(*) AS count
        FROM training_transactions
        GROUP BY category
        ORDER BY count DESC
    """).df()

def main():

    con = get_connection()

    create_training_dataset(con)

    print("Training dataset created.\n")

    print("Sample rows:")
    print(preview_training_dataset(con))

    print("\nCategory distribution:")
    print(dataset_summary(con))

if __name__ == "__main__":
    main()