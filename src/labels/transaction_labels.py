from src.database.connection import get_connection

def create_transaction_labels_table(con):

    con.execute("""
        CREATE TABLE IF NOT EXISTS transaction_labels (
            transaction_id VARCHAR,
            category VARCHAR,
            label_source VARCHAR,
            labeled_at TIMESTAMP
        )
    """)

def simulate_human_labels(con):
    """
    Simulate human confirmed labels for development. 
    We use the existing category column from synthetic data.
    """
    con.execute("DELETE FROM transaction_labels")

    con.execute("""
        INSERT INTO transaction_labels
        SELECT
            transaction_id,
            category,
            'simulated_human',
            CURRENT_TIMESTAMP
        FROM transactions
        LIMIT 500
    """)

def preview_transaction_labels(con):

    return con.execute("""
        SELECT *
        FROM transaction_labels
        LIMIT 20
    """).df()

def main():

    con = get_connection()

    create_transaction_labels_table(con)

    print("Transaction labels table ready.\n")
    print(preview_transaction_labels(con))

if __name__ == "__main__":
    main()