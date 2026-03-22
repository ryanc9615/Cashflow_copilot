from src.database.connection import get_connection


def create_merchant_memory_table(con):
    """
    Create merchant memory table if it does not exist.
    """

    con.execute("""
        CREATE TABLE IF NOT EXISTS merchant_memory (
            merchant_key VARCHAR,
            category VARCHAR,
            observation_count INTEGER,
            last_seen TIMESTAMP
        )
    """)


def update_merchant_memory_from_labels(con, sample_ratio=0.25):
    """
    Update merchant memory using ONLY human-confirmed labels,
    and simulate gradual learning via sampling.

    sample_ratio controls how much of the labelled data is used.
    """

    # Reset table (idempotent for dev)
    con.execute("DELETE FROM merchant_memory")

    threshold = int(sample_ratio * 100)

    con.execute(f"""
        INSERT INTO merchant_memory
        SELECT
            t.merchant_key,
            l.category,
            COUNT(*) AS observation_count,
            CURRENT_TIMESTAMP AS last_seen
        FROM transaction_labels l
        JOIN transactions t
            ON l.transaction_id = t.transaction_id
        WHERE l.label_source IN ('simulated_human', 'human_review')
          AND ABS(HASH(l.transaction_id)) % 100 < {threshold}
        GROUP BY
            t.merchant_key,
            l.category
    """)


def preview_merchant_memory(con):
    """
    Preview merchant memory contents.
    """

    return con.execute("""
        SELECT *
        FROM merchant_memory
        ORDER BY observation_count DESC
        LIMIT 20
    """).df()


def merchant_memory_coverage(con):
    """
    Show how many transactions are covered by merchant memory.
    """

    return con.execute("""
        SELECT
            COUNT(*) AS total_transactions,
            COUNT(m.category) AS matched_by_memory
        FROM transactions t
        LEFT JOIN merchant_memory m
            ON t.merchant_key = m.merchant_key
    """).df()


def main():

    con = get_connection()

    create_merchant_memory_table(con)

    update_merchant_memory_from_labels(con)

    print("Merchant memory updated from confirmed labels.\n")

    print("Merchant memory preview:")
    print(preview_merchant_memory(con))

    print("\nMerchant memory coverage:")
    print(merchant_memory_coverage(con))


if __name__ == "__main__":
    main()