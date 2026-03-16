from src.database.connection import get_connection


def create_classification_inbox(con):
    """
    Build unified classification inbox.

    Priority order:
    1. Merchant memory
    2. Rule engine
    3. ML model

    Determines suggested category and routing.
    """

    con.execute("""
        CREATE OR REPLACE TABLE classification_inbox AS

        SELECT
            t.transaction_id,
            t.date,
            t.description,
            t.merchant_key,

            -- determine category priority
            COALESCE(
                mm.category,
                rp.predicted_category,
                mp.predicted_category
            ) AS suggested_category,

            -- determine source
            CASE
                WHEN mm.category IS NOT NULL THEN 'merchant_memory'
                WHEN rp.predicted_category IS NOT NULL THEN 'rule_engine'
                WHEN mp.predicted_category IS NOT NULL THEN 'ml_model'
                ELSE 'unknown'
            END AS source,

            -- determine routing
            CASE
                WHEN mm.category IS NOT NULL THEN 'auto'
                WHEN rp.predicted_category IS NOT NULL THEN 'auto'
                WHEN mp.route = 'auto' THEN 'auto'
                ELSE 'review'
            END AS route

        FROM transactions t

        LEFT JOIN merchant_memory mm
            ON t.merchant_key = mm.merchant_key

        LEFT JOIN rule_predictions rp
            ON t.transaction_id = rp.transaction_id

        LEFT JOIN model_predictions mp
            ON t.transaction_id = mp.transaction_id
    """)


def preview_inbox(con):

    return con.execute("""
        SELECT *
        FROM classification_inbox
        LIMIT 20
    """).df()


def inbox_summary(con):

    return con.execute("""
        SELECT
            source,
            route,
            COUNT(*) AS count
        FROM classification_inbox
        GROUP BY source, route
        ORDER BY count DESC
    """).df()


def main():

    con = get_connection()

    create_classification_inbox(con)

    print("Classification inbox created.\n")

    print("Inbox summary:")
    print(inbox_summary(con))

    print("\nSample rows:")
    print(preview_inbox(con))


if __name__ == "__main__":
    main()