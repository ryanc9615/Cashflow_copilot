from src.database.connection import get_connection

def create_rule_predictions_table(con):
    """
    Create table to store rule-based predictions.
    """
    con.execute("""
        CREATE TABLE IF NOT EXISTS rule_predictions(
            transaction_id VARCHAR,
            predicted_category VARCHAR,
            rule_pattern VARCHAR,
            rule_source VARCHAR
        )
    """)

def generate_rule_predictions(con):
    """
    Apply merchant rules to transactions and generate predictions. 
    """

    con.execute("""
        CREATE OR REPLACE TABLE rule_predictions AS
        SELECT
            t.transaction_id,
            r.category AS predicted_category,
            r.pattern AS rule_pattern,
            'merchant_rule' AS rule_source
        FROM transactions t
        JOIN merchant_rules r
            ON t.merchant_key = r.merchant_key
            AND LOWER(t.description) LIKE '%' || LOWER(r.pattern) || '%'
    """)
    
def preview_rule_predictions(con):

    return con.execute("""
        SELECT *
        FROM rule_predictions
        LIMIT 20
    """).df()

def rule_coverage_summary(con):
    """
    Show how many transactions are matched by rules. 
    """
    return con.execute("""
        SELECT
            COUNT(*) AS total_transactions,
            COUNT(r.category) AS matched_by_rules
        FROM transactions t
        LEFT JOIN merchant_rules r
            ON t.merchant_key = r.merchant_key
            AND LOWER(t.description) LIKE '%' || LOWER(r.pattern) || '%'
    """).df()

def get_unmatched_transactions(con):
    return con.execute("""
        SELECT
            t.transaction_id,
            t.description,
            t.category
        FROM transactions t
        LEFT JOIN rule_predictions r
            ON t.transaction_id = r.transaction_id
        WHERE r.transaction_id IS NULL
    """).df()

def preview_unmatched_transactions(con):
    return con.execute("""
        SELECT
            t.transaction_id,
            t.description,
            t.category
        FROM transactions t
        LEFT JOIN rule_predictions r 
            ON t.transaction_id = r.transaction_id
        WHERE r.transaction_id is NULL
        LIMIT 20
    """).df()

def unmatched_transaction_count(con):
    return con.execute("""
        SELECT COUNT(*) AS unmatched_count
        FROM transactions t
        LEFT JOIN rule_predictions r 
            ON t.transaction_id = r.transaction_id
        WHERE r.transaction_id IS NULL
    """).df()

def main():

    con = get_connection()

    create_rule_predictions_table(con)
    generate_rule_predictions(con)

    print("Rule predictions generated.\n")

    print("Rule coverage summary:")
    print(rule_coverage_summary(con))

    print("\nPreview rule predictions:")
    print(preview_rule_predictions(con))

    print("\nUnmatched transaction count:")
    print(unmatched_transaction_count(con))

    print("\nPreview unmatched transactions:")
    print(preview_unmatched_transactions(con))

if __name__ == "__main__":
    main()
