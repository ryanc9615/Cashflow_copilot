from src.database.connection import get_connection

def create_merchant_rules_table(con):
    """
    Create the merchant_rules table if not already exist
    """
    con.execute("""
        CREATE TABLE IF NOT EXISTS merchant_rules(
            merchant_key VARCHAR,
            pattern VARCHAR,
            category VARCHAR
        )
    """)

def seed_merchant_rules(con):
    """
    Insert starter rules. 
    """
    con.execute("DELETE FROM merchant_rules")

    con.execute("""
        INSERT INTO merchant_rules (merchant_key, pattern, category)
        VALUES
            ('uber', 'trip', 'travel'),
            ('google', 'ads', 'marketing'),
            ('openai', 'api', 'software'),
            ('notion', 'labs', 'software'),
            ('pret', 'manger', 'meals'),
            ('starbucks', 'starbucks', 'meals'),
            ('trainline', 'trainline', 'travel'),
            ('ryanair', 'flight', 'travel'),
            ('wework', 'rent', 'rent'),
            ('amazon', 'amazon', 'equipment')
    """)

def preview_merchant_rules(con):
    """
    Show the current rules.
    """
    return con.execute("""
        SELECT *
        FROM merchant_rules
        ORDER BY merchant_key, pattern
    """).df()

def apply_merchant_rules(con):
    """
    Apply rules to transactions
    Returns previously matched transactions
    """
    return con.execute("""
        SELECT
            t.transaction_id,
            t.date,
            t.description,
            t.merchant_key,
            r.pattern,
            r.category AS rule_category,
            t.category AS true_category
        FROM transactions t
        LEFT JOIN merchant_rules r
            ON t.merchant_key = r.merchant_key
            AND LOWER(t.description) LIKE '%' || LOWER(r.pattern) || '%'
        WHERE r.category IS NOT NULL
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

def main():
    con = get_connection()

    create_merchant_rules_table(con)
    seed_merchant_rules(con)

    print("Merchant rules table created and seeded.\n")

    print("Merchant rules:")
    print(preview_merchant_rules(con))

    print("\nRule coverage summary:")
    print(rule_coverage_summary(con))

    print("\nSample matched transactions:")
    print(apply_merchant_rules(con))

if __name__ == "__main__":
    main()
