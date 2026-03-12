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


def main():
    con = get_connection()

    create_merchant_rules_table(con)
    seed_merchant_rules(con)

    print("Merchant rules table created and seeded.\n")

    print("Merchant rules:")
    print(preview_merchant_rules(con))

if __name__ == "__main__":
    main()
