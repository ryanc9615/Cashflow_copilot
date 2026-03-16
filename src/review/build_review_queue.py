from src.database.connection import get_connection

def create_review_queue_table(con):

    con.execute("""
        CREATE TABLE IF NOT EXISTS review_queue (
            transaction_id VARCHAR,
            suggested_category VARCHAR,
            review_status VARCHAR,
            reviewer_id VARCHAR,
            reviewed_at TIMESTAMP
        )
    """)

def populate_review_queue(con):

    con.execute("""
        INSERT INTO review_queue
        SELECT 
            transaction_id,
            suggested_category,
            'pending',
            NULL,
            NULL
        FROM classification_inbox
        WHERE route = 'review'
    """)

def preview_review_queue(con):

    return con.execute("""
        SELECT *
        FROM review_queue
        LIMIT 20
    """).df()