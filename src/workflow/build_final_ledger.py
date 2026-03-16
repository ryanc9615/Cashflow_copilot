from src.database.connection import get_connection

def build_final_ledger(con):

    con.execute("""
        CREATE OR REPLACE TABLE final_transaction_ledger AS 
                
        SELECT
            t.transaction_id,
            t.date,
            t.description,
            COALESCE(
                l.category,
                ci.suggested_category
            ) AS final_category,
                
            CASE
                WHEN l.label_source = 'human_review' THEN 'human_review'
                ELSE ci.source
            END AS classification_source,
                
            CASE
                WHEN l.label_source = 'human_review' THEN 'reviewed'
                ELSE 'auto'
            END AS status
                
        FROM transactions t
                
        LEFT JOIN classification_inbox ci
            ON t.transaction_id = ci.transaction_id
        
        LEFT JOIN transaction_labels l
            ON t.transaction_id = l.transaction_id
    """)

def preview_final_ledger(con):

    return con.execute("""
        SELECT *
        FROM final_transaction_ledger
        LIMIT 20
    """).df()

def ledger_summary(con):

    return con.execute("""
        SELECT 
            classification_source,
            status,
            COUNT(*) AS count
        FROM final_transaction_ledger
        GROUP BY classification_source, status
        ORDER BY count DESC
    """).df()