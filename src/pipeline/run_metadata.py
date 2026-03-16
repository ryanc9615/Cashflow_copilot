from datetime import datetime
def create_pipeline_runs_table(con):
    """
    Create table to log pipeline execution metadata.
    """

    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id INTEGER PRIMARY KEY,
            run_timestamp TIMESTAMP,
            rows_ingested INTEGER,
            labelled_rows INTEGER,
            rule_coverage FLOAT,
            model_version VARCHAR
        )
    """)


def calculate_rule_coverage(con):
    """
    Calculate proportion of transactions matched by rules.
    """

    result = con.execute("""
        SELECT
            COUNT(r.predicted_category) * 1.0 / COUNT(*) AS coverage
        FROM transactions t
        LEFT JOIN rule_predictions r
            ON t.transaction_id = r.transaction_id
    """).fetchone()

    return result[0]


def log_pipeline_run(con, rows_ingested, labelled_rows, rule_coverage):
    """
    Insert one pipeline run record.
    """

    con.execute("""
        INSERT INTO pipeline_runs
        SELECT
            COALESCE(MAX(run_id), 0) + 1,
            CURRENT_TIMESTAMP,
            ?,
            ?,
            ?,
            'v1'
        FROM pipeline_runs
    """, [rows_ingested, labelled_rows, rule_coverage])

