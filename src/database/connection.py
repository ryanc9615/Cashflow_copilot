from pathlib import Path
import duckdb

def get_connection():
    """
    Returns a DuckDB connection to the project database.
    """
    project_root = Path(__file__).resolve().parent.parent.parent
    db_path = project_root / "data" / "database" / "cashflow.duckdb"

    # ensure directory exists 
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))

    return con