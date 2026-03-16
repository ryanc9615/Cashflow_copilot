import time

# ingestion
from src.ingestion.ingest_transactions import ingest_transactions

#rules
from src.rules.merchant_rules import (
    create_merchant_rules_table,
    seed_merchant_rules
)

from src.rules.rule_engine import (
    create_rule_predictions_table,
    generate_rule_predictions   
)

#labels
from src.labels.transaction_labels import (
    create_transaction_labels_table,
    simulate_human_labels
)

# merchant memory
from src.memory.merchant_memory import (
    create_merchant_memory_table,
    update_merchant_memory_from_labels
)

# features
from src.features.build_training_dataset import create_training_dataset

# ML training
from src.models.train_transaction_classifier import train_transaction_classifier

# ML prediction
from src.models.predict_and_route import (
    create_model_predictions_table,
    main as run_model_predictions
)

# workflow inbox
from src.workflow.build_classification_inbox import create_classification_inbox

#pipeline metadata
from src.pipeline.run_metadata import (
    create_pipeline_runs_table,
    log_pipeline_run,
    calculate_rule_coverage
)

# human review

from src.review.human_review import run_human_review

# Ledger Summary

from src.workflow.build_final_ledger import(
    build_final_ledger,
    preview_final_ledger,
    ledger_summary
)

# review queue

from src.review.build_review_queue import(
    create_review_queue_table,
    populate_review_queue
)

from src.database.connection import get_connection

def run_pipeline():

    print("\n=============================")
    print(" CASHFLOW COPILOT PIPELINE")
    print("=============================")

    con = get_connection()
    
    print("\n[1] Ingesting transactions...")
    ingest_transactions()

    print("\n[2] Creating merchant rules...")
    create_merchant_rules_table(con)
    seed_merchant_rules(con)

    print("\n[3] Running rule engine...")
    create_rule_predictions_table(con)
    generate_rule_predictions(con)

    print("\n[4] Creating label table...")
    create_transaction_labels_table(con)

    print("\n[5] Simulating human labels...")
    simulate_human_labels(con)

    print("\n[6] Updating merchant memory...")
    create_merchant_memory_table(con)
    update_merchant_memory_from_labels(con)

    print("\n[7] Building training dataset...")
    create_training_dataset(con)

    print("\n[8] Training transaction classifier...")
    train_transaction_classifier()

    print("\n[9] Running ML prediction + routing...")
    create_model_predictions_table(con)
    run_model_predictions()

    print("\n[10] Building classification inbox...")
    create_classification_inbox(con)

    print("\n[11] Logging pipeline run...")
    
    create_pipeline_runs_table(con)

    rows_ingested = con.execute(
        "SELECT COUNT(*) FROM transactions"
    ).fetchone()[0]

    labelled_rows = con.execute(
        "SELECT COUNT(*) FROM transaction_labels"
    ).fetchone()[0]

    rule_coverage = calculate_rule_coverage(con)

    log_pipeline_run(con, rows_ingested, labelled_rows, rule_coverage)

    print("\n[12] Running human review loop...")
    run_human_review()
    
    print("\n[13] Building final ledger...")
    build_final_ledger(con)
    print("\n Ledger summary:")
    print(ledger_summary(con))
    print("\nSample ledger rows:")
    print(preview_final_ledger(con))

    print("\n[12] Building review queue...")
    create_review_queue_table(con)
    populate_review_queue(con)

    print("\n==============================")
    print(" PIPELINE COMPLETE")
    print("==============================\n")

if __name__ == "__main__":
    run_pipeline()