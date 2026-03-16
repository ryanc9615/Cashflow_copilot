from pathlib import Path

import joblib
import pandas as pd

from src.database.connection import get_connection

CONFIDENCE_THRESHOLD = 0.75

def create_model_predictions_table(con):
    """
    Create a table to store model predictions and routing decisions.
    """
    con.execute("""
        CREATE TABLE IF NOT EXISTS model_predictions(
            transaction_id VARCHAR,
            predicted_category VARCHAR,
            confidence DOUBLE,
            route VARCHAR,
            model_source VARCHAR
        )
""")
    
def load_model_and_vectorizer():
    """
    Load trained classifier and TF-IDF vectorizer from disk.
    """
    project_root = Path(__file__).resolve().parent.parent.parent

    model_path = project_root / "models" / "transaction_classifier.pkl"
    vectorizer_path = project_root / "models" / "tfidf_vectorizer.pkl"

    model = joblib.load(model_path)
    vectorizer = joblib.load(vectorizer_path)

    return model, vectorizer

def get_unmatched_transactions(con):
    """
    Return transactions that were NOT matched by merchant memory
    and NOT matched by rule predictions.
    """
    return con.execute("""
        SELECT
            t.transaction_id,
            t.normalised_description
        FROM transactions t
        LEFT JOIN merchant_memory m
            ON t.merchant_key = m.merchant_key
        LEFT JOIN rule_predictions r
            ON t.transaction_id = r.transaction_id
        WHERE m.merchant_key IS NULL
            AND r.transaction_id IS NULL
    """).df()

def predict_with_confidence(model, vectorizer, df):
    """
    Generate predicted category, confidence, and route for each transaction.
    """
    if df.empty:
        return pd.DataFrame(columns=[
            "transaction_id",
            "predicted_category",
            "confidence",
            "route",
            "model_source"
        ])
    
    X_vec = vectorizer.transform(df["normalised_description"])

    predicted_categories = model.predict(X_vec)
    predicted_probabilities = model.predict_proba(X_vec)

    max_confidence = predicted_probabilities.max(axis=1)

    prediction_df = pd.DataFrame({
        "transaction_id": df["transaction_id"].values,
        "predicted_category": predicted_categories,
        "confidence": max_confidence
    })

    prediction_df["route"] = prediction_df["confidence"].apply(
        lambda x: "auto" if x >= CONFIDENCE_THRESHOLD else "review"
    )

    prediction_df["model_source"] = "logistic_regression"

    return prediction_df

def save_model_predictions(con, prediction_df):
    """
    Save model predictions into DuckDB.
    """
    con.execute("DELETE FROM model_predictions")
    con.register("prediction_df", prediction_df)

    con.execute("""
        INSERT INTO model_predictions
        SELECT * FROM prediction_df
    """)

def preview_model_predictions(con):
    return con.execute("""
        SELECT *
        FROM model_predictions
        LIMIT 20
    """).df()

def routing_summary(con):
    return con.execute("""
        SELECT
            route,
            COUNT(*) AS count
        FROM model_predictions
        GROUP BY route
        ORDER BY count DESC
    """).df()

def main():
    con = get_connection()

    create_model_predictions_table(con)

    model, vectorizer = load_model_and_vectorizer()

    df_unmatched = get_unmatched_transactions(con)

    print(f"Unmatched transactions to classify: {len(df_unmatched)}")

    prediction_df = predict_with_confidence(model, vectorizer, df_unmatched)

    save_model_predictions(con, prediction_df)

    print("\nRouting summary:")
    print(routing_summary(con))

    print("\nPreview model predictions:")
    print(preview_model_predictions(con))

if __name__ == "__main__":
    main()