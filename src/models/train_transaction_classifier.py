from src.database.connection import get_connection

import pandas as pd

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from pathlib import Path

import joblib

def train_transaction_classifier():

    con = get_connection()

    # ------------------------------
    # 1 Load training dataset
    # ------------------------------

    df = con.execute("""
        SELECT
            normalised_description,
            category
        FROM training_transactions
    """).df()

    print(f"Loaded {len(df)} labelled transactions")

    # ------------------------------
    # 2 Split features and labels
    # ------------------------------

    X_text = df["normalised_description"]
    y = df["category"]

    # ------------------------------
    # 3 Train / test split
    # ------------------------------ 

    X_train, X_test, y_train, y_test = train_test_split(
        X_text,
        y,
        test_size=0.2,
        random_state=42
    )

    # ------------------------------
    # 4 TF-IDF vectorisation
    # ------------------------------    

    vectorizer = TfidfVectorizer(
        stop_words="english",
        max_features=5000
    )

    X_train_vec = vectorizer.fit_transform(X_train)
    X_test_vec = vectorizer.transform(X_test)

    # ------------------------------
    # 5 Train classifier
    # ------------------------------   

    model = LogisticRegression(max_iter=1000)
    model.fit(X_train_vec, y_train)

    # ------------------------------
    # 6 Evaluate model
    # ------------------------------

    y_pred = model.predict(X_test_vec)

    print("\nModel performance:")
    print(classification_report(y_test, y_pred))

    # ------------------------------
    # 7 Save model
    # ------------------------------ 

    project_root = Path(__file__).resolve().parent.parent.parent
    models_dir = project_root / "models"
    models_dir.mkdir(parents=True, exist_ok=True)

    joblib.dump(model, models_dir / "transaction_classifier.pkl")
    joblib.dump(vectorizer, models_dir / "tfidf_vectorizer.pkl")

    print("\nModel saved.")

if __name__ == "__main__":
    train_transaction_classifier()