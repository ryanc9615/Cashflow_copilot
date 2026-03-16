from src.database.connection import get_connection
import random

def fetch_review_queue(con):

    return con.execute("""
        SELECT
            transaction_id,
            suggested_category
        FROM classification_inbox
        WHERE route = 'review'
    """).df()

def simulate_human_review(df):

    decisions = []

    for _, row in df.iterrows():

        transaction_id = row["transaction_id"]
        suggestion = row["suggested_category"]

        # simulate correction 10% of the time

        if random.random() < 0.1:
            corrected_category = random.choice(
                ["software", "travel", "meals", "marketing", "equipment"]
            )
        else:
            corrected_category = suggestion

        decisions.append(
            {
                "transaction_id": transaction_id,
                "category": corrected_category
            }
        )
    return decisions

def write_reviewed_labels(con, decisions):

    for decision in decisions:

        con.execute("""
            INSERT INTO transaction_labels
            (transaction_id, category, label_source)
            VALUES (?, ?, 'human_review')
        """, [
            decision["transaction_id"],
            decision["category"]
        ])

def run_human_review():

    con = get_connection()

    review_df = fetch_review_queue(con)

    if review_df.empty:
        print("No transactions require review.")
        return
    
    print(f"Reviewing {len(review_df)} transactions")

    decisions = simulate_human_review(review_df)

    write_reviewed_labels(con, decisions)

    print("Human review completed and labels saved.")

if __name__ == "__main__":
    run_human_review()

