import streamlit as st
import pandas as pd

from src.database.connection import get_connection

st.set_page_config(page_title="Cashflow Copilot", layout="wide")

st.title("💸 Cashflow Copilot - Review Inbox")

# Connect to DB
con = get_connection()

# Load review items
df = con.execute("""
    SELECT
        transaction_id,
        date,
        description,
        merchant_key,
        suggested_category,
        source,
        route
    FROM classification_inbox
    WHERE route = 'review'
    LIMIT 100
""").df()

st.write(f"Transactions needing review: {len(df)}")

if df.empty:
    st.success("No transactions to review 🎉")
    st.stop()

# Category options
categories = ["software", "travel", "rent", "marketing", "meals", "equipment"]

# Create editable table
edited_rows = []

for i, row in df.iterrows():

    st.write("---")

    col1, col2, col3 = st.columns([3, 2, 2])

    with col1:
        st.write(f"**{row['description']}**")
        st.caption(f"Merchant: {row['merchant_key']}")

    with col2:
        st.write(f"Suggested: `{row['suggested_category']}`")
        st.caption(f"Source: {row['source']}")

    with col3:
        selected_category = st.selectbox(
            "Final Category",
            categories,
            index=categories.index(row["suggested_category"])
            if row["suggested_category"] in categories else 0,
            key=row["transaction_id"]
        )

        if st.button("Confirm", key=f"btn_{row['transaction_id']}"):

            edited_rows.append({
                "transaction_id": row["transaction_id"],
                "category": selected_category
            })

# Save updates
if edited_rows:

    update_df = pd.DataFrame(edited_rows)

    con.register("updates_df", update_df)

    con.execute("""
    INSERT INTO transaction_labels (
        transaction_id,
        category,
        label_source,
        labeled_at
    )
    SELECT
        transaction_id,
        category,
        'human_review' AS label_source,
        CURRENT_TIMESTAMP
    FROM updates_df
""")

    st.success("Saved! Re-run pipeline to update system.")