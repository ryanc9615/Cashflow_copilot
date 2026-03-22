from src.database.connection import get_connection


def classification_summary(con):
    """
    Overall system routing breakdown.
    """
    return con.execute("""
        SELECT
            source,
            route,
            COUNT(*) AS count
        FROM classification_inbox
        GROUP BY source, route
        ORDER BY count DESC
    """).df()


def automation_rate(con):
    """
    % of transactions auto-classified.
    """
    return con.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN route = 'auto' THEN 1 ELSE 0 END) AS auto_count,
            ROUND(
                SUM(CASE WHEN route = 'auto' THEN 1 ELSE 0 END) * 1.0 / COUNT(*),
                3
            ) AS automation_rate
        FROM classification_inbox
    """).df()


def review_rate(con):
    """
    % of transactions sent to review.
    """
    return con.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN route = 'review' THEN 1 ELSE 0 END) AS review_count,
            ROUND(
                SUM(CASE WHEN route = 'review' THEN 1 ELSE 0 END) * 1.0 / COUNT(*),
                3
            ) AS review_rate
        FROM classification_inbox
    """).df()


def model_accuracy_on_labelled(con):
    """
    Accuracy of model predictions vs known labels.
    Only evaluates rows that have a true label.
    """
    return con.execute("""
        SELECT
            COUNT(*) AS total_labelled,
            SUM(
                CASE 
                    WHEN mp.predicted_category = t.category THEN 1 
                    ELSE 0 
                END
            ) AS correct,
            ROUND(
                SUM(
                    CASE 
                        WHEN mp.predicted_category = t.category THEN 1 
                        ELSE 0 
                    END
                ) * 1.0 / COUNT(*),
                3
            ) AS accuracy
        FROM model_predictions mp
        JOIN transactions t
            ON mp.transaction_id = t.transaction_id
    """).df()


def confidence_distribution(con):
    """
    See how confident the model is.
    """
    return con.execute("""
        SELECT
            CASE
                WHEN confidence >= 0.9 THEN '0.9+'
                WHEN confidence >= 0.75 THEN '0.75-0.9'
                WHEN confidence >= 0.6 THEN '0.6-0.75'
                ELSE '<0.6'
            END AS confidence_bucket,
            COUNT(*) AS count
        FROM model_predictions
        GROUP BY confidence_bucket
        ORDER BY confidence_bucket DESC
    """).df()


def accuracy_by_confidence(con):
    """
    Key insight:
    How accuracy changes with confidence
    """
    return con.execute("""
        SELECT
            CASE
                WHEN mp.confidence >= 0.9 THEN '0.9+'
                WHEN mp.confidence >= 0.75 THEN '0.75-0.9'
                WHEN mp.confidence >= 0.6 THEN '0.6-0.75'
                ELSE '<0.6'
            END AS confidence_bucket,

            COUNT(*) AS total,

            SUM(
                CASE 
                    WHEN mp.predicted_category = t.category THEN 1 
                    ELSE 0 
                END
            ) AS correct,

            ROUND(
                SUM(
                    CASE 
                        WHEN mp.predicted_category = t.category THEN 1 
                        ELSE 0 
                    END
                ) * 1.0 / COUNT(*),
                3
            ) AS accuracy

        FROM model_predictions mp
        JOIN transactions t
            ON mp.transaction_id = t.transaction_id

        GROUP BY confidence_bucket
        ORDER BY confidence_bucket DESC
    """).df()


def main():

    con = get_connection()

    print("\n=============================")
    print(" SYSTEM EVALUATION")
    print("=============================\n")

    print("1. Classification Summary:")
    print(classification_summary(con))

    print("\n2. Automation Rate:")
    print(automation_rate(con))

    print("\n3. Review Rate:")
    print(review_rate(con))

    print("\n4. Model Accuracy (labelled data):")
    print(model_accuracy_on_labelled(con))

    print("\n5. Confidence Distribution:")
    print(confidence_distribution(con))

    print("\n6. Accuracy by Confidence:")
    print(accuracy_by_confidence(con))


if __name__ == "__main__":
    main()