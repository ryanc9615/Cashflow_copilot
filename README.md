# Cashflow Copilot

## System Architecture

1. Data generation and ingestion
2. Data storage with DuckDB
3. Feature Engineering and text preprocessing
4. Rule engine for deterministic logic
5. Label layer for simulated supervision
6. Merchant Memory for learned merchant behaviour
7. Training dataset builder
8. ML Model using TF-IDF and Logistic Regression
9. Prediction with confidence-based routing
10. Classification inbox as the decision layer
11. Evaluation layer
12. UI layer


## Strengths

# Hybrid classification architecture
Combines deterministic rules, merchant memory and machine learning rather than relying on a single approach. Improves system robustness and better reflects how real-world financial classfication systems are often designed

# Clear end-to-end pipeline
Demonstrates the flow from raw transaction generation through preprocessing, rule-based classfication, learned merchant memory, model training, prediction, routing, and final decisioning. 

# Strong learning value
Clearly shows how TF-IDF and Logistic Regression can be used as practical tools for transaction text classfication. 

# Merchant memory layer
Introduces a stateful learning component that can improve classification of repeated merchants without requiring full model retraining. 

# Explaninable decision structure
Architecture makes it easier to understand where a classfication came from: merchant memory, rule engine, or ML model. 

# Good foundation for extention
Modular structure makes it easier to improve individual layers over time, such as replacing simulated labels with real reviewed lables or expanding the decision logic. 


## Limitations and Areas for Improvement

# Single-label assumption
Current design assumes one transaction maps to one category label. In practice, the schema should be strengthened with a primary key, unique constraint, and potentially versioning logic to handle relabelling and label history. 

# Synthetic labels instead of real human supervision
Current label layer is simulated from synthetic categories, wheich is useful for development but does not reflect the ambiguity, inconsistency, and noise of real-world human-labelled data. A stronger version would use real reviewed examples and human-in-the-loop workflow. 

# Limited and non-random label sample
Current 500-row transaction label sample is small and not designed as a representative sample. This can distort downstream training and evaluation. A better approach would use random or stratified sampling. 

# Simple merchant memory logic
Merchant memory layer is easy to understand, fast to compute, and easy to debug, currently lacks richer logic such as conflict handling, recency weighting, confidence modelling, and stronger rules for when memory should be trusted. 

# Text-only ML limitations
ML model may struggle when categories use very similar language, when descriptions are ambiguoius, when non-text context is needed, or when some classes are rare and underrepresented. Future versions could incorporate additional features such as merchant identity, transaction amount, recurrence, and temporal context. 

# Decision-layer traust logic is still basic 
Current decision logic is largely hierarchy-based and may treat source existence as equivalent to source strength. A more mature system would evaluate confidence, conflict, and evidence strength more explicitly. 

# Evaluation still needs to test calibration and real-world readiness
Model confidence scores are useful for routing, but they are not automatically equivalent to true trustworthiness. Future evaluation should include calibration analysis and more realistic test conditions. 

