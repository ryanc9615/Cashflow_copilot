import re
import pandas as pd

def normalise_transaction_text(text: str) -> str:
    """
    Normalise transaction description text for rules and ML. 

    Steps:
    - handle nulls
    - lowercase
    - replace punctuation / non-letters / non-digits with spaces
    - optionally remove digits
    - collapse repeated whitespace
    - strip leading/trailing spaces
    """

    if pd.isna(text):
        return ""
    
    text = str(text).lower()

    # Replace anything not a letter, digit, or space with space 
    text = re.sub(r"[^a-z0-9\s]", " ", text)

    # Remove standalone numbers
    text = re.sub(r"\b\d+\b"," ", text)

    # Collapse multiple spaces into one 
    text = re.sub(r"\s+", " ", text)

    return text.strip()