import pandas as pd
import re
from pathlib import Path

CSV_PATH = "IOS_Tenant_Targets_cleaned.csv"


def normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s).lower().strip()
    s = re.sub(r"[^\w\s]", "", s)   # remove punctuation
    s = re.sub(r"\s+", " ", s)      # collapse whitespace
    return s


def load_with_real_header(path: Path) -> pd.DataFrame:
    """
    Your file has a junk first row like: Unnamed: 0,Unnamed: 1,...
    and the real header is on the next row.
    This loads using the 2nd row as the header.
    """
    # Read first line to detect the junk header
    first_line = path.read_text(encoding="latin1", errors="ignore").splitlines()[0]
    if first_line.lower().startswith("unnamed: 0"):
        # Use the second row as header
        return pd.read_csv(path, encoding="latin1", header=1)
    else:
        return pd.read_csv(path, encoding="latin1")


def main():
    path = Path(CSV_PATH)
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")

    df = load_with_real_header(path)

    # Trim header whitespace just in case
    df.columns = [c.strip() for c in df.columns]

    required = ["Address", "State"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print("Available columns:", list(df.columns))
        raise ValueError(f"Missing required columns: {missing}")

    # Dedupe on Address + State only
    df["_dedupe_key"] = (
        df["Address"].apply(normalize_text) + "|" +
        df["State"].apply(normalize_text)
    )

    before = len(df)
    df = df.drop_duplicates(subset="_dedupe_key", keep="first").drop(columns="_dedupe_key")
    after = len(df)

    # Overwrite same file (no extra junk header row gets re-written)
    df.to_csv(path, index=False)

    print("Deduplication complete.")
    print(f"Rows before: {before}")
    print(f"Rows after:  {after}")
    print(f"Removed:     {before - after}")
    print(f"File updated in place: {path}")


if __name__ == "__main__":
    main()
