import pandas as pd
from pathlib import Path

FILES = [
    "IOS_Tenant_Targets.csv",
    "IOS_Tenant_Targets_Wth_Coords.csv",
]

COLUMNS_TO_DROP = {
    "Ownership",
    "Contact",
    "Phone",
    "Email",
    "Notes",
}


def clean_csv(path: Path):
    print(f"Cleaning {path.name}...")

    df = pd.read_csv(path, encoding="latin1")

    # Drop columns only if they exist (safe for both files)
    existing = [c for c in COLUMNS_TO_DROP if c in df.columns]
    df = df.drop(columns=existing)

    output_path = path.with_stem(f"{path.stem}_cleaned")
    df.to_csv(output_path, index=False)

    print(f" → Wrote {output_path.name} ({len(existing)} columns removed)")


def main():
    for file in FILES:
        path = Path(file)
        if not path.exists():
            print(f"⚠️  Skipping {file} (not found)")
            continue
        clean_csv(path)


if __name__ == "__main__":
    main()
