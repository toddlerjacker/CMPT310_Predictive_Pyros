import pandas as pd
from pathlib import Path

# ─── CONFIG ───────────────────────────────────────────────────────────────────
# adjust this if you move the script elsewhere
BASE_DIR = Path(__file__).resolve().parents[2]

GT_PATH  = BASE_DIR / "data" / "raw" / "Wildfire" / "GroundTruth" / "2023_BCWS_WX_OBS.csv"
# ────────────────────────────────────────────────────────────────────────────────

def load_groundtruth(path: Path) -> pd.DataFrame:
    """Load the BCWS weather‐observation ground truth CSV."""
    if not path.exists():
        raise FileNotFoundError(f"Cannot find ground truth CSV at {path}")
    # parse DATE_TIME into actual datetime objects
    df = pd.read_csv(path, parse_dates=["DATE_TIME"], infer_datetime_format=True)
    return df

def evaluate(df: pd.DataFrame) -> None:
    """Prints out basic diagnostics on the loaded DataFrame."""
    print("\n--- DataFrame Info ---")
    df.info(memory_usage="deep")
    
    print("\n--- First 5 Rows ---")
    print(df.head(), "\n")
    
    print("--- Summary Statistics ---")
    print(df.describe(include="all").T, "\n")
    
    print("--- Missing Values by Column ---")
    print(df.isna().sum(), "\n")
    
    # If you have a station code column:
    if "STATION_CODE" in df.columns:
        print(f"Unique stations: {df['STATION_CODE'].nunique()}\n")
    
    # Date range
    if "DATE_TIME" in df.columns:
        dr = df["DATE_TIME"].agg(["min","max"])
        print(f"Date range: {dr['min']} → {dr['max']}\n")

def main():
    df = load_groundtruth(GT_PATH)
    evaluate(df)

if __name__ == "__main__":
    main()