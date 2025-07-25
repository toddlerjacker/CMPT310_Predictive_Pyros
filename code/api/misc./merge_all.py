import pandas as pd
import os

# === User Configuration ===
folder_path = "features"  # Change to your actual folder
output_path = "nasa_dataset.csv"

# Get and sort all CSV files
csv_files = sorted([f for f in os.listdir(folder_path) if f.endswith('.csv')])
merged_df = None

# Metadata fields to coalesce
meta_cols = ['STATION_NAME', 'lat', 'lon', 'ELEVATION_M', 'DANGER_RATING']
meta_trackers = {col: [] for col in meta_cols}  # Keep renamed versions for coalescing

for i, file in enumerate(csv_files):
    file_path = os.path.join(folder_path, file)
    df = pd.read_csv(file_path)

    # Drop all distance-related columns
    distance_cols = [col for col in df.columns if 'distance' in col.lower()]
    df = df.drop(columns=distance_cols, errors='ignore')

    # Rename metadata columns so they don't conflict on merge
    for col in meta_cols:
        if col in df.columns:
            new_col = f"{col}_{i}"
            df.rename(columns={col: new_col}, inplace=True)
            meta_trackers[col].append(new_col)

    if i == 0:
        merged_df = df
    else:
        # Do NOT drop renamed metadata — we will coalesce later
        merged_df = pd.merge(
            merged_df,
            df,
            on=['DATE_TIME', 'STATION_CODE'],
            how='outer'
        )

# Coalesce each metadata field into one clean column
for col, variants in meta_trackers.items():
    existing = [v for v in variants if v in merged_df.columns]
    if existing:
        merged_df[col] = merged_df[existing].bfill(axis=1).iloc[:, 0]
        merged_df.drop(columns=[v for v in existing if v != col], inplace=True)

# Save final merged output
merged_df.to_csv(output_path, index=False)
print(f"Merged CSV saved to: {output_path}")
