import os
import glob
import re
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import rasterio
from rasterio.transform import xy

# ── CONFIG START ────────────────────────────────────────────────────────────────

TIF_FOLDER   = "tiff_input/EVI"
OUT_CSV      = "csv_output/EVI.csv"
FEATURE_NAME = "evi" # keep lowercase for uniformity

# ── CONFIG END ─────────────────────────────────────────────────────────────────

def parse_date_from_filename(fname):
    """Extract a datetime.date from ‘doyYYYYDDD’ in the filename."""
    m = re.search(r'doy(\d{4})(\d{3})', fname)
    if not m:
        raise ValueError(f"Could not parse date from '{fname}'")
    year, doy = int(m.group(1)), int(m.group(2))
    return (datetime(year, 1, 1) + timedelta(days=doy-1)).date()

# convert geotiff to dataframe
def geotiff_to_dataframe(tif_path, nodata_value=None):
    """Read band-1 of tif_path and return DataFrame with columns lon, lat, value."""
    with rasterio.open(tif_path) as src:
        arr       = src.read(1)
        transform = src.transform
        h, w      = arr.shape

        rows, cols = np.indices((h, w))
        lons, lats = xy(transform, rows, cols)

        df = pd.DataFrame({
            'lon':   lons.ravel(),
            'lat':   lats.ravel(),
            'value': arr.ravel()
        })

        nod = nodata_value if nodata_value is not None else src.nodata
        if nod is not None:
            df = df[df['value'] != nod]
        return df

def main():
    # find all TIFFs
    pattern = os.path.join(TIF_FOLDER, "*.tif")
    files = sorted(glob.glob(pattern))
    if not files:
        print("No TIFFs found in", TIF_FOLDER)
        return

    # iterate over each TIFF and append conversion to output.csv
    first = True
    for tif in files:
        fname = os.path.basename(tif)
        date  = parse_date_from_filename(fname)
        print(f"Processing {fname} → date {date.isoformat()}")

        df = geotiff_to_dataframe(tif)
        df = df.rename(columns={'value': FEATURE_NAME})
        df['date'] = date.isoformat()

        df.to_csv(
            OUT_CSV,
            index=False,
            header=first,
            mode='w' if first else 'a'
        )
        first = False

    print("All done. Appended", len(files), "files to", OUT_CSV)

if __name__ == "__main__":
    main()
