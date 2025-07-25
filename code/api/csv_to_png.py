import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# 1) Load & pivot
df   = pd.read_csv("output.csv")
lons = np.sort(df["lon"].unique())
lats = np.sort(df["lat"].unique())
grid = (
    df
    .pivot(index="lat", columns="lon", values="value")
    .reindex(index=lats[::-1], columns=lons)
    .values
)

# 2) Plot & save
plt.imshow(grid, origin="upper", aspect="equal")
plt.axis("off")
plt.colorbar(label="Value")
plt.savefig("visualization.png", dpi=300, bbox_inches="tight")