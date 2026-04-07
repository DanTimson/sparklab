"""
Generates a synthetic e-commerce transactions dataset.

Usage:
  python generate_dataset.py [N_ROWS]   (default: 120_000)
  python generate_dataset.py 1000000
"""

import sys
import numpy as np
import pandas as pd
from pathlib import Path

SEED   = 42
N_ROWS = int(sys.argv[1]) if len(sys.argv) > 1 else 120_000
OUTPUT = Path(__file__).parent / "transactions.csv"

rng = np.random.default_rng(SEED)

CATEGORIES      = ["Electronics", "Clothing", "Books", "Home & Garden",
                   "Sports", "Toys", "Food & Beverage", "Health"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "crypto", "bank_transfer"]
REGIONS         = ["North America", "Europe", "Asia", "South America", "Africa", "Oceania"]

PRICE_RANGE = {
    "Electronics":    (50.0,  2000.0),
    "Clothing":       (10.0,   300.0),
    "Books":          (5.0,     80.0),
    "Home & Garden":  (15.0,   500.0),
    "Sports":         (20.0,   800.0),
    "Toys":           (8.0,    150.0),
    "Food & Beverage":(2.0,    60.0),
    "Health":         (5.0,    250.0),
}

print(f"Generating {N_ROWS:,} rows …")

categories = rng.choice(CATEGORIES, size=N_ROWS)
prices = np.array([rng.uniform(*PRICE_RANGE[c]) for c in categories])

df = pd.DataFrame({
    "user_id":        rng.integers(1, 50_001, size=N_ROWS),
    "product_id":     rng.integers(1,  5_001, size=N_ROWS),
    "quantity":       rng.integers(1,     21, size=N_ROWS),
    "price":          np.round(prices, 2),
    "discount":       np.round(rng.uniform(0.0, 0.5, size=N_ROWS), 3),
    "category":       categories,
    "payment_method": rng.choice(PAYMENT_METHODS, size=N_ROWS),
    "region":         rng.choice(REGIONS, size=N_ROWS),
})

OUTPUT.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUTPUT, index=False)

size_mb = OUTPUT.stat().st_size / 1_048_576
print(f"Saved → {OUTPUT}  ({N_ROWS:,} rows, {size_mb:.1f} MB)")