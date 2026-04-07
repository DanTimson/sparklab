"""
generate_dataset.py
───────────────────
Generates a synthetic e-commerce transactions dataset.

Schema (8 features, 3 types):
  user_id        int       unique customer identifier
  product_id     int       product SKU
  quantity       int       units ordered (1–20)
  price          float     unit price (USD)
  discount       float     discount fraction (0.0–0.5)
  category       string    product category  ← categorical
  payment_method string    payment type      ← categorical
  region         string    sales region      ← categorical

Output: data/transactions.csv  (~100 000 rows, ~12 MB)
"""

import numpy as np
import pandas as pd
from pathlib import Path

SEED       = 42
N_ROWS     = 120_000
OUTPUT     = Path(__file__).parent / "transactions.csv"

rng = np.random.default_rng(SEED)

CATEGORIES      = ["Electronics", "Clothing", "Books", "Home & Garden",
                   "Sports", "Toys", "Food & Beverage", "Health"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "crypto", "bank_transfer"]
REGIONS         = ["North America", "Europe", "Asia", "South America", "Africa", "Oceania"]

# Price ranges per category (min, max)
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
prices = np.array([
    rng.uniform(*PRICE_RANGE[c]) for c in categories
])

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
print(df.dtypes)
print(df.head(3))