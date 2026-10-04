"""
Synthetic Credit Card Transaction Dataset Generator
----------------------------------------------------
Generates a realistic, interpretable transaction dataset for the
Big Data Essentials project (fraud detection case study).

Unlike anonymized Kaggle datasets (PCA features V1-V28), every column
here is human-readable, so you can do real feature engineering and
explain your findings meaningfully in the report.

Usage:
    python generate_fraud_dataset.py --target-gb 1.2 --out ../data/transactions.csv

The script writes in chunks so it never loads the whole dataset into
memory, and stops once the file reaches the target size.
"""

import argparse
import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

MERCHANT_CATEGORIES = [
    "grocery", "electronics", "travel", "gas_station", "restaurant",
    "online_retail", "jewelry", "pharmacy", "entertainment", "utilities",
    "clothing", "hotel"
]

DEVICE_TYPES = ["mobile_app", "web_browser", "pos_terminal", "atm"]

CITIES = [
    ("Kigali", -1.9441, 30.0619), ("Nairobi", -1.2921, 36.8219),
    ("Kampala", 0.3476, 32.5825), ("Lagos", 6.5244, 3.3792),
    ("Johannesburg", -26.2041, 28.0473), ("Cairo", 30.0444, 31.2357),
    ("Dubai", 25.2048, 55.2708), ("London", 51.5074, -0.1278),
    ("New York", 40.7128, -74.0060), ("Paris", 48.8566, 2.3522),
]

RNG = np.random.default_rng(42)


def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dlambda / 2) ** 2
    return 2 * r * np.arcsin(np.sqrt(a))


def generate_chunk(n_rows, n_customers, start_time, chunk_index):
    customer_id = RNG.integers(1, n_customers + 1, n_rows)

    # Each customer has a stable "home" city -> derive deterministically
    home_city_idx = customer_id % len(CITIES)
    home_lat = np.array([CITIES[i][1] for i in home_city_idx])
    home_lon = np.array([CITIES[i][2] for i in home_city_idx])

    # Transaction city: usually home, sometimes elsewhere (travel)
    away = RNG.random(n_rows) < 0.08
    txn_city_idx = np.where(away, RNG.integers(0, len(CITIES), n_rows), home_city_idx)
    txn_lat = np.array([CITIES[i][1] for i in txn_city_idx]) + RNG.normal(0, 0.05, n_rows)
    txn_lon = np.array([CITIES[i][2] for i in txn_city_idx]) + RNG.normal(0, 0.05, n_rows)
    distance_from_home_km = haversine_km(home_lat, home_lon, txn_lat, txn_lon)

    merchant_category = RNG.choice(MERCHANT_CATEGORIES, n_rows)
    device_type = RNG.choice(DEVICE_TYPES, n_rows, p=[0.45, 0.30, 0.20, 0.05])
    card_present = np.isin(device_type, ["pos_terminal", "atm"])

    # Base amount depends loosely on merchant category
    category_base = {
        "grocery": 35, "electronics": 220, "travel": 450, "gas_station": 40,
        "restaurant": 30, "online_retail": 60, "jewelry": 500, "pharmacy": 25,
        "entertainment": 45, "utilities": 70, "clothing": 55, "hotel": 300,
    }
    base = np.array([category_base[c] for c in merchant_category])
    amount = np.round(np.abs(RNG.normal(base, base * 0.6)) + 1, 2)

    customer_age = RNG.integers(18, 75, n_rows)
    customer_tenure_days = RNG.integers(1, 3650, n_rows)
    avg_txn_amount_last_30d = np.round(np.abs(RNG.normal(base, base * 0.3)), 2)
    transactions_last_1h = RNG.poisson(0.3, n_rows)

    seconds_offset = np.sort(RNG.integers(0, 60 * 60 * 24 * 90, n_rows))  # spread over ~90 days
    timestamps = [start_time + timedelta(seconds=int(s)) for s in seconds_offset]

    # ---- Fraud scoring: combine risk factors into a probability ----
    # v2 (Sep 2026): stronger, more realistic signal. In v1 almost every row had a
    # ~5% fraud chance regardless of its features, so fraud was mostly random noise
    # and even a perfect model could only reach AUC ~0.61. Now a normal transaction
    # has a very low fraud chance, and each red flag raises it sharply:
    #   - 1 strong red flag alone            -> roughly 3-5% fraud chance
    #   - 2 strong red flags together        -> roughly 50%+
    #   - 2-3 flags + high-risk merchant     -> 80-95%
    # Overall fraud rate ~0.9%, close to real-world card fraud levels.
    risk = (
        3.5 * (amount > 3 * avg_txn_amount_last_30d).astype(float)    # spending spike
        + 3.5 * (distance_from_home_km > 500).astype(float)           # far from home
        + 2.8 * (transactions_last_1h >= 3).astype(float)             # burst of transactions
        + 1.2 * (merchant_category == "jewelry").astype(float)        # easy-to-resell goods
        + 0.8 * (merchant_category == "electronics").astype(float)
        + 0.6 * (~card_present).astype(float)                         # card not physically used
    )
    fraud_prob = 1 / (1 + np.exp(-(risk - 7.0)))  # logistic squashing, centre at 7
    fraud_prob = np.clip(fraud_prob, 0.0002, 0.97)
    is_fraud = (RNG.random(n_rows) < fraud_prob).astype(int)

    df = pd.DataFrame({
        "transaction_id": np.arange(chunk_index, chunk_index + n_rows),
        "customer_id": customer_id,
        "timestamp": timestamps,
        "amount": amount,
        "merchant_category": merchant_category,
        "device_type": device_type,
        "card_present": card_present,
        "customer_age": customer_age,
        "customer_tenure_days": customer_tenure_days,
        "avg_txn_amount_last_30d": avg_txn_amount_last_30d,
        "transactions_last_1h": transactions_last_1h,
        "distance_from_home_km": np.round(distance_from_home_km, 2),
        "txn_city": [CITIES[i][0] for i in txn_city_idx],
        "is_fraud": is_fraud,
    })
    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-gb", type=float, default=1.2, help="Target output size in GB")
    parser.add_argument("--out", type=str, default="../data/transactions.csv")
    parser.add_argument("--n-customers", type=int, default=50000)
    parser.add_argument("--chunk-rows", type=int, default=500_000)
    args = parser.parse_args()

    target_bytes = args.target_gb * (1024 ** 3)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    # The script writes in "append" mode chunk by chunk, so an old file must be
    # removed first -- otherwise new rows would be added on top of the old dataset.
    if os.path.exists(args.out):
        print(f"Removing existing file: {args.out}")
        os.remove(args.out)

    start_time = datetime(2026, 1, 1)
    chunk_index = 0
    first_chunk = True

    while True:
        df = generate_chunk(args.chunk_rows, args.n_customers, start_time, chunk_index)
        df.to_csv(args.out, mode="a", header=first_chunk, index=False)
        first_chunk = False
        chunk_index += args.chunk_rows

        current_size = os.path.getsize(args.out)
        print(f"Rows written: {chunk_index:,} | File size: {current_size / (1024**3):.3f} GB")

        if current_size >= target_bytes:
            break

    print(f"\nDone. Final file: {args.out} ({os.path.getsize(args.out) / (1024**3):.3f} GB)")
    print("Expected fraud rate: ~0.9% (v2 fraud formula).")


if __name__ == "__main__":
    main()
