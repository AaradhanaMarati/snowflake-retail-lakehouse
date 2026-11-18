"""
Generate deterministic, realistic sample data for the retail lakehouse.

The data intentionally includes a few "dirty" records (duplicate order lines,
null emails, negative quantities, mixed-case country codes) so the
STAGING layer and data quality checks have something real to catch.

Usage:
    python scripts/generate_sample_data.py --out data/raw
"""
import argparse
import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

SEED = 42
FIRST = ["Ava", "Liam", "Noah", "Emma", "Olivia", "Mia", "Lucas", "Arjun", "Priya",
         "Sofia", "Mateo", "Chloe", "Ethan", "Zara", "Ravi", "Hana", "Omar", "Grace"]
LAST = ["Smith", "Patel", "Garcia", "Kim", "Nguyen", "Brown", "Reddy", "Lopez",
        "Chen", "Wilson", "Khan", "Martin", "Rossi", "Singh"]
COUNTRIES = ["US", "us", "CA", "GB", "IN", "DE", "AU"]
SEGMENTS = ["CONSUMER", "SMALL_BUSINESS", "ENTERPRISE"]
CATEGORIES = {
    "Electronics": [("Wireless Earbuds", 59.99), ("4K Monitor 27in", 329.00),
                    ("USB-C Hub", 39.50), ("Mechanical Keyboard", 119.00),
                    ("Webcam 1080p", 74.99)],
    "Home": [("Air Purifier", 189.00), ("Smart Bulb 4-pack", 44.99),
             ("Robot Vacuum", 279.00), ("Coffee Grinder", 69.00)],
    "Outdoors": [("Hiking Backpack 40L", 129.00), ("Trekking Poles", 49.00),
                 ("Insulated Bottle", 29.99), ("Camping Lantern", 34.50)],
    "Apparel": [("Rain Jacket", 99.00), ("Running Shoes", 139.00),
                ("Merino T-Shirt", 54.00)],
}
CHANNELS = ["WEB", "MOBILE_APP", "MARKETPLACE"]
STATUSES = ["COMPLETED"] * 8 + ["RETURNED", "CANCELLED"]


def write_csv(path: Path, header, rows):
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"wrote {len(rows):>5} rows -> {path}")


def main(out: Path):
    rng = random.Random(SEED)
    out.mkdir(parents=True, exist_ok=True)
    start = date(2025, 1, 1)

    # ---- customers ---------------------------------------------------------
    customers = []
    for cid in range(1, 201):
        fn, ln = rng.choice(FIRST), rng.choice(LAST)
        email = f"{fn}.{ln}{cid}@example.com".lower() if rng.random() > 0.03 else ""
        signup = start + timedelta(days=rng.randint(0, 300))
        customers.append([cid, fn, ln, email, rng.choice(COUNTRIES),
                          rng.choice(SEGMENTS), signup.isoformat(),
                          datetime(2025, 11, 1, 8, 0).isoformat()])
    # SCD2 demo: a later snapshot where some customers changed segment/country
    customer_updates = []
    for c in rng.sample(customers, 15):
        upd = list(c)
        upd[5] = rng.choice([s for s in SEGMENTS if s != c[5]])
        upd[7] = datetime(2025, 11, 15, 9, 30).isoformat()
        customer_updates.append(upd)

    cust_header = ["customer_id", "first_name", "last_name", "email", "country_code",
                   "segment", "signup_date", "updated_at"]
    write_csv(out / "customers_2025_11_01.csv", cust_header, customers)
    write_csv(out / "customers_2025_11_15.csv", cust_header, customer_updates)

    # ---- products ----------------------------------------------------------
    products = []
    pid = 1000
    for cat, items in CATEGORIES.items():
        for name, price in items:
            pid += 1
            products.append([pid, name, cat, f"{price:.2f}", round(price * 0.55, 2), True])
    write_csv(out / "products.csv",
              ["product_id", "product_name", "category", "list_price", "unit_cost", "is_active"],
              products)

    # ---- orders + order_items ---------------------------------------------
    orders, items = [], []
    line_id = 0
    for oid in range(50001, 51501):
        cust = rng.choice(customers)
        ts = datetime(2025, 9, 1) + timedelta(minutes=rng.randint(0, 60 * 24 * 76))
        orders.append([oid, cust[0], ts.isoformat(), rng.choice(CHANNELS),
                       rng.choice(STATUSES), cust[4].upper()])
        for _ in range(rng.randint(1, 4)):
            line_id += 1
            p = rng.choice(products)
            qty = rng.randint(1, 3)
            if rng.random() < 0.005:
                qty = -1  # bad record for DQ checks
            discount = rng.choice([0, 0, 0, 0.05, 0.10, 0.15])
            row = [line_id, oid, p[0], qty, p[3], discount]
            items.append(row)
            if rng.random() < 0.01:
                items.append(list(row))  # duplicate line for dedupe logic

    write_csv(out / "orders.csv",
              ["order_id", "customer_id", "order_ts", "channel", "status", "ship_country"],
              orders)
    write_csv(out / "order_items.csv",
              ["order_line_id", "order_id", "product_id", "quantity", "unit_price",
               "discount_pct"],
              items)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/raw", type=Path)
    main(ap.parse_args().out)
