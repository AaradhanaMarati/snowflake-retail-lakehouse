"""
Tests that run in CI without Snowflake:
  1. Sample data contract (schemas, keys, planted dirty records exist)
  2. Pipeline logic via the DuckDB preview (dedupe, SCD2, point-in-time join)
  3. Static checks on the Snowflake SQL files
"""
import csv
import re
import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
SQL = ROOT / "sql"
sys.path.insert(0, str(ROOT / "scripts"))
import local_preview  # noqa: E402


def read(name):
    with (RAW / name).open() as f:
        return list(csv.DictReader(f))


# ---------------------------------------------------------------- data contract
EXPECTED_HEADERS = {
    "orders.csv": ["order_id", "customer_id", "order_ts", "channel", "status", "ship_country"],
    "order_items.csv": ["order_line_id", "order_id", "product_id", "quantity",
                        "unit_price", "discount_pct"],
    "products.csv": ["product_id", "product_name", "category", "list_price",
                     "unit_cost", "is_active"],
}


@pytest.mark.parametrize("fname,header", EXPECTED_HEADERS.items())
def test_headers(fname, header):
    with (RAW / fname).open() as f:
        assert next(csv.reader(f)) == header


def test_order_ids_unique():
    ids = [r["order_id"] for r in read("orders.csv")]
    assert len(ids) == len(set(ids))


def test_dirty_records_planted():
    items = read("order_items.csv")
    ids = [r["order_line_id"] for r in items]
    assert len(ids) > len(set(ids)), "expected duplicate lines for dedupe demo"
    assert any(int(r["quantity"]) <= 0 for r in items), "expected invalid quantities"


def test_referential_integrity_raw():
    product_ids = {r["product_id"] for r in read("products.csv")}
    order_ids = {r["order_id"] for r in read("orders.csv")}
    for r in read("order_items.csv"):
        assert r["product_id"] in product_ids
        assert r["order_id"] in order_ids


# ---------------------------------------------------------------- pipeline logic
@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    local_preview.build(c)
    return c


def test_dedupe(con):
    dupes = con.sql("SELECT count(*) FROM (SELECT order_line_id FROM stg_order_items "
                    "GROUP BY 1 HAVING count(*) > 1)").fetchall()[0][0]
    assert dupes == 0


def test_invalid_lines_excluded_from_fact(con):
    assert con.execute("SELECT count(*) FROM fct_sales WHERE quantity <= 0").fetchall()[0][0] == 0


def test_scd2_one_current_row_per_customer(con):
    bad = con.sql("SELECT count(*) FROM (SELECT customer_id FROM dim_customer "
                  "WHERE valid_to IS NULL GROUP BY 1 HAVING count(*) <> 1)").fetchall()[0][0]
    assert bad == 0


def test_scd2_history_exists(con):
    n = con.sql("SELECT count(*) FROM (SELECT customer_id FROM dim_customer "
                "GROUP BY 1 HAVING count(*) > 1)").fetchall()[0][0]
    assert n > 0


def test_point_in_time_join_resolves_all_customers(con):
    assert con.execute("SELECT count(*) FROM fct_sales WHERE customer_key = -1").fetchall()[0][0] == 0


def test_revenue_reconciles(con):
    stg = con.execute("SELECT round(sum(net_amount), 2) FROM stg_order_items WHERE is_valid").fetchall()[0][0]
    fct = con.execute("SELECT round(sum(net_amount), 2) FROM fct_sales").fetchall()[0][0]
    assert stg == fct


# ---------------------------------------------------------------- SQL static checks
SQL_FILES = sorted(SQL.rglob("*.sql"))


def test_sql_files_exist():
    assert len(SQL_FILES) >= 10


@pytest.mark.parametrize("path", SQL_FILES, ids=lambda p: str(p.relative_to(SQL)))
def test_sql_balanced(path):
    text = path.read_text()
    no_comments = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    no_comments = re.sub(r"--[^\n]*", "", no_comments)
    no_strings = re.sub(r"'(?:[^']|'')*'", "''", no_comments)
    assert no_strings.count("(") == no_strings.count(")"), "unbalanced parentheses"
    assert text.count("$$") % 2 == 0, "unbalanced $$ procedure body"


@pytest.mark.parametrize("path", SQL_FILES, ids=lambda p: str(p.relative_to(SQL)))
def test_no_hardcoded_secrets(path):
    text = path.read_text().upper()
    assert "AWS_SECRET_KEY" not in text and "AWS_KEY_ID" not in text
