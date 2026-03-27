"""Pipeline configuration: paths, schema, and quality thresholds."""
from pathlib import Path

# ── Root paths ─────────────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"

RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
DB_DIR = DATA_DIR / "db"
REPORTS_DIR = ROOT_DIR / "reports"
LOGS_DIR = ROOT_DIR / "logs"

# ── Source file ────────────────────────────────────────────────────────────────
RAW_FILE = RAW_DIR / "daily_ecommerce_orders.csv"

# ── Output files ───────────────────────────────────────────────────────────────
PROCESSED_FILE = PROCESSED_DIR / "orders_cleaned.csv"
SUMMARY_FILE = PROCESSED_DIR / "orders_summary.csv"
CATEGORY_SUMMARY_FILE = PROCESSED_DIR / "orders_by_category.csv"
PAYMENT_SUMMARY_FILE = PROCESSED_DIR / "orders_by_payment.csv"
DB_FILE = DB_DIR / "ecommerce.db"

# ── Schema definition ──────────────────────────────────────────────────────────
EXPECTED_COLUMNS = [
    "order_id",
    "order_date",
    "customer_age",
    "product_category",
    "order_value",
    "discount_applied",
    "payment_method",
    "delivery_time_days",
    "customer_rating",
    "order_status",
]

COLUMN_DTYPES = {
    "order_id": "int64",
    "customer_age": "int64",
    "order_value": "float64",
    "delivery_time_days": "int64",
    "customer_rating": "float64",
}

# ── Valid categorical values ───────────────────────────────────────────────────
VALID_ORDER_STATUSES = {"Delivered", "Cancelled", "Returned", "Pending"}
VALID_PAYMENT_METHODS = {"Card", "COD", "Wallet", "UPI", "Net Banking"}
VALID_DISCOUNT_VALUES = {"Yes", "No"}

# ── Business rules / quality thresholds ───────────────────────────────────────
MIN_ORDER_VALUE = 0.0
MAX_ORDER_VALUE = 1_000_000.0
MIN_CUSTOMER_AGE = 18
MAX_CUSTOMER_AGE = 100
MIN_DELIVERY_DAYS = 1
MAX_DELIVERY_DAYS = 60
MIN_RATING = 1.0
MAX_RATING = 5.0

# Maximum acceptable % of nulls per critical column before pipeline aborts
NULL_THRESHOLD_PCT = 0.10  # 10 %

# ── Database table names ───────────────────────────────────────────────────────
DB_TABLE_ORDERS = "orders"
DB_TABLE_CATEGORY_SUMMARY = "category_summary"
DB_TABLE_PAYMENT_SUMMARY = "payment_summary"
DB_TABLE_RUN_LOG = "pipeline_run_log"
