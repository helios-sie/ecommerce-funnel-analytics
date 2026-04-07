"""
E-commerce Funnel & Revenue Drop Analysis
Synthetic Dataset Generator
Generates: ~50,000 users | ~300,000 events | ~20,000 orders
Intentionally includes real-world data quality issues
"""

import random
import uuid
import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from psycopg2.extras import execute_values

# ─────────────────────────────────────────────
# DATABASE CONFIG
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "database": "ecommerce_analytics",
    "user":     "postgres",
    "password": "Browny_402"
}

# ─────────────────────────────────────────────
# SEED & CONSTANTS
# ─────────────────────────────────────────────
random.seed(42)
np.random.seed(42)

NUM_USERS   = 50_000
NUM_ORDERS  = 20_000

COUNTRIES        = ["India", "USA", "UK", "Germany", "Brazil", "Canada", "Australia", "France"]
COUNTRY_WEIGHTS  = [0.30, 0.25, 0.10, 0.08, 0.08, 0.07, 0.07, 0.05]

DEVICES         = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS  = [0.60, 0.32, 0.08]           # mobile-heavy → conversion problem baked in

SOURCES         = ["google", "instagram", "direct", "email", "tiktok", "referral"]
SOURCE_WEIGHTS  = [0.35, 0.25, 0.15, 0.12, 0.08, 0.05]

FUNNEL_STEPS    = ["visit", "product_view", "add_to_cart", "checkout", "payment"]

PRODUCTS        = [f"PROD_{str(i).zfill(4)}" for i in range(1, 501)]   # 500 products

START_DATE = datetime(2024, 1, 1)
END_DATE   = datetime(2024, 12, 31)


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def random_date(start=START_DATE, end=END_DATE):
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


# ─────────────────────────────────────────────
# STEP 1 — CREATE TABLES
# ─────────────────────────────────────────────
def create_tables(conn):
    print("📦  Creating tables...")
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS orders, events, users CASCADE;")

        cur.execute("""
            CREATE TABLE users (
                user_id       VARCHAR(36) PRIMARY KEY,
                signup_date   TIMESTAMP,
                country       VARCHAR(50),
                device_type   VARCHAR(20),
                traffic_source VARCHAR(30)
            );
        """)

        cur.execute("""
            CREATE TABLE events (
                event_id    VARCHAR(36) PRIMARY KEY,
                user_id     VARCHAR(36),
                event_name  VARCHAR(30),
                timestamp   TIMESTAMP,
                product_id  VARCHAR(20)
            );
        """)

        cur.execute("""
            CREATE TABLE orders (
                order_id         VARCHAR(36) PRIMARY KEY,
                user_id          VARCHAR(36),
                order_amount     NUMERIC(10, 2),
                payment_status   VARCHAR(20),
                order_timestamp  TIMESTAMP
            );
        """)

    conn.commit()
    print("✅  Tables created.\n")


# ─────────────────────────────────────────────
# STEP 2 — GENERATE USERS
# ─────────────────────────────────────────────
def generate_users():
    print("👤  Generating users...")
    users = []
    for _ in range(NUM_USERS):
        user_id = str(uuid.uuid4())
        signup  = random_date()
        country = random.choices(COUNTRIES, COUNTRY_WEIGHTS)[0]

        # MESSY ISSUE 1: ~3% of users have NULL device_type
        device = random.choices(DEVICES, DEVICE_WEIGHTS)[0] if random.random() > 0.03 else None

        # MESSY ISSUE 2: ~2% of users have NULL traffic_source
        source = random.choices(SOURCES, SOURCE_WEIGHTS)[0] if random.random() > 0.02 else None

        users.append((user_id, signup, country, device, source))

    print(f"✅  {len(users):,} users generated.\n")
    return users


# ─────────────────────────────────────────────
# STEP 3 — GENERATE EVENTS (with messy issues)
# ─────────────────────────────────────────────
def generate_events(users):
    """
    Funnel drop-off rates (realistic, intentionally bad mobile/checkout):
      visit           → 100% of users get this
      product_view    → 65% of visitors
      add_to_cart     → 40% of product_viewers
      checkout        → 55% of cart-adders   (big drop here — intentional)
      payment         → 75% of checkout-starters (failures baked in)

    Messy issues injected:
      - Duplicate events          (~4% of events duplicated)
      - Out-of-order timestamps   (~3% of sessions)
      - Missing funnel steps      (users jump visit → checkout, skipping middle)
      - Delayed events            (timestamp gap > 7 days between steps)
      - Anomalous revenue spikes  (handled in orders)
      - Partial / abandoned sessions
    """
    print("📋  Generating events (this may take ~30–60 seconds)...")
    events      = []
    event_count = 0

    user_ids = [u[0] for u in users]

    for user_id in user_ids:
        # Each user starts with a visit timestamp anchored near their signup
        session_start = random_date()
        current_ts    = session_start

        # Decide how deep into the funnel this user goes
        roll = random.random()

        if roll < 0.35:
            # 35% — visit only (bounce)
            depth = 1
        elif roll < 0.60:
            # 25% — visit + product_view
            depth = 2
        elif roll < 0.76:
            # 16% — visit → product_view → add_to_cart
            depth = 3
        elif roll < 0.87:
            # 11% — visit → ... → checkout
            depth = 4
        else:
            # 13% — full funnel including payment
            depth = 5

        # MESSY ISSUE 3: ~2% of users skip middle steps (jump to checkout directly)
        skip_middle = random.random() < 0.02
        if skip_middle and depth >= 4:
            steps_to_generate = ["visit", "checkout"]
            if depth == 5:
                steps_to_generate.append("payment")
        else:
            steps_to_generate = FUNNEL_STEPS[:depth]

        # Build events for this user
        session_events = []
        for step in steps_to_generate:
            # Time gap between steps: 1 min to 2 hours normally
            gap_seconds = random.randint(60, 7200)

            # MESSY ISSUE 4: ~3% delayed events (multi-day gaps)
            if random.random() < 0.03:
                gap_seconds = random.randint(86400, 604800)  # 1–7 days

            current_ts = current_ts + timedelta(seconds=gap_seconds)

            product_id = random.choice(PRODUCTS) if step in ("product_view", "add_to_cart") else None

            session_events.append({
                "event_id":   str(uuid.uuid4()),
                "user_id":    user_id,
                "event_name": step,
                "timestamp":  current_ts,
                "product_id": product_id
            })

        # MESSY ISSUE 5: ~3% out-of-order timestamps (shuffle order)
        if random.random() < 0.03 and len(session_events) > 1:
            random.shuffle(session_events)

        for e in session_events:
            events.append((
                e["event_id"],
                e["user_id"],
                e["event_name"],
                e["timestamp"],
                e["product_id"]
            ))
            event_count += 1

        # MESSY ISSUE 6: ~4% duplicate events (same event, same user, same timestamp)
        if random.random() < 0.04 and session_events:
            dup = random.choice(session_events)
            events.append((
                str(uuid.uuid4()),      # new event_id so PK doesn't conflict
                dup["user_id"],
                dup["event_name"],
                dup["timestamp"],       # identical timestamp → detectable duplicate
                dup["product_id"]
            ))
            event_count += 1

    print(f"✅  {event_count:,} events generated.\n")
    return events


# ─────────────────────────────────────────────
# STEP 4 — GENERATE ORDERS (with messy issues)
# ─────────────────────────────────────────────
def generate_orders(users):
    """
    Messy issues:
      - ~15% failed payments
      - ~5% duplicate orders (same user, very close timestamp)
      - ~2% anomalous order amounts (spike outliers)
      - Some orders from users with no matching payment event
    """
    print("🛒  Generating orders...")
    orders   = []
    user_ids = [u[0] for u in users]

    # Only a subset of users place orders
    ordering_users = random.sample(user_ids, NUM_ORDERS)

    for user_id in ordering_users:
        order_id = str(uuid.uuid4())
        order_ts = random_date()

        # MESSY ISSUE 7: ~2% anomalous revenue spikes
        if random.random() < 0.02:
            amount = round(random.uniform(5000, 20000), 2)   # outlier
        else:
            # Log-normal distribution → realistic e-commerce order values
            amount = round(np.random.lognormal(mean=4.5, sigma=0.8), 2)
            amount = max(5.0, min(amount, 2000.0))            # clamp to sane range

        # MESSY ISSUE 8: ~15% failed payments
        status = "failed" if random.random() < 0.15 else "success"

        orders.append((order_id, user_id, amount, status, order_ts))

        # MESSY ISSUE 9: ~5% duplicate orders
        if random.random() < 0.05:
            dup_ts = order_ts + timedelta(seconds=random.randint(1, 30))
            orders.append((str(uuid.uuid4()), user_id, amount, status, dup_ts))

    print(f"✅  {len(orders):,} orders generated.\n")
    return orders


# ─────────────────────────────────────────────
# STEP 5 — INSERT INTO POSTGRES (batch insert)
# ─────────────────────────────────────────────
def insert_users(conn, users):
    print("⬆️   Inserting users into PostgreSQL...")
    with conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO users (user_id, signup_date, country, device_type, traffic_source) VALUES %s",
            users,
            page_size=1000
        )
    conn.commit()
    print(f"✅  {len(users):,} users inserted.\n")


def insert_events(conn, events):
    print("⬆️   Inserting events into PostgreSQL (batched)...")
    BATCH = 5000
    with conn.cursor() as cur:
        for i in range(0, len(events), BATCH):
            execute_values(
                cur,
                "INSERT INTO events (event_id, user_id, event_name, timestamp, product_id) VALUES %s",
                events[i:i+BATCH],
                page_size=1000
            )
            if (i // BATCH) % 10 == 0:
                print(f"   ... inserted {min(i+BATCH, len(events)):,} / {len(events):,}")
    conn.commit()
    print(f"✅  {len(events):,} events inserted.\n")


def insert_orders(conn, orders):
    print("⬆️   Inserting orders into PostgreSQL...")
    with conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO orders (order_id, user_id, order_amount, payment_status, order_timestamp) VALUES %s",
            orders,
            page_size=1000
        )
    conn.commit()
    print(f"✅  {len(orders):,} orders inserted.\n")


# ─────────────────────────────────────────────
# STEP 6 — VERIFY COUNTS
# ─────────────────────────────────────────────
def verify_counts(conn):
    print("🔍  Verifying row counts...")
    with conn.cursor() as cur:
        for table in ["users", "events", "orders"]:
            cur.execute(f"SELECT COUNT(*) FROM {table};")
            count = cur.fetchone()[0]
            print(f"   {table:<10}: {count:>10,} rows")
    print()


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    print("=" * 55)
    print("  E-Commerce Analytics — Synthetic Data Generator")
    print("=" * 55, "\n")

    conn = get_connection()
    print("🔗  Connected to PostgreSQL ✓\n")

    create_tables(conn)

    users  = generate_users()
    events = generate_events(users)
    orders = generate_orders(users)

    insert_users(conn, users)
    insert_events(conn, events)
    insert_orders(conn, orders)

    verify_counts(conn)

    conn.close()

    print("=" * 55)
    print("  ✅  Dataset generation complete!")
    print("  ➡️   Next step: Run SQL cleaning scripts")
    print("=" * 55)


if __name__ == "__main__":
    main()
