import os
import time
import logging
import threading
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from twelvedata import TDClient

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

MAX_BATCH_SIZE = 20
FLUSH_INTERVAL = 10
HEARTBEAT_INTERVAL = 10

# AAPL is silent on weekends (market closed); BTC/USD streams 24/7.
SYMBOLS = ["BTC/USD", "AAPL"]


def get_connection():
    required = ["TIGER_HOST", "TIGER_PORT", "TIGER_USER", "TIGER_PASSWORD",
                "TIGER_DATABASE", "TWELVEDATA_API_KEY"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise ValueError("Missing environment variables in .env: " + ", ".join(missing))

    conn = psycopg2.connect(
        host=os.getenv("TIGER_HOST"),
        port=int(os.getenv("TIGER_PORT")),
        user=os.getenv("TIGER_USER"),
        password=os.getenv("TIGER_PASSWORD"),
        dbname=os.getenv("TIGER_DATABASE"),
        connect_timeout=15,
        sslmode="require",
    )
    print("Connected to Tiger Cloud!")
    return conn


def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stocks_real_time (
                time TIMESTAMPTZ NOT NULL,
                symbol TEXT NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                day_volume DOUBLE PRECISION
            );
        """)
        cur.execute("""
            SELECT create_hypertable('stocks_real_time', 'time', if_not_exists => TRUE);
        """)
    conn.commit()
    print("Financial data hypertable is ready!")


class WebsocketPipeline:
    def __init__(self, conn):
        self.conn = conn
        self.current_batch = []
        self.insert_counter = 0
        self.lock = threading.Lock()

    def insert_batch(self):
        with self.lock:
            if not self.current_batch:
                return
            batch = list(self.current_batch)
            try:
                with self.conn.cursor() as cur:
                    execute_values(
                        cur,
                        "INSERT INTO stocks_real_time (time, symbol, price, day_volume) VALUES %s",
                        batch,
                    )
                self.conn.commit()
                self.insert_counter += 1
                print(f"Batch #{self.insert_counter}: {len(batch)} price records saved")
                self.current_batch.clear()
            except Exception:
                self.conn.rollback()
                logging.exception("Database insert failed; retaining batch.")

    def on_event(self, event):
        if not isinstance(event, dict):
            print(event)
            return

        event_type = event.get("event")

        if event_type == "subscribe-status":
            print("Subscription status:", event)
            return

        if event_type != "price":
            print("WebSocket message:", event)
            return

        try:
            timestamp = datetime.fromtimestamp(int(event["timestamp"]), tz=timezone.utc)
            symbol = event["symbol"]
            price = float(event["price"])
            volume = event.get("day_volume")
            if volume is not None:
                volume = float(volume)

            with self.lock:
                self.current_batch.append((timestamp, symbol, price, volume))
                full = len(self.current_batch) >= MAX_BATCH_SIZE

            print(f"{symbol}: {price}")

            if full:
                self.insert_batch()

        except (KeyError, TypeError, ValueError):
            logging.exception("Could not parse price event.")

    def start(self, symbols):
        client = TDClient(apikey=os.getenv("TWELVEDATA_API_KEY"))
        ws = client.websocket(on_event=self.on_event)

        try:
            ws.subscribe(symbols)
            ws.connect()

            print("WebSocket started.")
            print("Subscribed symbols:", ", ".join(symbols))
            print("Press Ctrl+C to stop.")

            last_flush = last_hb = time.monotonic()

            while True:
                time.sleep(1)
                now = time.monotonic()

                if now - last_hb >= HEARTBEAT_INTERVAL:
                    ws.heartbeat()
                    last_hb = now

                if now - last_flush >= FLUSH_INTERVAL:
                    self.insert_batch()
                    last_flush = now

        except KeyboardInterrupt:
            print("\nStopping the data pipeline...")

        finally:
            self.insert_batch()
            try:
                ws.disconnect()
            except Exception:
                pass
            print("Data pipeline stopped.")


def main():
    conn = None
    try:
        conn = get_connection()
        create_table(conn)
        WebsocketPipeline(conn).start(SYMBOLS)
    except Exception:
        logging.exception("Pipeline failed.")
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()