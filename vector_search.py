import os
from datetime import datetime, timezone, timedelta

import psycopg2
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("TIGER_HOST"), port=int(os.getenv("TIGER_PORT")),
    user=os.getenv("TIGER_USER"), password=os.getenv("TIGER_PASSWORD"),
    dbname=os.getenv("TIGER_DATABASE"), sslmode="require", connect_timeout=15,
)
cur = conn.cursor()

cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
cur.execute("""
    CREATE TABLE IF NOT EXISTS market_news (
        time TIMESTAMPTZ NOT NULL,
        headline TEXT NOT NULL,
        embedding vector(384)
    );
""")
cur.execute("SELECT create_hypertable('market_news', 'time', if_not_exists => TRUE);")
conn.commit()

model = SentenceTransformer("all-MiniLM-L6-v2")
vec = lambda t: str(model.encode(t).tolist())

headlines = [
    "Bitcoin surges past $84,000 as ETF inflows rise",
    "Ethereum upgrade lowers network fees",
    "Apple reports record iPhone revenue",
    "Tesla shares fall after delivery miss",
    "Fed holds interest rates steady",
    "Crypto exchange hacked, millions stolen",
    "Oil prices drop on weak demand",
    "Nvidia earnings beat expectations on AI chips",
]

now = datetime.now(timezone.utc)
for i, h in enumerate(headlines):
    cur.execute(
        "INSERT INTO market_news VALUES (%s, %s, %s::vector)",
        (now - timedelta(hours=i * 3), h, vec(h)),
    )
conn.commit()
print(f"Inserted {len(headlines)} rows")

query = "cryptocurrency price rally"
cur.execute("""
    SELECT time, headline, embedding <=> %s::vector AS distance
    FROM market_news
    WHERE time > now() - interval '1 day'
    ORDER BY distance
    LIMIT 5;
""", (vec(query),))

print(f"\nTop matches for: {query!r} (last 24h)")
for t, h, d in cur.fetchall():
    print(f"{d:.4f} | {t:%Y-%m-%d %H:%M} | {h}")

conn.close()