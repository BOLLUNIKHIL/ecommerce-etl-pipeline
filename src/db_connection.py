import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv()

DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT")
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# quote_plus handles special characters like @ in your password
DB_PASSWORD_ENCODED = quote_plus(DB_PASSWORD)

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD_ENCODED}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def get_engine():
    engine = create_engine(DB_URL, pool_pre_ping=True)
    return engine

if __name__ == "__main__":
    print("🔌 Testing database connection...\n")
    engine = get_engine()
    with engine.connect() as connection:
        result = connection.execute(text("SELECT version();"))
        version = result.fetchone()
        print(f"✅ Connected successfully!")
        print(f"📦 PostgreSQL version: {version[0]}")