import os
from dotenv import load_dotenv

# Load environment variables from .env file
# Assuming we run from the project root or similar, we find .env
load_dotenv()

class Settings:
    ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "demo")
    
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-market-data")
    
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
    POSTGRES_DB = os.getenv("POSTGRES_DB", "stock_dw")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "stock_user")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "stock_pass")
    
    @classmethod
    def get_jdbc_url(cls):
        return f"jdbc:postgresql://{cls.POSTGRES_HOST}:{cls.POSTGRES_PORT}/{cls.POSTGRES_DB}"

settings = Settings()
