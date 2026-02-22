"""
Global settings for Mark 3.1.
Loads environment variables and provides configuration.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

class Settings:
    """Application settings loaded from environment."""
    
    # Alpaca
    ALPACA_BASE_URL = os.getenv('ALPACA_BASE_URL', 'https://paper-api.alpaca.markets')
    ALPACA_DATA_KEY = os.getenv('ALPACA_DATA_KEY', '')
    ALPACA_TRADING_KEY = os.getenv('ALPACA_TRADING_KEY', '')
    ALPACA_SECRET_KEY = os.getenv('ALPACA_SECRET_KEY', '')
    ALPACA_FEED = os.getenv('ALPACA_FEED', 'iex')
    ALPACA_PAPER = os.getenv('ALPACA_PAPER', 'true').lower() == 'true'
    
    # Google Sheets
    GSHEET_CLIENT_EMAIL = os.getenv('GSHEET_CLIENT_EMAIL', '')
    GSHEET_PRIVATE_KEY = os.getenv('GSHEET_PRIVATE_KEY', '')
    GSHEET_SPREADSHEET_ID = os.getenv('GSHEET_SPREADSHEET_ID', '')
    
    # Database
    DB_PATH = Path(os.getenv('DB_PATH', 'data/trade_log.db'))
    
    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    # Trading parameters
    TOTAL_CAPITAL = 10000
    MAX_PER_TRADE = 2000
    DAILY_LOSS_LIMIT = 500
    MAX_CONCURRENT_TRADES = 5
    
    # Universe
    MIN_PRICE = 5
    MAX_PRICE = 100
    MIN_VOLUME = 500000
    MAX_UNIVERSE_SIZE = 300
    
    @classmethod
    def validate(cls):
        """Validate required settings are present."""
        required = [
            ('ALPACA_DATA_KEY', cls.ALPACA_DATA_KEY),
            ('ALPACA_TRADING_KEY', cls.ALPACA_TRADING_KEY),
            ('ALPACA_SECRET_KEY', cls.ALPACA_SECRET_KEY),
            ('GSHEET_CLIENT_EMAIL', cls.GSHEET_CLIENT_EMAIL),
            ('GSHEET_PRIVATE_KEY', cls.GSHEET_PRIVATE_KEY),
            ('GSHEET_SPREADSHEET_ID', cls.GSHEET_SPREADSHEET_ID),
        ]
        
        missing = [name for name, value in required if not value]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        
        return True

# Create global settings instance
settings = Settings()