import os
from dotenv import load_dotenv

load_dotenv(override=True)

class AISettings:
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
    DB_HOST = os.getenv("DB_HOST", "labestrela.fmddns.com")
    DB_NAME = os.getenv("DB_NAME", "smart")
    DB_USER = os.getenv("DB_USER", "sa")
    DB_PASS = os.getenv("DB_PASS", "sa")
    DB_PORT = int(os.getenv("DB_PORT", 1433))
    
    @classmethod
    def validate(cls):
        if not cls.GEMINI_API_KEY:
            print("WARNING: GEMINI_API_KEY not found in environment variables. AI features will not work.")

settings = AISettings()
