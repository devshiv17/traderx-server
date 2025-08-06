from typing import List
import os
from dotenv import load_dotenv

# Load .env file explicitly
load_dotenv()

try:
    # Pydantic v2 - BaseSettings moved to pydantic-settings
    from pydantic_settings import BaseSettings
    from pydantic import field_validator, ConfigDict
    PYDANTIC_V2 = True
except ImportError:
    try:
        # Pydantic v2 early versions
        from pydantic import BaseSettings, validator
        PYDANTIC_V2 = True
    except ImportError:
        # Pydantic v1
        from pydantic import BaseSettings, validator
        PYDANTIC_V2 = False


class Settings(BaseSettings):
    # MongoDB Configuration
    mongodb_url: str = "mongodb+srv://mail2shivap17:syqzpekjQBCc9oee@traders.w2xrjgy.mongodb.net/?retryWrites=true&w=majority&appName=traders"
    database_name: str = "trading_signals"
    
    # JWT Configuration
    secret_key: str = "your-secret-key-here-make-it-long-and-secure"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    
    # Application Configuration
    app_name: str = "Trading Signals API"
    debug: bool = False
    environment: str = "production"
    
    # CORS Configuration
    allowed_origins: List[str] = ["http://localhost:3000", "http://localhost:3001", "http://localhost:3002", "http://localhost:5173"]
    
    # Angel One Configuration
    angel_one_api_key: str = os.getenv("ANGEL_ONE_API_KEY", "")
    angel_one_secret: str = os.getenv("ANGEL_ONE_SECRET", "")
    angel_one_client_code: str = os.getenv("ANGEL_ONE_CLIENT_CODE", "")  
    angel_one_pin: str = os.getenv("ANGEL_ONE_PIN", "")
    angel_one_totp_token: str = os.getenv("ANGEL_ONE_TOTP_TOKEN", "")
    
    if PYDANTIC_V2:
        model_config = ConfigDict(
            env_file=".env",
            case_sensitive=True,
            extra="ignore"
        )
    else:
        class Config:
            env_file = ".env"
            case_sensitive = True
            extra = "ignore"
    
    if PYDANTIC_V2:
        @field_validator("allowed_origins", mode="before")
        @classmethod
        def assemble_cors_origins(cls, v):
            if isinstance(v, str) and not v.startswith("["):
                return [i.strip() for i in v.split(",")]
            elif isinstance(v, (list, str)):
                return v
            raise ValueError(v)
    else:
        @validator("allowed_origins", pre=True)
        def assemble_cors_origins(cls, v):
            if isinstance(v, str) and not v.startswith("["):
                return [i.strip() for i in v.split(",")]
            elif isinstance(v, (list, str)):
                return v
            raise ValueError(v)


settings = Settings() 