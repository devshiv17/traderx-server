from typing import List
from pydantic_settings import BaseSettings
from pydantic import validator, ConfigDict
import os


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
    allowed_origins: List[str] = ["http://localhost:3000", "http://localhost:3001", "http://localhost:5173"]
    
    # Angel One Configuration
    angel_one_api_key: str = ""
    angel_one_secret: str = ""
    angel_one_client_code: str = ""
    angel_one_pin: str = ""
    angel_one_totp_token: str = ""
    
    model_config = ConfigDict(
        env_file=".env",
        case_sensitive=True,
        extra="ignore"
    )
    
    @validator("allowed_origins", pre=True)
    def assemble_cors_origins(cls, v):
        if isinstance(v, str) and not v.startswith("["):
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, (list, str)):
            return v
        raise ValueError(v)


settings = Settings() 