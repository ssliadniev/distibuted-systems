from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    delay_seconds: int = 5


settings = Settings()
