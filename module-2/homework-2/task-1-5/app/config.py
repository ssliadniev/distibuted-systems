from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # db config
    postgres_user: str = "admin"
    postgres_password: str = "secretpassword"
    postgres_db: str = "counter_db"
    postgres_host: str = "db"
    postgres_port: int = 5432

    # app config
    threads: int = 10
    iterations: int = 10000
    target_user_id: int = 1

    class Config:
        env_file = ".env"


settings = Settings()
