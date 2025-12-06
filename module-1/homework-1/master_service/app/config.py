from typing import List
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    secondaries: str = ""
    port: int = 8000
    timeout: int = 60

    @property
    def secondary_hosts(self) -> List[str]:
        if not self.secondaries:
            return []
        return [h.strip() for h in self.secondaries.split(",")]


settings = Settings()
