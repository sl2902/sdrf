from abc import ABC, abstractmethod


class BaseModelClient(ABC):
    """Abstract interface for LLM clients. All model clients must implement this."""

    @abstractmethod
    async def extract(self, system_prompt: str, user_prompt: str) -> dict:
        """
        Call the model and return parsed JSON dict.
        Handles retries and rate limits internally.
        """
        ...
