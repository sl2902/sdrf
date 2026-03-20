import json
import logging

from google import genai
from google.genai import types
from tenacity import (
    retry, stop_after_attempt, wait_exponential,
    before_sleep_log,
)

from .base import BaseModelClient

logger = logging.getLogger(__name__)


class GeminiClient(BaseModelClient):
    # def __init__(self, api_key: str, model: str = "gemini-2.5-pro-preview-03-25", temperature: float = 0):
        # self.client = genai.Client(api_key=api_key)
        # self.model = model
        # self.temperature = temperature
    def __init__(self, project: str, location: str, model: str = "gemini-2.5-pro", temperature: float = 0):
        self.client = genai.Client(
            vertexai=True,
            project=project,
            location=location,
        )
        self.model = model
        self.temperature = temperature

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=60),
        stop=stop_after_attempt(5),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    async def extract(self, system_prompt: str, user_prompt: str) -> dict:
        response = await self.client.aio.models.generate_content(
            model=self.model,
            contents=user_prompt,
            config=types.GenerateContentConfig(
                system_instruction=system_prompt,
                temperature=self.temperature,
                response_mime_type="application/json",
            ),
        )
        text = response.text.strip()
        # Strip markdown fences if present
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text)
