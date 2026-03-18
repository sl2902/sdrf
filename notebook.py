# ── SDRF Extraction — Kaggle Notebook ────────────────────────
# Pluggable model + prompt, two-pass, async, cached

# ── Cell 1: Install deps ──────────────────────────────────────
# !pip install tenacity google-genai -q

# ── Cell 2: Config ────────────────────────────────────────────
import asyncio, logging
logging.basicConfig(level=logging.INFO)

from kaggle_secrets import UserSecretsClient
secrets = UserSecretsClient()

# ── Cell 3: Choose model ──────────────────────────────────────
# Option A: OpenAI
from sdrf.models import OpenAIClient
model = OpenAIClient(
    api_key=secrets.get_secret("OPENAI_API_KEY"),
    model="gpt-4o",
)

# Option B: Gemini (uncomment to use)
# from sdrf.models import GeminiClient
# model = GeminiClient(
#     api_key=secrets.get_secret("GEMINI_API_KEY"),
#     model="gemini-2.5-pro-preview-03-25",
# )

# ── Cell 4: Choose prompt ─────────────────────────────────────
from sdrf.prompts import v1 as prompt

# ── Cell 5: Run extraction ────────────────────────────────────
TWO_PASS      = False   # set True to enable verification pass
MAX_CONCURRENT = 2      # reduce if hitting rate limits

from sdrf.pipeline import run_extraction
results = await run_extraction(
    model=model,
    prompt=prompt,
    two_pass=TWO_PASS,
    max_concurrent=MAX_CONCURRENT,
)

# ── Cell 6: Build & save submission ──────────────────────────
from sdrf.submission import save_submission
save_submission(results, two_pass=TWO_PASS)
