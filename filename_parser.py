"""
Filename Parser — Per-file metadata extraction from raw filenames.
Lightweight LLM call: no paper text, just filenames + experiment summary.
Returns per-file overrides for fraction, replicate, label.
"""

import json
import logging

logger = logging.getLogger(__name__)

FILENAME_SYSTEM_PROMPT = """You are a proteomics filename parser. Extract metadata encoded in raw data filenames.
Return ONLY valid JSON. No markdown, no commentary."""


def build_filename_prompt(raw_files: list, global_metadata: dict) -> str:
    """Build a lightweight prompt for filename parsing."""
    # Summarize experiment context from global metadata
    label = global_metadata.get("characteristics[label]", "unknown")
    organism = global_metadata.get("characteristics[organism]", "unknown")
    instrument = global_metadata.get("comment[instrument]", "unknown")
    disease = global_metadata.get("characteristics[disease]", "unknown")

    files_str = json.dumps(raw_files[:200])  # cap at 200 files to avoid token blow-up

    return f"""These are raw data filenames from a proteomics experiment.

EXPERIMENT CONTEXT:
- Labeling: {label}
- Organism: {organism}
- Instrument: {instrument}

RAW FILENAMES:
{files_str}

For each filename, extract any of these values if encoded in the filename:
- "fraction": fraction number (look for patterns like F1, Frac01, fraction_12, _F12_, fr12). Return just the integer as a string e.g. "1", "12".
- "replicate": biological replicate number (look for BR1, Rep2, bio_rep_3, biorep1, _R1_). Return just the integer e.g. "1", "2".
- "label": TMT/iTRAQ channel if multiplexed (look for TMT126, TMT127N, TMT131C, iTRAQ114, 126C, 127N). Return the full channel name e.g. "TMT126", "TMT127N".

Return a JSON object where keys are filenames and values are objects with only the fields you can confidently extract. Omit filenames where nothing is parseable. Example:

{{
  "sample1_F3_BR2.raw": {{"fraction": "3", "replicate": "2"}},
  "TMT126_Control_F1.raw": {{"fraction": "1", "label": "TMT126"}}
}}

Only include fields you are confident about. Do NOT guess. If a filename has no parseable metadata, omit it entirely."""


async def parse_filenames(raw_files: list, global_metadata: dict, model) -> dict:
    """
    Parse filenames using a lightweight LLM call.
    Returns dict: {filename: {fraction, replicate, label}}
    """
    if not raw_files:
        return {}

    prompt = build_filename_prompt(raw_files, global_metadata)

    try:
        result = await model.extract(FILENAME_SYSTEM_PROMPT, prompt)
        if not isinstance(result, dict):
            logger.warning("Filename parser returned non-dict")
            return {}

        # Validate structure — each value should be a dict with known keys
        valid_keys = {"fraction", "replicate", "label"}
        cleaned = {}
        for fname, overrides in result.items():
            if not isinstance(overrides, dict):
                continue
            clean = {k: str(v).strip() for k, v in overrides.items() if k in valid_keys and v}
            if clean:
                cleaned[fname] = clean

        logger.info(f"Filename parser: {len(cleaned)}/{len(raw_files)} files with overrides")
        return cleaned

    except Exception as e:
        logger.warning(f"Filename parsing failed: {e}")
        return {}