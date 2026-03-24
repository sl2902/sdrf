
"""
SDRF Extraction Pipeline
------------------------
Pluggable model and prompt. Two-pass extraction with async, retry, and caching.
 
Usage in Kaggle notebook:
    from sdrf.models import OpenAIClient       # or GeminiClient
    from sdrf.prompts import v1 as prompt
    from sdrf.pipeline import run_extraction
 
    model   = OpenAIClient(api_key=OPENAI_API_KEY, model="gpt-4o")
    results = await run_extraction(model, prompt, model_name="gpt_4o")
"""
 
import os, json, glob, ast, asyncio, logging
import pandas as pd
 
logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────
TEST_PATH     = "/kaggle/input/competitions/harmonizing-the-data-of-your-data/Test PubText/Test PubText"
SAMPLE_SUB    = "/kaggle/input/competitions/harmonizing-the-data-of-your-data/SampleSubmission.csv"
OUTPUT_DIR    = "/kaggle/working"
CACHE_FILE    = os.path.join(OUTPUT_DIR, "extraction_cache.json")


async def _extract_one(pxd_id, fpath, model, prompt, semaphore, two_pass: bool):
    async with semaphore:
        # Load paper
        with open(fpath) as f:
            paper = json.load(f)

        title    = paper.get("TITLE", "")
        abstract = paper.get("ABSTRACT", "")
        methods  = paper.get("METHODS", "")

        raw_files = paper.get("Raw Data Files", [])
        if isinstance(raw_files, str):
            try:    raw_files = ast.literal_eval(raw_files)
            except: raw_files = [raw_files]
        raw_files = list(dict.fromkeys(raw_files))

        try:
            # Pass 1 — extraction
            user_prompt_1 = prompt.build_user_prompt(title, abstract, methods)
            pass1 = await model.extract(prompt.SYSTEM_PROMPT, user_prompt_1)
            logger.info(f"Pass 1 complete | {pxd_id}")

            if two_pass:
                # Pass 2 — verification
                user_prompt_2 = prompt.build_verify_prompt(title, abstract, methods, json.dumps(pass1, indent=2))
                pass2 = await model.extract(prompt.VERIFY_SYSTEM_PROMPT, user_prompt_2)
                logger.info(f"Pass 2 complete | {pxd_id}")
                metadata = pass2
            else:
                metadata = pass1
            
            # Fetch from external sources (PRIDE, ProteomeXchange, BigBio)
            # and fill any gaps left by the LLM
            try:
                from .fetchers import fetch_all
                external = await fetch_all(pxd_id)
                for k, v in external.items():
                    if not metadata.get(k) or str(metadata[k]).lower() in ["not applicable", "n/a", ""]:
                        metadata[k] = v
                logger.info(f"External fetch complete | {pxd_id} | {len(external)} fields")
            except Exception as e:
                logger.warning(f"External fetch failed {pxd_id}: {e}")
            
            # Regex extraction — fills gaps for fields with predictable patterns
            try:
                from .regex_extractor import run_regex_extraction
                regex_vals = run_regex_extraction(paper)
                for k, v in regex_vals.items():
                    if not metadata.get(k) or str(metadata[k]).lower() in ["not applicable", "n/a", ""]:
                        metadata[k] = v
                logger.info(f"Regex extraction complete | {pxd_id} | {len(regex_vals)} fields")
            except Exception as e:
                logger.warning(f"Regex extraction failed {pxd_id}: {e}")

            status = "ok"
        except Exception as e:
            logger.error(f"FAILED {pxd_id}: {e}")
            metadata = {}
            status = f"error: {e}"

        return pxd_id, {
            "metadata": metadata,
            "raw_files": raw_files,
            "status": status,
        }


async def run_extraction(
    model,
    prompt,
    two_pass: bool = False,
    max_concurrent: int = 2,
    model_name: str = "default",
    cache_dir: str = OUTPUT_DIR,
) -> dict:
    """
    Run async extraction over all test papers.
    Skips already-cached successful results.
    Saves cache after each completion for crash safety.
    """
    cache_file = os.path.join(cache_dir, f"extraction_cache_{model_name}.json")
    # Load existing cache
    if os.path.exists(cache_file):
        with open(cache_file) as f:
            results = json.load(f)
        logger.info(f"Loaded {len(results)} cached results")
    else:
        results = {}

    # Find test files
    test_files = sorted([
        f for f in glob.glob(os.path.join(TEST_PATH, "*.json"))
        if os.path.basename(f).upper().startswith("PXD")
    ])
    logger.info(f"Total test papers: {len(test_files)}")

    # Determine pending
    pending = []
    for fpath in test_files:
        fname  = os.path.basename(fpath)
        pxd_id = fname.replace("_PubText.json", "").replace("_pubtext.json", "")
        cache_key = f"{pxd_id}_pass2" if two_pass else pxd_id
        already_done = (
            cache_key in results and
            results[cache_key].get("status") == "ok" and
            results[cache_key].get("metadata")
        )
        if not already_done:
            pending.append((pxd_id, fpath))

    print(f"Cached : {len(test_files) - len(pending)} | Pending: {len(pending)}")

    if not pending:
        print("All papers cached.")
        return results

    semaphore = asyncio.Semaphore(max_concurrent)
    tasks = [
        _extract_one(pxd_id, fpath, model, prompt, semaphore, two_pass)
        for pxd_id, fpath in pending
    ]

    completed = 0
    for coro in asyncio.as_completed(tasks):
        pxd_id, result = await coro
        cache_key = f"{pxd_id}_pass2" if two_pass else pxd_id
        results[cache_key] = result
        completed += 1
        s = "✓" if result["status"] == "ok" else "✗"
        print(f"  [{completed}/{len(pending)}] {s} {pxd_id} — {len(result['raw_files'])} raw files")

        with open(cache_file, "w") as f:
            json.dump(results, f, indent=2)

    succeeded = sum(1 for k, r in results.items() if r.get("status") == "ok")
    print(f"\nExtraction complete. {succeeded}/{len(results)} succeeded.")
    return results
