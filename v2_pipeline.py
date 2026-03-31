"""
V2 Per-file Pipeline — Runs after V1 global extraction.
Takes cached V1 results, sends filenames to Gemini for per-file metadata.
Caches results separately.
"""

import os
import json
import asyncio
import logging

logger = logging.getLogger(__name__)

OUTPUT_DIR = "/kaggle/working"


async def _extract_perfile_one(pxd_id, raw_files, global_metadata, model, prompt_module, semaphore, chunk_size=40):
    """Extract per-file metadata for a single PXD."""
    async with semaphore:
        all_per_file = {}
        
        # Chunk files if needed
        chunks = prompt_module.chunk_files(raw_files, chunk_size)
        
        for chunk_idx, chunk in enumerate(chunks):
            try:
                user_prompt = prompt_module.build_perfile_prompt(chunk, global_metadata)
                result = await model.extract(prompt_module.PERFILE_SYSTEM_PROMPT, user_prompt)
                
                if isinstance(result, dict):
                    # Validate structure
                    valid_keys = {"fraction", "replicate", "label"}
                    for fname, overrides in result.items():
                        if isinstance(overrides, dict):
                            clean = {k: str(v).strip() for k, v in overrides.items() 
                                    if k in valid_keys and v}
                            if clean:
                                all_per_file[fname] = clean
                
                logger.info(f"Chunk {chunk_idx+1}/{len(chunks)} complete | {pxd_id} | {len(result) if isinstance(result, dict) else 0} files")
                
            except Exception as e:
                logger.warning(f"Chunk {chunk_idx+1} failed for {pxd_id}: {e}")
        
        return pxd_id, all_per_file


async def run_perfile_extraction(
    v1_results: dict,
    model,
    prompt_module,
    max_concurrent: int = 2,
    cache_dir: str = OUTPUT_DIR,
    chunk_size: int = 40,
) -> dict:
    """
    Run per-file extraction for all PXDs using V1 cached results.
    Returns dict: {pxd: {filename: {fraction, replicate, label}}}
    """
    cache_file = os.path.join(cache_dir, "extraction_cache_perfile.json")
    
    # Load existing cache
    if os.path.exists(cache_file):
        with open(cache_file) as f:
            perfile_results = json.load(f)
        logger.info(f"Loaded {len(perfile_results)} cached per-file results")
    else:
        perfile_results = {}
    
    # Determine pending
    pending = []
    for pxd_id, result in v1_results.items():
        pxd_clean = pxd_id.replace("_pass2", "")
        if result.get("status") != "ok":
            continue
        if pxd_clean in perfile_results and perfile_results[pxd_clean]:
            continue
        
        raw_files = result.get("raw_files", [])
        metadata = result.get("metadata", {})
        if isinstance(metadata, list):
            metadata = metadata[0] if metadata else {}
        
        if raw_files:
            pending.append((pxd_clean, raw_files, metadata))
    
    print(f"Per-file extraction: Cached {len(perfile_results)} | Pending {len(pending)}")
    
    if not pending:
        print("All per-file extractions cached.")
        return perfile_results
    
    semaphore = asyncio.Semaphore(max_concurrent)
    tasks = [
        _extract_perfile_one(pxd_id, raw_files, metadata, model, prompt_module, semaphore, chunk_size)
        for pxd_id, raw_files, metadata in pending
    ]
    
    completed = 0
    for coro in asyncio.as_completed(tasks):
        pxd_id, per_file = await coro
        perfile_results[pxd_id] = per_file
        completed += 1
        print(f"  [{completed}/{len(pending)}] {pxd_id} — {len(per_file)} files with overrides")
        
        # Save after each completion
        with open(cache_file, "w") as f:
            json.dump(perfile_results, f, indent=2)
    
    print(f"\nPer-file extraction complete. {len(perfile_results)} PXDs processed.")
    return perfile_results