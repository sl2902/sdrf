"""
V2 Per-file Prompt — Lightweight filename-based metadata distribution.
No paper text. Just filenames + experiment context from V1 global extraction.
Only extracts fields that vary per file: FractionIdentifier, BiologicalReplicate, Label.
"""

import json

PERFILE_SYSTEM_PROMPT = """You are a proteomics filename parser. Analyze raw data filenames to determine per-file metadata.
Return ONLY valid JSON. No markdown, no commentary."""


def build_perfile_prompt(raw_files: list, global_metadata: dict) -> str:
    """Build prompt for per-file metadata extraction from filenames."""
    
    # Summarize experiment context
    label = global_metadata.get("characteristics[label]", "Not Applicable")
    organism = global_metadata.get("characteristics[organism]", "Not Applicable")
    instrument = global_metadata.get("comment[instrument]", "Not Applicable")
    n_fractions_global = global_metadata.get("comment[fractionidentifier]", "1")
    n_replicates_global = global_metadata.get("characteristics[biologicalreplicate]", "1")
    fractionation = global_metadata.get("comment[fractionationmethod]", "Not Applicable")
    
    # Determine if TMT/iTRAQ experiment
    is_multiplexed = any(x in str(label).lower() for x in ["tmt", "itraq", "silac"])
    
    files_str = json.dumps(raw_files)
    
    label_instruction = ""
    if is_multiplexed:
        label_instruction = """- "label": TMT/iTRAQ channel if encoded in filename. Use exact channel names:
    TMT: TMT126, TMT127N, TMT127C, TMT128N, TMT128C, TMT129N, TMT129C, TMT130N, TMT130C, TMT131, TMT131C, TMT132N, TMT132C, TMT133N, TMT133C, TMT134N
    iTRAQ: iTRAQ4plex-114, iTRAQ4plex-115, iTRAQ4plex-116, iTRAQ4plex-117
    SILAC: SILAC light, SILAC medium, SILAC heavy"""
    
    return f"""Analyze these proteomics raw data filenames and extract per-file metadata.

EXPERIMENT CONTEXT:
- Labeling: {label}
- Organism: {organism}  
- Instrument: {instrument}
- Fractionation: {fractionation}
- Expected fractions: {n_fractions_global}
- Expected replicates: {n_replicates_global}

RAW FILENAMES:
{files_str}

For EACH filename, extract:
- "fraction": fraction/run number. Look for patterns: F1, F01, Frac1, fraction_1, _F12_, fr12, _01.raw, run1. Return integer as string e.g. "1", "12". If unfractionated experiment, return "1".
- "replicate": biological replicate number. Look for patterns: BR1, Rep1, biorep1, _R1_, rep_1, biological_replicate_1. Return integer as string e.g. "1", "2", "3". Do NOT confuse with technical replicates or fraction numbers.
{label_instruction}

RULES:
- Every filename MUST have "fraction" and "replicate" keys.
- If you cannot determine fraction, default to "1".
- If you cannot determine replicate, default to "1". 
- Look at ALL filenames together to understand the naming pattern before assigning values.
- Filenames from the same biological sample but different fractions should have the same replicate number.
- Be consistent: if file_A_F1 and file_A_F2 exist, they are fractions of the same replicate.

Return JSON object with ALL filenames as keys:
{{
  "filename1.raw": {{"fraction": "1", "replicate": "1"}},
  "filename2.raw": {{"fraction": "2", "replicate": "1"}},
  "filename3.raw": {{"fraction": "1", "replicate": "2"}}
}}"""


def chunk_files(raw_files: list, chunk_size: int = 40) -> list:
    """Split raw files into chunks for processing."""
    return [raw_files[i:i + chunk_size] for i in range(0, len(raw_files), chunk_size)]