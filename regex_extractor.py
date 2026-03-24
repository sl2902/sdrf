"""
Regex-based metadata extractor.
Runs over full paper text and fills gaps left by LLM extraction.
All functions return a value string or None.
"""

import re
from typing import Optional


def _clean(text: str) -> str:
    return re.sub(r'\s+', ' ', text).strip()


# ── Individual extractors ─────────────────────────────────────

def extract_age(text: str) -> Optional[str]:
    t = text.lower()
    # Age ranges e.g. "18-65 years", "20 to 80 years"
    m = re.search(r'(\d+)\s*[-–to]+\s*(\d+)\s*(year|yr|week|month|day)s?', t)
    if m:
        return f"{m.group(1)}-{m.group(2)} {m.group(3)}s"
    # Single age e.g. "65 years", "8 weeks old"
    m = re.search(r'(\d+)\s*(year|yr|week|month|day)s?\s*(?:old|of age)?', t)
    if m:
        return f"{m.group(1)} {m.group(2)}s"
    return None


def extract_sex(text: str) -> Optional[str]:
    t = text.lower()
    # Remove FBS and serum mentions to avoid false positives
    t = re.sub(r'fetal\s+bovine\s+serum|foetal\s+bovine\s+serum|\bfbs\b', '', t)
    has_female = bool(re.search(
        r'\b(?:female\s+(?:patient|donor|rat|mouse|mice|subject|volunteer|participant|donor)|'
        r'(?:patient|donor|subject)s?\s+were\s+female|'
        r'women|woman|\bfemale\b)', t))
    has_male = bool(re.search(
        r'\b(?:male\s+(?:patient|donor|rat|mouse|mice|subject|volunteer|participant|donor)|'
        r'(?:patient|donor|subject)s?\s+were\s+male|'
        r'men\b|man\b|\bmale\b)', t))
    if has_female and has_male:
        return "male|female"
    if has_female:
        return "female"
    if has_male:
        return "male"
    return None


def extract_developmental_stage(text: str) -> Optional[str]:
    t = text.lower()
    t = re.sub(r'fetal\s+bovine\s+serum|foetal\s+bovine\s+serum|\bfbs\b|\bfcs\b', '', t)
    if re.search(r'\bembryo(?:nic)?\s+(?:stem|cell|tissue|development)', t):
        return "embryo"
    if re.search(r'\bfetal\b|\bfoetal\b', t):
        return "fetal"
    if re.search(r'\bneonat|\bnewborn\b', t):
        return "neonatal"
    if re.search(r'\badult\b', t):
        return "adult"
    return None


def extract_strain(text: str) -> Optional[str]:
    t = text
    patterns = [
        (r'\bC57BL/6\w*', 'C57BL/6'),
        (r'\bBALB/c\w*', 'BALB/c'),
        (r'\bC57BL/6J\b', 'C57BL/6J'),
        (r'\bFVB/N\b', 'FVB/N'),
        (r'\bSprague[- ]Dawley\b', 'Sprague-Dawley'),
        (r'\bWistar\b', 'Wistar'),
        (r'\bNOD/SCID\b', 'NOD/SCID'),
        (r'\bNMRI\b', 'NMRI'),
        (r'\bDBA/2\b', 'DBA/2'),
        (r'\b129/Sv\b', '129/Sv'),
        (r'\bW303\b', 'W303'),
        (r'\bBY4741\b', 'BY4741'),
        (r'\b3D7\b', '3D7'),
        (r'\bK562\b', None),  # cell line not strain
    ]
    for pattern, value in patterns:
        if value and re.search(pattern, t, re.IGNORECASE):
            return value
    return None


def extract_ancestry(text: str) -> Optional[str]:
    t = text.lower()
    if re.search(r'\bcaucasian\b|\beuropean\s+(?:descent|ancestry|american)\b|\bwhite\b', t):
        return "Caucasian"
    if re.search(r'\bafrican\s+american\b|\bblack\s+(?:american|patient|donor)\b', t):
        return "African American"
    if re.search(r'\bafrican\b', t):
        return "African"
    if re.search(r'\basian\b', t):
        return "Asian"
    if re.search(r'\bhispanic\b|\blatino\b|\blatina\b', t):
        return "Hispanic"
    return None


def extract_collision_energy(text: str) -> Optional[str]:
    t = text.lower()
    # e.g. "28 NCE", "35% NCE", "27 eV"
    m = re.search(r'(\d+(?:\.\d+)?)\s*%?\s*(?:nce|normalized\s+collision\s+energy)', t)
    if m:
        val = m.group(1)
        orig = m.group(0)
        if '%' in orig:
            return f"{val}% NCE"
        return f"{val} NCE"
    # Only fall back to eV if NCE not found
    m = re.search(r'collision\s+energy\s+(?:of\s+)?(\d+(?:\.\d+)?)\s*(%|ev|nce)?', t)
    if m:
        val = m.group(1)
        unit = (m.group(2) or "NCE").upper()
        if unit == "EV":
            unit = "NCE"  # normalize eV to NCE
        return f"{val} {unit}"
    return None


def extract_missed_cleavages(text: str) -> Optional[str]:
    t = text.lower()
    m = re.search(r'(?:up to|maximum|max\.?|allowing?)\s+(\d)\s+missed\s+cleavage', t)
    if m:
        return m.group(1)
    m = re.search(r'(\d)\s+missed\s+cleavage', t)
    if m:
        return m.group(1)
    return None


def extract_gradient_time(text: str) -> Optional[str]:
    t = text.lower()
    m = re.search(r'(\d+)\s*[-–]\s*min(?:ute)?\s+(?:gradient|elution|lc)', t)
    if m:
        return f"{m.group(1)} min"
    m = re.search(r'(?:gradient|elution)\s+(?:of\s+)?(\d+)\s*min', t)
    if m:
        return f"{m.group(1)} min"
    m = re.search(r'(\d+)\s*min(?:ute)?\s+(?:linear\s+)?gradient', t)
    if m:
        return f"{m.group(1)} min"
    return None


def extract_flow_rate(text: str) -> Optional[str]:
    t = text.lower()
    m = re.search(r'(\d+(?:\.\d+)?)\s*nl/min', t)
    if m:
        return f"{m.group(1)} nL/min"
    m = re.search(r'(\d+(?:\.\d+)?)\s*μl/min', t)
    if m:
        return f"{m.group(1)} μL/min"
    m = re.search(r'flow\s+rate\s+(?:of\s+)?(\d+(?:\.\d+)?)\s*(nl|μl|ul)/min', t)
    if m:
        return f"{m.group(1)} {m.group(2).replace('ul','μL').replace('nl','nL')}/min"
    return None


def extract_acquisition_method(text: str) -> Optional[str]:
    t = text.lower()
    if re.search(r'\bdia\b|data.independent', t):
        return "DIA"
    if re.search(r'\bdda\b|data.dependent', t):
        return "DDA"
    if re.search(r'\bprm\b|parallel\s+reaction\s+monitoring', t):
        return "PRM"
    if re.search(r'\bsrm\b|\bmrm\b|multiple\s+reaction\s+monitoring', t):
        return "MRM"
    return None


def extract_ionization(text: str) -> Optional[str]:
    t = text.lower()
    if re.search(r'\bnanospray\b|\bnano.?esi\b', t):
        return "nanospray"
    if re.search(r'\besi\b|electrospray', t):
        return "ESI"
    if re.search(r'\bmaldi\b', t):
        return "MALDI"
    return None


def extract_separation(text: str) -> Optional[str]:
    t = text.lower()
    if re.search(r'nano.?lc|nano\s+liquid\s+chrom', t):
        return "nano-LC"
    if re.search(r'\blc.ms/ms\b|\blc-ms\b', t):
        return "LC-MS/MS"
    if re.search(r'reversed.phase\s+(?:lc|chrom|hplc)', t):
        return "reversed-phase LC"
    return None


def extract_enrichment(text: str) -> Optional[str]:
    t = text.lower()
    if re.search(r'phospho(?:peptide|protein)?\s*enrich|enrich.*phospho|tio2|imac', t):
        return "enrichment of phosphorylated Protein"
    if re.search(r'ubiquitin.*enrich|diglycine|k.?gg\s+enrich', t):
        return "ubiquitination enrichment"
    if re.search(r'glyco(?:peptide|protein)?\s*enrich|lectin\s+enrich', t):
        return "glycopeptide enrichment"
    return None


def extract_depletion(text: str) -> Optional[str]:
    t = text.lower()
    if re.search(r'no\s+deplet|without\s+deplet|not\s+deplet', t):
        return "no depletion"
    if re.search(r'deplet(?:ed|ion)\s+(?:of\s+)?(?:high.?abund|abundant)', t):
        return "depleted fraction"
    if re.search(r'\bigy14\b|\bmars\b.*deplet|deplet.*\bmars\b', t):
        return "depleted fraction"
    return None


# ── Column → extractor mapping ────────────────────────────────

EXTRACTORS = {
    "characteristics[age]":               extract_age,
    "characteristics[sex]":               extract_sex,
    "characteristics[developmentalstage]": extract_developmental_stage,
    "characteristics[strain]":            extract_strain,
    "characteristics[ancestrycategory]":  extract_ancestry,
    "comment[collisionenergy]":           extract_collision_energy,
    "comment[numberofmissedcleavages]":   extract_missed_cleavages,
    "comment[gradienttime]":              extract_gradient_time,
    "comment[flowratechromatogram]":      extract_flow_rate,
    "comment[acquisitionmethod]":         extract_acquisition_method,
    "comment[ionizationtype]":            extract_ionization,
    "comment[separation]":                extract_separation,
    "comment[enrichmentmethod]":          extract_enrichment,
    "characteristics[depletion]":         extract_depletion,
}


def run_regex_extraction(paper: dict) -> dict:
    """
    Run all regex extractors over paper text.
    Returns dict of col -> value for non-None results.
    Uses full text (all sections) for maximum coverage.
    """
    full_text = " ".join(str(v) for v in paper.values() if isinstance(v, str))

    results = {}
    for col, extractor in EXTRACTORS.items():
        try:
            val = extractor(full_text)
            if val:
                results[col] = val
        except Exception:
            pass

    return results