"""
External metadata fetchers for PRIDE and ProteomeXchange.
Uses httpx for native async HTTP. Returns metadata in lowercase key format.
"""

import io
import re
import logging
import httpx
import pandas as pd
from xml.etree import ElementTree as ET
from collections import defaultdict

logger = logging.getLogger(__name__)

PRIDE_API  = "https://www.ebi.ac.uk/pride/ws/archive/v2/projects"
PX_XML_TPL = "https://proteomecentral.proteomexchange.org/cgi/GetDataset?ID={pxd}&outputMode=XML&test=no"
BIGBIO_RAW = "https://raw.githubusercontent.com/bigbio/proteomics-sample-metadata/master/annotated-projects"

_MS_KEYWORDS = [
    "orbitrap", "qtof", "timstof", "exactive", "ltq", "velos",
    "fusion", "lumos", "eclipse", "exploris", "synapt", "xevo",
    "qtrap", "bruker", "astral", "zeno"
]

_HEADERS = {"User-Agent": "SDRF-Pipeline/1.0 (kaggle)"}


async def _get(client: httpx.AsyncClient, url: str):
    try:
        resp = await client.get(url, timeout=15)
        if resp.status_code == 200:
            return resp
    except Exception as e:
        logger.debug(f"Request failed {url}: {e}")
    return None


async def fetch_pride(pxd: str, client: httpx.AsyncClient) -> dict:
    meta = {}
    resp = await _get(client, f"{PRIDE_API}/{pxd}")
    if not resp:
        return meta
    try:
        data = resp.json()

        # Organism — strip NCBI ID suffix e.g. "Homo sapiens (human)" → "Homo sapiens"
        organisms = data.get("organisms", [])
        if organisms:
            name = organisms[0].get("name", "")
            name = re.sub(r'\s*\(.*?\)', '', name).strip()  # remove "(human)" etc
            if name:
                meta["characteristics[organism]"] = name

        # OrganismPart
        parts = data.get("organismParts", [])
        if parts:
            meta["characteristics[organismpart]"] = parts[0].get("name", "")

        # Diseases
        diseases = data.get("diseases", [])
        if diseases:
            names = [d.get("name", "") for d in diseases if d.get("name")]
            if names:
                meta["characteristics[disease]"] = names[0]
                if len(names) > 1:
                    meta["characteristics[disease].1"] = names[1]

        # Instruments
        instruments = data.get("instruments", [])
        if instruments:
            ms_instruments = [
                i.get("name", "") for i in instruments
                if any(kw in i.get("name", "").lower() for kw in _MS_KEYWORDS)
            ]
            if ms_instruments:
                meta["comment[instrument]"] = ms_instruments[0]
                if len(ms_instruments) > 1:
                    meta["comment[instrument].1"] = ms_instruments[1]

        # PTMs
        ptms = data.get("identifiedPTMStrings", [])
        if ptms:
            clean = [re.sub(r'\s*\(.*?\)', '', p).strip() for p in ptms]
            clean = [p for p in clean if p]
            if clean:
                meta["characteristics[modification]"] = clean[0]
                for i, p in enumerate(clean[1:], 1):
                    meta[f"characteristics[modification].{i}"] = p

        # Keywords for label/acquisition/fragmentation
        keywords = data.get("keywords", [])
        kw_text = " ".join([k.get("name", k) if isinstance(k, dict) else str(k) 
                           for k in keywords]).lower()
        if "label free" in kw_text or "label-free" in kw_text:
            meta["characteristics[label]"] = "label free sample"
        elif "tmt" in kw_text:
            meta["characteristics[label]"] = "TMT"
        elif "silac" in kw_text:
            meta["characteristics[label]"] = "SILAC"
        if "hcd" in kw_text:
            meta["comment[fragmentationmethod]"] = "HCD"
        elif "cid" in kw_text:
            meta["comment[fragmentationmethod]"] = "CID"
        if "dia" in kw_text or "data independent" in kw_text:
            meta["comment[acquisitionmethod]"] = "DIA"
        elif "dda" in kw_text or "data dependent" in kw_text:
            meta["comment[acquisitionmethod]"] = "DDA"

    except Exception as e:
        logger.warning(f"PRIDE parse error {pxd}: {e}")
    return meta


async def fetch_px_xml(pxd: str, client: httpx.AsyncClient) -> dict:
    result = defaultdict(list)
    resp = await _get(client, PX_XML_TPL.format(pxd=pxd))
    if not resp:
        return {}
    try:
        if "<ProteomeXchangeDataset" not in resp.text:
            return {}
        root = ET.fromstring(resp.text)
        for cv in root.iter("cvParam"):
            acc    = cv.attrib.get("accession", "")
            name   = cv.attrib.get("name", "")
            cv_ref = cv.attrib.get("cvRef", "")
            if not name:
                continue
            nl = name.lower()
            if cv_ref == "MS" and acc.startswith("MS:"):
                if any(k in nl for k in _MS_KEYWORDS):
                    result["comment[instrument]"].append(f"AC={acc};NT={name}")
                elif any(k in nl for k in ["hcd", "cid", "etd", "ecd"]):
                    result["comment[fragmentationmethod]"].append(name.upper().split()[0])
                elif "data-dependent" in nl or " dda" in nl:
                    result["comment[acquisitionmethod]"].append("DDA")
                elif "data-independent" in nl or " dia" in nl:
                    result["comment[acquisitionmethod]"].append("DIA")
            elif cv_ref in ("NEWT", "NCBI") and name:
                result["characteristics[organism]"].append(name)
    except Exception as e:
        logger.warning(f"PX XML parse error {pxd}: {e}")
    return {k: list(dict.fromkeys(v))[0] for k, v in result.items() if v}


async def fetch_bigbio(pxd: str, client: httpx.AsyncClient) -> dict:
    for url in [
        f"{BIGBIO_RAW}/{pxd}/{pxd}.sdrf.tsv",
        f"{BIGBIO_RAW}/{pxd}/sdrf.tsv",
    ]:
        resp = await _get(client, url)
        if resp and len(resp.text) > 50:
            try:
                df = pd.read_csv(io.StringIO(resp.text), sep="\t", low_memory=False)
                return _sdrf_df_to_meta(df)
            except Exception as e:
                logger.debug(f"BigBio parse error {pxd}: {e}")
    return {}


def _sdrf_df_to_meta(df: pd.DataFrame) -> dict:
    meta = {}
    df.columns = [c.strip().lower() for c in df.columns]
    skip = {"source name", "assay name", "raw data file", "comment[data file]", "usage"}
    for col in df.columns:
        if col in skip:
            continue
        vals = df[col].dropna().astype(str).unique()
        vals = [v for v in vals if v.lower() not in ["not applicable", "n/a", ""]]
        if vals:
            meta[col] = vals[0]
    return meta


async def fetch_all(pxd: str) -> dict:
    """
    Fetch from all external sources concurrently and merge.
    Priority: BigBio (highest) > PRIDE > ProteomeXchange (lowest)
    """
    import asyncio
    async with httpx.AsyncClient(headers=_HEADERS, follow_redirects=True) as client:
        px, pride, bb = await asyncio.gather(
            fetch_px_xml(pxd, client),
            fetch_pride(pxd, client),
            fetch_bigbio(pxd, client),
        )

    meta = {}
    meta.update(px) # lowest priority
    for k, v in pride.items():
        if k not in meta or not meta[k]:
            meta[k] = v # update missing values in px with pride
    meta.update(bb)  # BigBio highest priority

    logger.info(f"External fetch | {pxd} | px={len(px)} pride={len(pride)} bb={len(bb)}")
    return meta