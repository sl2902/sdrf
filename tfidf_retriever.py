"""
TF-IDF based metadata retrieval.
Finds most similar training papers to each test paper and uses their
SDRF values as additional signal to fill metadata gaps.

Usage in pipeline:
    from sdrf.tfidf_retriever import TFIDFRetriever
    retriever = TFIDFRetriever()
    retriever.fit()
    similar_meta = retriever.retrieve("PXD004010", paper_text)
"""

import os
import glob
import json
import logging
import numpy as np
import pandas as pd
from typing import Optional

logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────
BASE = "/kaggle/input/competitions/harmonizing-the-data-of-your-data"
TRAIN_TEXT_DIR = f"{BASE}/Training_PubText/PubText"
TRAIN_SDRF_DIR = f"{BASE}/Training_SDRFs/HarmonizedFiles"

# Columns to retrieve from similar papers
TARGET_COLS = [
    "characteristics[organism]",
    # "characteristics[organismpart]",
    # "characteristics[disease]",
    # "characteristics[materialtype]",
    "characteristics[label]",
    "characteristics[cleavageagent]",
    "characteristics[modification]",
    "characteristics[modification].1",
    "characteristics[modification].2",
    # "characteristics[sex]",
    # "characteristics[developmentalstage]",
    # "characteristics[celltype]",
    "comment[instrument]",
    "comment[fragmentationmethod]",
    "comment[acquisitionmethod]",
    "comment[fractionationmethod]",
    "comment[precursormasstolerance]",
    "comment[fragmentmasstolerance]",
    "comment[collisionenergy]",
    "comment[separation]",
    "comment[ionizationtype]",
    "comment[ms2massanalyzer]",
    "characteristics[reductionreagent]",
    "characteristics[alkylationreagent]",
]

NA_VALS = {"not applicable", "n/a", "na", "nan", "", "textspan"}


class TFIDFRetriever:
    def __init__(self, top_k: int = 3):
        self.top_k = top_k
        self.fitted = False
        self.train_pxds = []
        self.train_texts = []
        self.train_meta = {}   # pxd -> {col: value}
        self.vectorizer = None
        self.tfidf_matrix = None

    def _load_paper_text(self, fpath: str) -> str:
        """Load and concatenate all text sections from a paper JSON."""
        try:
            with open(fpath) as f:
                data = json.load(f)
            sections = []
            for key in ["TITLE", "ABSTRACT", "METHODS"]:
                val = data.get(key, "")
                if val:
                    sections.append(str(val))
            return " ".join(sections)
        except Exception as e:
            logger.debug(f"Failed to load {fpath}: {e}")
            return ""

    def _load_sdrf_meta(self, pxd: str) -> dict:
        """Load unique non-NA values per column from training SDRF."""
        sdrf_path = os.path.join(TRAIN_SDRF_DIR, f"Harmonized_{pxd}.csv")
        if not os.path.exists(sdrf_path):
            return {}
        try:
            df = pd.read_csv(sdrf_path, dtype=str, low_memory=False)
            df.columns = [c.strip().lower() for c in df.columns]
            meta = {}
            for col in TARGET_COLS:
                if col in df.columns:
                    vals = df[col].dropna().astype(str).unique()
                    vals = [v for v in vals if v.lower() not in NA_VALS]
                    if vals:
                        meta[col] = vals[0]  # take first unique value
            return meta
        except Exception as e:
            logger.debug(f"Failed to load SDRF {pxd}: {e}")
            return {}

    def fit(self):
        """Build TF-IDF matrix from all training papers."""
        from sklearn.feature_extraction.text import TfidfVectorizer

        logger.info("Building TF-IDF index from training papers...")

        train_files = glob.glob(os.path.join(TRAIN_TEXT_DIR, "*.json"))
        for fpath in train_files:
            fname = os.path.basename(fpath)
            pxd = fname.replace("_PubText.json", "").replace("_pubtext.json", "")
            if not pxd.upper().startswith("PXD"):
                continue

            text = self._load_paper_text(fpath)
            if not text:
                continue

            meta = self._load_sdrf_meta(pxd)
            if not meta:
                continue

            self.train_pxds.append(pxd)
            self.train_texts.append(text)
            self.train_meta[pxd] = meta

        if not self.train_texts:
            logger.warning("No training papers loaded for TF-IDF")
            return

        self.vectorizer = TfidfVectorizer(
            max_features=10000,
            ngram_range=(1, 2),
            stop_words="english",
            min_df=2,
        )
        self.tfidf_matrix = self.vectorizer.fit_transform(self.train_texts)
        self.fitted = True
        logger.info(f"TF-IDF index built | {len(self.train_pxds)} training papers")

    def retrieve(self, pxd_id: str, paper: dict) -> dict:
        """
        Find top-K similar training papers and return their metadata.
        Only returns values where all top-K papers agree (high confidence).
        """
        if not self.fitted:
            logger.warning("TFIDFRetriever not fitted — call fit() first")
            return {}

        # Build query text
        text = " ".join(str(paper.get(k, "")) for k in ["TITLE", "ABSTRACT", "METHODS"])
        if not text.strip():
            return {}

        try:
            query_vec = self.vectorizer.transform([text])
        except Exception as e:
            logger.warning(f"TF-IDF transform failed {pxd_id}: {e}")
            return {}

        # Compute cosine similarities
        from sklearn.metrics.pairwise import cosine_similarity
        sims = cosine_similarity(query_vec, self.tfidf_matrix).flatten()

        # Get top-K indices (excluding the paper itself if in training)
        top_indices = np.argsort(sims)[::-1]
        top_k_pxds = []
        for idx in top_indices:
            if self.train_pxds[idx] != pxd_id:
                top_k_pxds.append((self.train_pxds[idx], sims[idx]))
            if len(top_k_pxds) >= self.top_k:
                break

        if not top_k_pxds:
            return {}

        logger.info(
            f"TF-IDF similar papers for {pxd_id}: "
            + ", ".join(f"{p}({s:.2f})" for p, s in top_k_pxds)
        )

        # Collect metadata from similar papers
        # Only use values where similarity is reasonable (>0.1)
        # and all top-K papers agree on the value
        candidates = {}
        for pxd, sim in top_k_pxds:
            if sim < 0.05:  # too dissimilar — skip
                continue
            for col, val in self.train_meta[pxd].items():
                if col not in candidates:
                    candidates[col] = []
                candidates[col].append(val.lower().strip())

        # Only return values where majority of similar papers agree
        result = {}
        min_agreement = max(1, len(top_k_pxds) // 2)  # majority
        for col, vals in candidates.items():
            from collections import Counter
            counter = Counter(vals)
            most_common_val, count = counter.most_common(1)[0]
            if count >= min_agreement:
                # Get original cased value
                for pxd, _ in top_k_pxds:
                    meta_val = self.train_meta[pxd].get(col, "")
                    if meta_val.lower().strip() == most_common_val:
                        result[col] = meta_val
                        break

        logger.info(f"TF-IDF retrieved {len(result)} consensus fields for {pxd_id}")
        return result


# ── Singleton instance ─────────────────────────────────────────
_retriever: Optional[TFIDFRetriever] = None


def get_retriever(top_k: int = 3) -> TFIDFRetriever:
    """Get or build the singleton TF-IDF retriever."""
    global _retriever
    if _retriever is None or not _retriever.fitted:
        _retriever = TFIDFRetriever(top_k=top_k)
        _retriever.fit()
    return _retriever