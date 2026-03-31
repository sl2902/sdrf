# SDRF Metadata Extraction Pipeline

Dual-LLM extraction pipeline for harmonizing proteomics metadata from scientific papers into SDRF format.

## Method

**Extraction:**
- Gemini 2.5 Pro (primary) and GPT-4o (secondary) extract structured metadata from paper text (title, abstract, methods[:18000])
- Single-pass extraction with 60+ SDRF fields in one JSON prompt
- Results merged: Gemini primary, GPT-4o fills gaps where Gemini returned "Not Applicable"

**External Data Sources:**
- PRIDE API: organism, disease, instrument, keywords
- ProteomeXchange XML: instrument, organism, fragmentation
- Regex extractor: age, sex, collision energy, gradient time, flow rate

**Post-processing:**
- Normalization maps for instrument (AC/NT ontology codes), cleavage agent, modification (UNIMOD codes), organism, alkylation reagent (IAA), ionization type (nanoESI/ESI)
- Global mode fallback from training data (>75% frequency threshold) for high-prevalence columns
- Default nanoESI for missing ionization type

**Key Design Decisions:**
- Context-aware disease extraction gated on clinical keywords
- Pruned organism part normalization map to preserve granularity
- Test format follows SampleSubmission.csv (plain text), not training SDRF format (ontology codes)

## Results

- Local Training F1: 0.519 (87 valid PXDs)
- Kaggle Public LB: 0.371
- Key finding: format alignment (IAA, nanoESI, DDA/DIA) drove larger gains than extraction improvements

## Installation & Usage
```bash
pip install -r requirements.txt
```

Run the Kaggle notebook cells sequentially or click `Save Version`. Extraction caches are stored in `/kaggle/working/` for crash recovery. If running
using `Save Version`, the caches including the submission.csv are persisted.

## References
[Never Global](https://www.kaggle.com/code/mawramusawwar/harmonizing-the-data-of-your-data-0-27)