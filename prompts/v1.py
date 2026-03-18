SYSTEM_PROMPT = """You are an expert in proteomics experiment metadata extraction.
Your job is to read scientific paper text and extract structured metadata in SDRF format.
Always respond with valid JSON only. No explanation, no markdown, no extra text.
Use "Not Applicable" when information is genuinely absent from the text.
Do NOT guess or hallucinate values — only extract what is explicitly stated.
For Comment[Instrument]: ONLY extract mass spectrometer names. Do NOT extract LC systems,
flow cytometers, sequencers, microscopes, or any non-MS equipment."""


VERIFY_SYSTEM_PROMPT = """You are an expert proteomics data curator.
You will be given a scientific paper and a first-pass metadata extraction.
Your job is to verify each extracted value against the paper text and correct any errors.
Return only the corrected JSON object. No explanation, no markdown."""


def build_user_prompt(title: str, abstract: str, methods: str) -> str:
    return f"""Extract SDRF metadata from this proteomics paper. Return a JSON object with exactly these keys:

{{
  "characteristics[organism]": "Latin binomial species name e.g. 'Homo sapiens', 'Mus musculus'",
  "characteristics[organismpart]": "tissue or organ e.g. 'liver', 'brain cortex', 'plasma', 'erythrocytes', 'cerebrospinal fluid'. If multiple tissues studied, list the most important one here and others in characteristics[organismpart].1 and .2",
  "characteristics[organismpart].1": "second tissue if present else Not Applicable",
  "characteristics[organismpart].2": "third tissue if present else Not Applicable",
  "characteristics[celltype]": "cell type e.g. 'macrophage', 'neuron'",
  "characteristics[cellline]": "cell line name e.g. 'HeLa', 'HEK293', 'MCF7'",
  "characteristics[disease]": "disease or condition e.g. 'breast cancer', 'normal', 'uninfected'. If both disease and control present, put control in characteristics[disease].1",
  "characteristics[disease].1": "control/normal condition if present else Not Applicable",
  "characteristics[sex]": "male or female or Not Applicable",
  "characteristics[age]": "age value with unit e.g. '8 weeks', '65 years'",
  "characteristics[developmentalstage]": "e.g. 'adult', 'embryo', 'larva'",
  "characteristics[strain]": "strain name e.g. 'C57BL/6', '3D7'",
  "characteristics[treatment]": "treatment applied e.g. 'DMSO', 'rapamycin 100nM'",
  "characteristics[label]": "DEFAULT to 'label free sample' if no labeling reagent mentioned. MUST be one of: 'label free sample', 'TMT126', 'TMT127', 'TMT127N', 'TMT127C', 'TMT128', 'TMT128N', 'TMT128C', 'TMT129', 'TMT129N', 'TMT129C', 'TMT130', 'TMT130N', 'TMT130C', 'TMT131', 'TMT131C', 'iTRAQ4plex-114', 'iTRAQ4plex-115', 'iTRAQ4plex-116', 'iTRAQ4plex-117', 'SILAC light', 'SILAC medium', 'SILAC heavy'",
  "characteristics[cleavageagent]": "protease name e.g. 'Trypsin', 'Trypsin/P', 'Lys-C', 'chymotrypsin'. First enzyme only.",
  "characteristics[cleavageagent].1": "second cleavage enzyme if multiple used e.g. 'Lys-C', else Not Applicable",
  "characteristics[modification]": "first PTM name e.g. 'Oxidation', 'Phospho', 'Carbamidomethyl', 'Acetyl'. Do NOT add residue annotations like (M).",
  "characteristics[modification].1": "second PTM if present else Not Applicable",
  "characteristics[modification].2": "third PTM if present else Not Applicable",
  "characteristics[modification].3": "fourth PTM if present else Not Applicable",
  "characteristics[modification].4": "fifth PTM if present else Not Applicable",
  "characteristics[reductionreagent]": "e.g. 'DTT', 'TCEP'",
  "characteristics[alkylationreagent]": "e.g. 'iodoacetamide', 'chloroacetamide'",
  "characteristics[materialtype]": "MUST be one of: 'organism part', 'tissue', 'cell', 'cell line', 'plasma', 'serum', 'secretome', 'membrane fraction'",
  "characteristics[biologicalreplicate]": "number of biological replicates as integer e.g. '3'. Use '1' if not mentioned.",
  "characteristics[depletion]": "depletion method e.g. 'IgY14', 'MARS', 'depleted fraction', 'no depletion', else Not Applicable",
  "characteristics[specimen]": "specimen type e.g. 'biopsy', 'resection', 'FFPE', 'fresh frozen', else Not Applicable",
  "characteristics[geneticmodification]": "e.g. 'knockout', 'overexpression', 'wildtype', else Not Applicable",
  "characteristics[genotype]": "e.g. 'wildtype', 'KRAS G12D', else Not Applicable",
  "characteristics[compound]": "compound or drug e.g. 'rapamycin', 'DMSO', else Not Applicable",
  "characteristics[cellpart]": "subcellular fraction e.g. 'nucleus', 'cytoplasm', 'membrane', else Not Applicable",
  "characteristics[bait]": "bait protein in AP-MS e.g. 'Flag-tagged protein X', else Not Applicable",
  "characteristics[temperature]": "incubation temperature e.g. '37 C', else Not Applicable",
  "characteristics[time]": "time point e.g. '24 hours', '7 days', else Not Applicable",
  "characteristics[tumorstage]": "e.g. 'stage II', 'stage III', else Not Applicable",
  "characteristics[tumorgrade]": "e.g. 'grade 2', 'high grade', else Not Applicable",
  "characteristics[syntheticpeptide]": "MUST always return either 'yes' or 'no'. Return 'yes' if synthetic/spiked-in peptides were used (e.g. iRT peptides, QconCAT, heavy-labeled standards). Return 'no' for all regular proteomics experiments.",
  "characteristics[ancestrycategory]": "ethnic or racial background of study participants if explicitly stated e.g. 'Caucasian', 'Black', 'African American', 'Asian', 'African', 'European', else Not Applicable",
  "characteristics[samplingtime]": "time point of sampling e.g. 'baseline', '0W', '4W', else Not Applicable",
  "characteristics[numberofbiologicalreplicates]": "total number of biological replicates as integer, else Not Applicable",
  "characteristics[numberofsamples]": "total number of samples as integer, else Not Applicable",
  "characteristics[numberoftechnicalreplicates]": "total number of technical replicates as integer, else Not Applicable",
  "characteristics[pooledsample]": "'yes' or 'no', else Not Applicable",
  "characteristics[anatomicsitetumor]": "anatomic site of tumor e.g. 'colon', 'breast', 'lung', else Not Applicable",
  "characteristics[originsitedisease]": "e.g. 'primary', 'metastatic', else Not Applicable",
  "characteristics[tumorcellularity]": "e.g. '70%', else Not Applicable",
  "characteristics[tumorsize]": "e.g. '2 cm', else Not Applicable",
  "comment[instrument]": "mass spectrometer ONLY e.g. 'Q Exactive HF', 'LTQ Orbitrap Velos', 'Orbitrap Exploris 480', 'timsTOF Pro'. NOT LC systems or other equipment.",
  "comment[instrument].1": "second MS instrument if present else Not Applicable",
  "comment[fragmentationmethod]": "MUST use abbreviations: 'HCD', 'CID', 'ETD', 'EThcD', 'UVPD'",
  "comment[fragmentationmethod].1": "second fragmentation method if present else Not Applicable",
  "comment[precursormasstolerance]": "e.g. '10 ppm', '5 ppm'",
  "comment[fragmentmasstolerance]": "e.g. '0.02 Da', '20 ppm'",
  "comment[collisionenergy]": "e.g. '28 NCE', '35%'",
  "comment[ms2massanalyzer]": "e.g. 'Orbitrap', 'ion trap', 'TOF'",
  "comment[fractionationmethod]": "e.g. 'high pH reverse phase', 'SCX', 'no fractionation'",
  "comment[enrichmentmethod]": "e.g. 'IMAC', 'TiO2', 'enrichment of phosphorylated Protein', else Not Applicable",
  "comment[separation]": "e.g. 'LC-MS/MS', 'nano-LC', 'Reversed-phase chromatography'",
  "comment[gradienttime]": "e.g. '120 min', '90 min'",
  "comment[ionizationtype]": "e.g. 'ESI', 'nanospray'",
  "comment[acquisitionmethod]": "e.g. 'DDA', 'DIA', 'PRM'",
  "comment[numberofmissedcleavages]": "integer e.g. '2'",
  "comment[fractionidentifier]": "number of LC fractions per sample as integer. '1' if unfractionated.",
  "comment[numberoffractions]": "total number of fractions if fractionated e.g. '24', else Not Applicable",
  "comment[flowratechromatogram]": "LC flow rate e.g. '300 nL/min', else Not Applicable",
  "factorvalue[cellpart]": "subcellular fraction ONLY if experimental variable, else Not Applicable",
  "factorvalue[compound]": "compound ONLY if experimental variable, else Not Applicable",
  "factorvalue[concentrationofcompound]": "concentration ONLY if experimental variable e.g. '100 nM', else Not Applicable",
  "factorvalue[disease]": "disease ONLY if experimental variable, else Not Applicable",
  "factorvalue[fractionidentifier]": "fraction ONLY if experimental variable, else Not Applicable",
  "factorvalue[geneticmodification]": "genetic modification ONLY if experimental variable, else Not Applicable",
  "factorvalue[treatment]": "treatment ONLY if experimental variable, else Not Applicable",
  "factorvalue[bait]": "bait protein ONLY if experimental variable in AP-MS, else Not Applicable"
}}

TITLE: {title}

ABSTRACT: {abstract}

METHODS: {methods[:6000]}

Return only the JSON object. No other text."""


def build_verify_prompt(title: str, abstract: str, methods: str, first_pass: dict) -> str:
    return f"""You extracted the following metadata from a proteomics paper. Verify each value is supported by the paper text. Correct any errors, remove hallucinated values (replace with "Not Applicable"), and fill in any values you missed.

PAPER:
TITLE: {title}
ABSTRACT: {abstract}
METHODS: {methods[:12000]}

FIRST PASS EXTRACTION:
{first_pass}

Return the corrected JSON object with the same keys. No other text."""
