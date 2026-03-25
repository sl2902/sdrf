import re
import json, os

INSTRUMENT_AC = {
    "ltq orbitrap velos":    "AC=MS:1001742;NT=LTQ Orbitrap Velos",
    "ltq-orbitrap velos":    "AC=MS:1001742;NT=LTQ Orbitrap Velos",
    "ltq orbitrap elite":    "NT=LTQ Orbitrap Elite;AC=MS:1001910",
    "ltq-orbitrap elite":    "NT=LTQ Orbitrap Elite;AC=MS:1001910",
    "ltq orbitrap xl":       "NT=LTQ Orbitrap XL;AC=MS:1000556",
    "ltq-orbitrap xl":       "NT=LTQ Orbitrap XL;AC=MS:1000556",
    "q exactive":            "NT=Q Exactive;AC=MS:1001911",
    "q-exactive":            "NT=Q Exactive;AC=MS:1001911",
    "q exactive hf":         "NT=Q Exactive HF;AC=MS:1002877",
    "q-exactive hf":         "NT=Q Exactive HF;AC=MS:1002877",
    "q exactive hf-x":       "NT=Q Exactive HF-X;AC=MS:1003000",
    "q-exactive hf-x":       "NT=Q Exactive HF-X;AC=MS:1003000",
    "q exactive plus":       "NT=Q Exactive Plus;AC=MS:1002634",
    "q-exactive plus":       "NT=Q Exactive Plus;AC=MS:1002634",
    "orbitrap exploris 480": "NT=Orbitrap Exploris 480;AC=MS:1003028",
    "orbitrap exploris480":  "NT=Orbitrap Exploris 480;AC=MS:1003028",
    "exploris 480":          "NT=Orbitrap Exploris 480;AC=MS:1003028",
    "exploris480":           "NT=Orbitrap Exploris 480;AC=MS:1003028",
    "exploris480 mass spectrometer": "NT=Orbitrap Exploris 480;AC=MS:1003028",
    "orbitrap fusion":       "NT=Orbitrap Fusion;AC=MS:1002416",
    "orbitrap fusion lumos": "NT=Orbitrap Fusion Lumos;AC=MS:1002732",
    "fusion lumos":          "NT=Orbitrap Fusion Lumos;AC=MS:1002732",
    "orbitrap astral":       "NT=Orbitrap Astral;AC=MS:1003378",
    "timstof pro":           "NT=timsTOF Pro;AC=MS:1003005",
    "timstof scp":           "NT=timsTOF SCP;AC=MS:1003229",
    "tripletof 6600":        "NT=TripleTOF 6600;AC=MS:1002863",
    "triple tof 6600":       "NT=TripleTOF 6600;AC=MS:1002863",
    "tripletof 5600":        "NT=TripleTOF 5600+;AC=MS:1002048",
    "tripletof 5600+":       "NT=TripleTOF 5600+;AC=MS:1002048",
    "triple tof 5600":       "NT=TripleTOF 5600+;AC=MS:1002048",
    "synapt xs":             "NT=Synapt XS;AC=MS:1003250",
    "zeno tof 7600":         "NT=ZenoTOF 7600;AC=MS:1003294",
    "ab sciex zeno tof 7600": "NT=ZenoTOF 7600;AC=MS:1003294",
    "orbitrap exploris 480 mass spectrometer": "NT=Orbitrap Exploris 480;AC=MS:1003028",
    "synapt xs mass spectrometer": "NT=Synapt XS;AC=MS:1003250",
    "synapt xs": "NT=Synapt XS;AC=MS:1003250",
    "q-exactive hf x": "NT=Q Exactive HF-X;AC=MS:1003000",
    "q exactive hf x": "NT=Q Exactive HF-X;AC=MS:1003000",
    "orbitrap elite": "NT=LTQ Orbitrap Elite;AC=MS:1001910",
    "q exactive hf hybrid quadrupole-orbitrap": "NT=Q Exactive HF;AC=MS:1002877",
    "q exactive hf hybrid quadrupole orbitrap": "NT=Q Exactive HF;AC=MS:1002877",
    # "zeno tof 7600": "NT=ZenoTOF 7600;AC=MS:1003294",
    # "zenotof 7600": "NT=ZenoTOF 7600;AC=MS:1003294",
    # "ab sciex zeno tof 7600": "NT=ZenoTOF 7600;AC=MS:1003294",
    # "orbitrap astral mass spectrometer": "NT=Orbitrap Astral;AC=MS:1003378",
}

CLEAVAGE_AC = {
    "trypsin":              "NT=Trypsin;AC=MS:1001251",
    "trypsin/p":            "NT=Trypsin/P;AC=MS:1001313",
    "lys-c":                "NT=Lys-C;AC=MS:1001309",
    "lysc":                 "NT=Lys-C;AC=MS:1001309",
    "lys c":                "NT=Lys-C;AC=MS:1001309",
    "glu-c":                "NT=Glu-C;AC=MS:1001917",
    "gluc":                 "NT=Glu-C;AC=MS:1001917",
    "glu c":                "NT=Glu-C;AC=MS:1001917",
    "asp-n":                "NT=Asp-N;AC=MS:1001303",
    "aspn":                 "NT=Asp-N;AC=MS:1001303",
    "asp n":                "NT=Asp-N;AC=MS:1001303",
    "chymotrypsin":         "NT=Chymotrypsin;AC=MS:1001306",
    "lys-n":                "NT=Lys-N;AC=MS:1003093",
    "lysn":                 "NT=Lys-N;AC=MS:1003093",
    "trypsin/lys-c mix":    "NT=Trypsin/Lys-C Mix;AC=MS:1003009",
    "trypsin/lys-c":        "NT=Trypsin/Lys-C Mix;AC=MS:1003009",
    "lys-c/trypsin":        "NT=Trypsin/Lys-C Mix;AC=MS:1003009",
}

MODIFICATION_AC = {
    "carbamidomethyl":        "NT=Carbamidomethyl;TA=C;AC=UNIMOD:4;MT=fixed",
    "carbamidomethylation":   "NT=Carbamidomethyl;TA=C;AC=UNIMOD:4;MT=fixed",
    "iodoacetamide":          "NT=Carbamidomethyl;TA=C;AC=UNIMOD:4;MT=fixed",
    "propionamide":           "NT=Propionamide;MT=variable;TA=C;AC=UNIMOD:24",
    "oxidation":              "NT=Oxidation;AC=UNIMOD:35;TA=M;MT=variable",
    "phospho":                "NT=Phospho;AC=UNIMOD:21;TA=S,T,Y;MT=variable",
    "phosphorylation":        "NT=Phospho;AC=UNIMOD:21;TA=S,T,Y;MT=variable",
    "acetyl":                 "NT=Acetyl;AC=UNIMOD:1;TA=K;MT=variable",
    "acetylation":            "NT=Acetyl;AC=UNIMOD:1;TA=K;MT=variable",
    "deamidated":             "NT=Deamidated;MT=Variable;TA=N,Q;AC=UNIMOD:7",
    "deamidation":            "NT=Deamidated;MT=Variable;TA=N,Q;AC=UNIMOD:7",
    "methyl":                 "NT=Methyl;AC=UNIMOD:34;TA=K,R;MT=variable",
    "methylation":            "NT=Methyl;AC=UNIMOD:34;TA=K,R;MT=variable",
    "dimethyl":               "NT=Dimethyl;AC=UNIMOD:36;TA=K,R;MT=variable",
    "trimethyl":              "NT=Trimethyl;AC=UNIMOD:37;TA=K;MT=variable",
    "carbamyl":               "NT=Carbamyl;AC=UNIMOD:5;TA=K,N-term;MT=variable",
    "ammonia-loss":           "NT=Ammonia-loss;AC=UNIMOD:27;TA=N,Q,K,R;MT=variable",
    "pyro-glu":               "NT=Pyro-glu;AC=UNIMOD:28;TA=Q,E;MT=variable",
    "digly":                  "NT=GlyGly;AC=UNIMOD:121;TA=K;MT=variable",
    "ubiquitination":         "NT=GlyGly;AC=UNIMOD:121;TA=K;MT=variable",
    "s-nitrosylation":        "NT=Nitrosyl;AC=UNIMOD:275;TA=C;MT=variable",
    "tmtpro":                 "NT=TMTpro;AC=UNIMOD:2016;TA=K,N-term;MT=fixed",
    "tmt6plex":               "NT=TMT6plex;AC=UNIMOD:737;TA=K,N-term;MT=fixed",
    "itraq4plex":             "NT=iTRAQ4plex;AC=UNIMOD:214;TA=K,N-term;MT=fixed",
    "itraq8plex":             "NT=iTRAQ8plex;AC=UNIMOD:730;TA=K,N-term;MT=fixed",
    "ubiquitin":              "NT=GlyGly;AC=UNIMOD:121;TA=K;MT=variable",
    "ubiquitination":         "NT=GlyGly;AC=UNIMOD:121;TA=K;MT=variable",
}

FRAGMENTATION_MAP = {
    "collision-induced dissociation": "CID",
    "higher-energy collisional dissociation": "HCD",
    "higher energy collisional dissociation": "HCD",
    "electron transfer dissociation": "ETD",
    "electron-transfer/higher-energy collision dissociation": "EThcD",
    "etd/hcd": "EThcD",
    "ethcd": "EThcD",
    "ultraviolet photodissociation": "UVPD",
    "hcd fragmentation": "HCD",
    "cid fragmentation": "CID",
}

ORGANISM_MAP = {
    "human": "Homo sapiens",
    "homo sapiens": "Homo sapiens",
    "mouse": "Mus musculus",
    "mus musculus": "Mus musculus",
    "rat": "Rattus norvegicus",
    "rattus norvegicus": "Rattus norvegicus",
    "yeast": "Saccharomyces cerevisiae",
    "saccharomyces cerevisiae": "Saccharomyces cerevisiae",
    "e. coli": "Escherichia coli",
    "escherichia coli": "Escherichia coli",
    "arabidopsis": "Arabidopsis thaliana",
    "arabidopsis thaliana": "Arabidopsis thaliana",
    "zebrafish": "Danio rerio",
    "danio rerio": "Danio rerio",
    "drosophila": "Drosophila melanogaster",
    "drosophila melanogaster": "Drosophila melanogaster",
    "c. elegans": "Caenorhabditis elegans",
    "caenorhabditis elegans": "Caenorhabditis elegans",
    "plasmodium falciparum": "Plasmodium falciparum",
    "p. falciparum": "Plasmodium falciparum",
    "sars-cov-2": "Severe acute respiratory syndrome coronavirus 2",
    "sars-cov2": "Severe acute respiratory syndrome coronavirus 2",
    "pig": "Sus scrofa",
    "sus scrofa": "Sus scrofa",
    "chicken": "Gallus gallus",
    "gallus gallus": "Gallus gallus",
    "xenopus": "Xenopus laevis",
    "xenopus laevis": "Xenopus laevis",
    "bovine": "Bos taurus",
    "bos taurus": "Bos taurus",
    "cow": "Bos taurus",
    "rabbit": "Oryctolagus cuniculus",
    "oryctolagus cuniculus": "Oryctolagus cuniculus",
    "hamster": "Mesocricetus auratus",
    "mesocricetus auratus": "Mesocricetus auratus",
}

ORGANISM_PART_MAP = {
    "plasma": "blood plasma",
    "serum": "blood serum",
    "csf": "cerebrospinal fluid",
    "frontal cortex": "brain",
    "cerebellum": "brain",
    "cortex": "brain",
    "hippocampus": "brain",
    "left atrium": "heart",
    "right atrium": "heart",
    "left ventricle": "heart",
    "right ventricle": "heart",
    "ovarian": "ovary",
    "left ovary": "ovary",
    "right ovary": "ovary",
}

FRACTIONATION_MAP = {
    "no fractionation":                  "no fractionation",
    "none":                              "no fractionation",
    "high ph reverse phase":             "NT=High-pH reversed-phase chromatography;AC=PRIDE:0000564",
    "high ph rp":                        "NT=High-pH reversed-phase chromatography;AC=PRIDE:0000564",
    "high-ph reversed-phase":            "NT=High-pH reversed-phase chromatography;AC=PRIDE:0000564",
    "hprp":                              "NT=High-pH reversed-phase chromatography;AC=PRIDE:0000564",
    "hphrp":                             "NT=High-pH reversed-phase chromatography;AC=PRIDE:0000564",
    "basic ph reverse phase":            "NT=High-pH reversed-phase chromatography;AC=PRIDE:0000564",
    "basic reverse phase":               "NT=High-pH reversed-phase chromatography;AC=PRIDE:0000564",
    "scx":                               "NT=Strong cation-exchange chromatography (SCX);AC=PRIDE:0000561",
    "strong cation exchange":            "NT=Strong cation-exchange chromatography (SCX);AC=PRIDE:0000561",
    "sax":                               "NT=Strong anion-exchange chromatography (SAX);AC=PRIDE:0000558",
    "strong anion exchange":             "NT=Strong anion-exchange chromatography (SAX);AC=PRIDE:0000558",
    "sds-page":                          "NT=SDS-PAGE;AC=PRIDE:0000568",
    "sds page":                          "NT=SDS-PAGE;AC=PRIDE:0000568",
    "reversed-phase chromatography":     "NT=Reversed-phase chromatography;AC=PRIDE:0000563",
    "reversed phase chromatography":     "NT=Reversed-phase chromatography;AC=PRIDE:0000563",
    "rplc":                              "NT=Reversed-phase chromatography;AC=PRIDE:0000563",
    "hplc":                              "NT=High-performance liquid chromatography;AC=PRIDE:0000565",
}

ANCESTRY_MAP = {
    "caucasian": "Caucasian",
    "white": "Caucasian",
    "european": "Caucasian",
    "african american": "African American",
    "black": "Black",
    "african": "African",
    "asian": "Asian",
    "hispanic": "Hispanic",
    "latino": "Hispanic",
}

DISEASE_MAP = {
            "wildtype": "normal",
            "wild type": "normal", 
            "healthy": "normal",
            "wt": "normal",
            # "lung carcinoma": "lung cancer",
            # "adenocarcinoma of lung": "lung adenocarcinoma",
            # "gfp expression": "normal",
            # "gfp": "normal", 
            # "mock-infected": "normal",
            # "mock infected": "normal",
            # "uninfected": "normal",       
}

_PRIDE_PTM_MAP = {
    "monohydroxylated residue": "Oxidation",
    "iodoacetamide derivatized residue": "Carbamidomethyl",
    "phosphorylated residue": "Phospho",
    "acetylated residue": "Acetyl",
    "deamidated residue": "Deamidated",
    "ubiquitinylated lysine": "GlyGly",
    "methylated residue": "Methyl",
    "dimethylated residue": "Dimethyl",
    "trimethylated residue": "Trimethyl",
}

# Non-MS instruments to block
_NON_MS_KEYWORDS = [
    "cryo", "flow cytom", "sequenc", "illumina", "acoustic", "therapy",
    "microscop", "microlc", "nanodrop", "bioanalyzer", "tapestation",
]

# MS instrument keywords required for whitelist
_MS_KEYWORDS = [
    "orbitrap", "exactive", "exploris", "fusion", "lumos", "astral",
    "timstof", "qtof", "sciex", "bruker", "synapt", "xevo",
    "velos", "elite", "impact", "compact", "tof 5600", "tof 6600",
    "zeno", "astral",
]


def normalize_value(col: str, val: str) -> str:
    col = col.lower().strip()
    if not val or str(val).strip().lower() in ["not applicable", "n/a", ""]:
        return "Not Applicable"
    v  = str(val).strip()
    vl = v.lower()

    if "instrument" in col:
        if any(kw in vl for kw in _NON_MS_KEYWORDS):
            return "Not Applicable"
        if not any(kw in vl for kw in _MS_KEYWORDS):
            return "Not Applicable"
        return INSTRUMENT_AC.get(vl, v)
    # not working; OBO lookup uses lowercase matching
    # elif "modification" in col and "factorvalue" not in col:
    #     vl_clean = re.sub(r'\s*\([^)]*\)', '', vl).strip()
    #     return (MODIFICATION_AC.get(vl_clean) or 
    #             MODIFICATION_AC.get(vl) or 
    #             _UNIMOD_LOOKUP.get(vl_clean) or 
    #             _UNIMOD_LOOKUP.get(vl) or v)

    elif "fragmentationmethod" in col:
        return FRAGMENTATION_MAP.get(vl, v.upper() if len(v) <= 6 else v)

    elif "modification" in col and "factorvalue" not in col:
        vl_clean = re.sub(r'\s*\([^)]*\)', '', vl).strip()
        return MODIFICATION_AC.get(vl_clean, MODIFICATION_AC.get(vl, v))

    elif "cleavageagent" in col:
        return CLEAVAGE_AC.get(vl, v)

    elif "organism" in col and "part" not in col:
        return ORGANISM_MAP.get(vl, v)

    # elif "sex" in col:
    #     if "female" in vl or "woman" in vl:
    #         return "female"
    #     elif "male" in vl or "man" in vl:
    #         return "male"
    #     elif "female" in vl or "woman" in vl and "male" in vl or "man" in vl:
    #         return "male|female"
    #     return v
    elif "sex" in col:
        if "male|female" in vl or "female|male" in vl:
            return "male"
        if "female" in vl or "woman" in vl or "women" in vl:
            return "female"
        elif "male" in vl or "man" in vl or "men" in vl:
            return "male"
        elif vl == "f":
            return "female"
        elif vl == "m":
            return "male"
        return v

    elif "cellline" in col:
        return re.sub(r'\s+', '', v)

    elif "alkylationreagent" in col:
        if "iodoacetamide" in vl or "2-iodoacetamide" in vl:
            return "iodoacetamide"
        if "chloroacetamide" in vl:
            return "chloroacetamide"
        return v

    elif "reductionreagent" in col:
        if "dtt" in vl or "dithiothreitol" in vl:
            return "DTT"
        if "tcep" in vl:
            return "TCEP"
        return v
    
    elif "fractionationmethod" in col:
        return FRACTIONATION_MAP.get(vl, v)

    elif "collisionenergy" in col:
        # Normalize e.g. "28" → "28 NCE", "30%" → "30% NCE"
        if re.search(r'nce|nce|% ce$', vl):
            return v  # already has units
        v_clean = v.strip().rstrip('%').strip()
        if v_clean.replace('.','').isdigit():
            return f"{v.strip()} NCE"
        return v
    
    elif "ancestrycategory" in col:
        return ANCESTRY_MAP.get(vl, v)
    
    elif "organismpart" in col:
        return ORGANISM_PART_MAP.get(vl, v)
    
    # elif "disease" in col and "factorvalue" not in col:
    #     return DISEASE_MAP.get(vl, v)

    return v


def _load_obo_lookup(filename: str) -> dict:
    """Load OBO lookup JSON if available."""
    paths = [
        f"/kaggle/input/datasets/laxmsun/sdrf-dir/{filename}",
        f"/kaggle/working/{filename}",
    ]
    for path in paths:
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f)
    return {}

# # Load once at module level
# _PSI_MS_LOOKUP = _load_obo_lookup("psi_ms_lookup.json")
# _UNIMOD_LOOKUP = _load_obo_lookup("unimod_lookup.json")
