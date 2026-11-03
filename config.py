"""
EpiScope Configuration Module
Central configuration for all pipeline components.
"""
import os
import sys

# Force UTF-8 stdout/stderr on Windows to prevent cp1252 charmap encoding errors
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# Ensure JAVA_HOME points to a full JDK 17+ (with jdk.incubator.vector for PySpark 4)
jdk_candidates = [
    r"C:\Program Files\Eclipse Adoptium\jdk-21.0.12.101-hotspot",
    r"C:\Program Files\Eclipse Adoptium\jdk-25.0.4.101-hotspot",
]
for jdk in jdk_candidates:
    if os.path.isdir(jdk):
        os.environ["JAVA_HOME"] = jdk
        bin_dir = os.path.join(jdk, "bin")
        current_path = os.environ.get("PATH", "")
        if bin_dir not in current_path:
            os.environ["PATH"] = bin_dir + os.pathsep + current_path
        break

# ─── Paths ───────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
BRONZE_DIR = os.path.join(DATA_DIR, "bronze")
SILVER_DIR = os.path.join(DATA_DIR, "silver")
GOLD_DIR = os.path.join(DATA_DIR, "gold")
SERVING_DIR = os.path.join(DATA_DIR, "serving")

# ─── Generator defaults ────────────────────────────────────────────
DEFAULT_SEED = 42
DEFAULT_SF = 0.25          # Scale factor: 0.25 for dev, 1.0 for full
START_YEAR = 2021
END_YEAR = 2025
NUM_DISTRICTS = 64
MIN_FACILITIES = 600

# ─── Disease ICD-10 codes ──────────────────────────────────────────
DISEASE_CODES = {
    "dengue":    ["A90", "A91"],
    "diarrhoea": ["A00", "A09"],
    "influenza": ["J10", "J11"],
    "malaria":   ["B50", "B51", "B52", "B53", "B54"],
}
BACKGROUND_CODES = ["R50", "J06", "K52", "L30", "M79", "N39", "S00", "T14", "Z00"]

# ─── Epidemiological parameters ────────────────────────────────────
# Peak epi-week for each disease (1-indexed)
DISEASE_PEAK_WEEK = {
    "dengue": 35,       # Late monsoon (Aug-Sep)
    "diarrhoea": 20,    # Pre-monsoon + post-flood (May)
    "influenza": 28,    # Rainy season (Jul)
    "malaria": 30,      # Post-monsoon (Jul-Aug)
}

# Base annual incidence per 100,000 population
DISEASE_BASE_INCIDENCE = {
    "dengue": 180,
    "diarrhoea": 400,
    "influenza": 250,
    "malaria": 30,
}

# Admission rate, case fatality rate
DISEASE_SEVERITY = {
    "dengue":    {"admit_rate": 0.35, "cfr": 0.005},
    "diarrhoea": {"admit_rate": 0.15, "cfr": 0.002},
    "influenza": {"admit_rate": 0.08, "cfr": 0.001},
    "malaria":   {"admit_rate": 0.25, "cfr": 0.003},
}

# Reporting delay distribution parameters (lognormal)
REPORTING_DELAY_MU = 1.0
REPORTING_DELAY_SIGMA = 0.8
MAX_REPORTING_DELAY = 21

# ─── Data quality defect rates ─────────────────────────────────────
DUPLICATE_RATE = 0.02        # 2% duplicates
ORPHAN_FK_RATE = 0.005       # 0.5% orphan facility keys
INVALID_AGE_RATE = 0.01      # 1% invalid ages

# ─── Privacy ───────────────────────────────────────────────────────
SMALL_CELL_THRESHOLD = 5     # Suppress counts 1-4
HASH_ALGORITHM = "sha256"

# ─── Spark ─────────────────────────────────────────────────────────
SPARK_APP_NAME = "EpiScope"
SPARK_PARTITIONS = 8

# ─── Bangladesh districts ─────────────────────────────────────────
DIVISIONS = {
    "Barishal":    ["Barguna", "Barishal", "Bhola", "Jhalokati", "Patuakhali", "Pirojpur"],
    "Chattogram":  ["Bandarban", "Brahmanbaria", "Chandpur", "Chattogram", "Comilla",
                    "Cox's Bazar", "Feni", "Khagrachhari", "Lakshmipur", "Noakhali", "Rangamati"],
    "Dhaka":       ["Dhaka", "Faridpur", "Gazipur", "Gopalganj", "Kishoreganj",
                    "Madaripur", "Manikganj", "Munshiganj", "Narayanganj", "Narsingdi",
                    "Rajbari", "Shariatpur", "Tangail"],
    "Khulna":      ["Bagerhat", "Chuadanga", "Jessore", "Jhenaidah", "Khulna",
                    "Kushtia", "Magura", "Meherpur", "Narail", "Satkhira"],
    "Mymensingh":  ["Jamalpur", "Mymensingh", "Netrokona", "Sherpur"],
    "Rajshahi":    ["Bogura", "Chapainawabganj", "Joypurhat", "Naogaon", "Natore",
                    "Pabna", "Rajshahi", "Sirajganj"],
    "Rangpur":     ["Dinajpur", "Gaibandha", "Kurigram", "Lalmonirhat", "Nilphamari",
                    "Panchagarh", "Rangpur", "Thakurgaon"],
    "Sylhet":      ["Habiganj", "Moulvibazar", "Sunamganj", "Sylhet"],
}

# District population estimates (simplified, based on Census 2022 proportions)
# Total ~170M; populations in thousands
DISTRICT_POPULATIONS = {
    "Dhaka": 14000, "Chattogram": 8500, "Gazipur": 5300, "Narayanganj": 3600,
    "Comilla": 6200, "Rajshahi": 3200, "Khulna": 2600, "Sylhet": 3800,
    "Rangpur": 3200, "Mymensingh": 5900, "Barishal": 2500, "Bogura": 3800,
    "Tangail": 4100, "Narsingdi": 2400, "Brahmanbaria": 3100, "Kishoreganj": 3300,
    "Sirajganj": 3400, "Dinajpur": 3300, "Jessore": 3100, "Pabna": 2800,
    "Faridpur": 2100, "Noakhali": 3800, "Jamalpur": 2600, "Cox's Bazar": 2700,
    "Chandpur": 2600, "Feni": 1600, "Lakshmipur": 1900, "Gopalganj": 1300,
    "Manikganj": 1600, "Madaripur": 1300, "Rajbari": 1100, "Shariatpur": 1200,
    "Munshiganj": 1700, "Satkhira": 2100, "Natore": 1900,
    "Kushtia": 2100, "Magura": 1100, "Narail": 800, "Chuadanga": 1200,
    "Meherpur": 700, "Jhenaidah": 1900, "Bagerhat": 1500, "Bhola": 2000,
    "Patuakhali": 1700, "Pirojpur": 1200, "Jhalokati": 700, "Barguna": 1000,
    "Bandarban": 500, "Khagrachhari": 700, "Rangamati": 700,
    "Nilphamari": 2000, "Kurigram": 2300, "Lalmonirhat": 1400, "Gaibandha": 2700,
    "Panchagarh": 1100, "Thakurgaon": 1500, "Chapainawabganj": 1800,
    "Naogaon": 2900, "Joypurhat": 1000, "Netrokona": 2400, "Sherpur": 1500,
    "Habiganj": 2300, "Moulvibazar": 2100, "Sunamganj": 2700,
}

# Hill tract districts (higher malaria risk)
HILL_DISTRICTS = ["Bandarban", "Khagrachhari", "Rangamati", "Cox's Bazar"]

# Major cities (higher dengue risk)
URBAN_DISTRICTS = ["Dhaka", "Chattogram", "Gazipur", "Narayanganj", "Rajshahi",
                   "Khulna", "Sylhet", "Rangpur", "Comilla", "Bogura"]

def get_division(district):
    """Return the division a district belongs to."""
    for div, districts in DIVISIONS.items():
        if district in districts:
            return div
    return "Unknown"

def get_all_districts():
    """Return a flat list of all districts."""
    return [d for districts in DIVISIONS.values() for d in districts]
