"""
This file contains globals and other variables used by process_incoming_data.py

Part of a utility that processes data from the Office of Research and
munges it into output formats expected by the CFPB chart display organisms.

Output formats are documented at
www.github.com/cfpb/consumer-credit-trends
"""

# Global variables for processing data files from the Office of Research

# Default save folder if another folder is not specified
DEFAULT_INPUT_FOLDER = "~/Github/consumer-credit-trends-data/data"
DEFAULT_OUTPUT_FOLDER = "~/Github/consumer-credit-trends-data/processed_data/"

# Filenames for non-market-specific files:
# Data snapshot, inquiry index
SNAPSHOT_FNAME_KEY = "data_snapshot"

# Text descriptors for data snapshots
PERCENT_CHANGE_DESCRIPTORS = ["decrease", "increase"]

# Data base year
BASE_YEAR = 2000
DATA_FILE_DATE_SCHEMA = "%Y-%m"
SNAPSHOT_DATE_SCHEMA = "%Y-%m-%d"

# Output column name schema for index files
INQUIRY_INDEX_OUTPUT_SCHEMA = [
    "month",
    "date",
    "inquiry_index",
    "unadjusted_inquiry_index"
]
TIGHTNESS_INDEX_OUTPUT_SCHEMA = [
    "month",
    "date",
    "tightness_index",
    "unadjusted_credit_tightness_index"
]

# Input/output schemas
MAP_OUTPUT_SCHEMA = ["fips_code", "state_abbr", "value"]
SUMMARY_NUM_OUTPUT_SCHEMA = ["month", "date", "num", "num_unadj"]
SUMMARY_VOL_OUTPUT_SCHEMA = ["month", "date", "vol", "vol_unadj"]
YOY_SUMMARY_OUTPUT_SCHEMA = ["month", "date", "yoy_num", "yoy_vol"]

# Groups - become column name prefixes
AGE = "age"
INCOME = "income_level"
SCORE = "credit_score"

# Output: "month", "date", "yoy_<type>", "yoy_<type>",..., "yoy_<type>"
# All the "yoy_<type>" inputs get added in processing
GROUP_YOY_OUTPUT_SCHEMA = ["month", "date"]
# YOY groups
# CFPB design standards: sentence case and no spaces around dashes
AGE_YOY_IN = ["Younger than 30", "30-44", "45-64", "65 and older"]
AGE_YOY_COLS = ["younger-than-30", "30-44", "45-64", "65-and-older"]
AGE_YOY_JSON = ["Younger than 30", "30-44", "45-64", "65 and older"]
INCOME_YOY_IN = ["Low", "Moderate", "Middle", "High"]
INCOME_YOY_COLS = ["low", "moderate", "middle", "high"]
INCOME_YOY_JSON = INCOME_YOY_IN  # No changes for dashes or caps
SCORE_YOY_IN = [
    "Deep Subprime",
    "Subprime",
    "Near Prime",
    "Prime",
    "Superprime"
]
SCORE_YOY_COLS = [
    "deep-subprime",
    "subprime",
    "near-prime",
    "prime",
    "super-prime"
]
SCORE_YOY_JSON = [
    "Deep subprime",
    "Subprime",
    "Near-prime",
    "Prime",
    "Super-prime"
]

# Fixes input text to follow agency guidelines and design manual
TEXT_FIXES = {"30-44": "Age 30-44",
              "45-64": "Age 45-64",
              "65 and older": "Age 65 and older",
              "Deep Subprime": "Deep subprime",
              "Near Prime": "Near-prime",
              "Superprime": "Super-prime",
              }

# Output: "month", "date", "vol", "vol_unadj", "<grouptype>_group"
GROUP_VOL_OUTPUT_SCHEMA = ["month", "date", "vol", "vol_unadj", "{}_group"]

# Market names - these become directory names
MARKET_NAMES = {"AUT": "auto-loans",
                "CRC": "credit-cards",
                "HCE": "heces",
                "HLC": "helocs",
                "MTG": "mortgages",
                "PER": "personal-loans",
                "RET": "retail-loans",
                "STU": "student-loans",
                }

# State FIPS codes - used to translate state codes into abbr
FIPS_CODES = {1:  "AL",
              2:  "AK",
              4:  "AZ",
              5:  "AR",
              6:  "CA",
              8:  "CO",
              9:  "CT",
              10: "DE",
              11: "DC",
              12: "FL",
              13: "GA",
              15: "HI",
              16: "ID",
              17: "IL",
              18: "IN",
              19: "IA",
              20: "KS",
              21: "KY",
              22: "LA",
              23: "ME",
              24: "MD",
              25: "MA",
              26: "MI",
              27: "MN",
              28: "MS",
              29: "MO",
              30: "MT",
              31: "NE",
              32: "NV",
              33: "NH",
              34: "NJ",
              35: "NM",
              36: "NY",
              37: "NC",
              38: "ND",
              39: "OH",
              40: "OK",
              41: "OR",
              42: "PA",
              44: "RI",
              45: "SC",
              46: "SD",
              47: "TN",
              48: "TX",
              49: "UT",
              50: "VT",
              51: "VA",
              53: "WA",
              54: "WV",
              55: "WI",
              56: "WY",
              }
