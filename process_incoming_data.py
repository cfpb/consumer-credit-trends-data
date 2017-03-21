#!/usr/bin/env python
"""
Processes incoming data from the Office of Research and munges it into
the output formats expected by the CFPB chart display organisms.

Output formats are documented at 
www.github.com/cfpb/consumer-credit-trends
"""

## Python library imports
import os
import csv
import datetime
import math
from pprint import pformat


__author__ = "Consumer Financial Protection Bureau"
__credits__ = ["Hillary Jeffrey"]
__license__ = "CC0-1.0"
__version__ = "0.1"
__maintainer__ = "CFPB"
__email__ = "tech@cfpb.gov"
__status__ = "Development"

## Global variables
# Default save folder if another folder is not specified
DEFAULT_INPUT_FOLDER = "~/Github/consumer-credit-trends-data/data"
DEFAULT_OUTPUT_FOLDER = "~/Github/consumer-credit-trends-data/processed_data/"

## Data snapshot variables
# Data snapshot default name
SNAPSHOT_FNAME_KEY = "data_snapshot"
SNAPSHOT_FNAME_HTML = "data_snapshot.html"

# Data snapshot snippet template
SNAPSHOT_HTML = \
"""<h3><b>{}</b><br>{} originated</h3>
<h3><br><b>${}</b><br>Dollar volume of new {}</h3>
<h3><br><b>{}% {}</b><br>In year-over-year originations</h3>
"""

# Text filler for data snapshot templates
HTML_MKT_NAMES = {"AUT": ["Auto loans", "loans"],     # Auto loans
                  "CRC": ["Credit cards", "cards"],   # Credit cards
                  "HCE": ["HECE loans", "loans"],     # Home Equity, Closed-End
                  "HLC": ["HELOCs", "HELOCs"],        # Home Equity Line of Credit (HELOC)
                  "MTG": ["Mortgages", "mortgages"],  # Mortgages
                  "PER": ["Personal loans", "loans"], # Personal loans
                  "RET": ["Retail loans", "loans"],   # Retail loans
                  "STU": ["Student loans", "loans"],  # Student loans
                  }
HTML_DESC = ["decrease", "increase"]

# Market+.csv filename suffix length
MKT_SFX_LEN = -8

# Data base year
BASE_YEAR = 2000
SEC_TO_MS = 1000

# Input/output schemas
MAP_OUTPUT_SCHEMA = ["fips_code", "state_abbr", "value"]
SUMMARY_NUM_OUTPUT_SCHEMA = ["month","date","num","num_unadj"]
SUMMARY_VOL_OUTPUT_SCHEMA = ["month","date","vol","vol_unadj"]
YOY_SUMMARY_OUTPUT_SCHEMA = ["month","date","yoy_num","yoy_vol"]
DATE_SCHEMA = "%Y-%m"

# Groups - become column name prefixes
AGE = "age"
INCOME = "income_level"
SCORE = "credit_score"

# Output: "month","date","yoy_<type>","yoy_<type>",...,"yoy_<type>"
# All the "yoy_<type>" inputs get added in processing
GROUP_YOY_OUTPUT_SCHEMA = ["month","date"]
# YOY groups
# CFPB design standards: sentence case and no spaces around dashes
AGE_YOY_IN = ["Younger than 30","30 - 44","45 - 64","65 and older"]
AGE_YOY_COLS = ["younger-than-30","30-44","45-64","65-and-older"]
AGE_YOY_JSON = ["Younger than 30","30-44","45-64","65 and older"]
INCOME_YOY_IN = ["Low","Moderate","Middle","High"]
INCOME_YOY_COLS = ["low","moderate","middle","high"]
INCOME_YOY_JSON = INCOME_YOY_IN # No changes for dashes or caps
SCORE_YOY_IN = ["Deep Subprime","Subprime","Near Prime","Prime","Superprime"]
SCORE_YOY_COLS = ["deep-subprime","subprime","near-prime","prime","super-prime"]
SCORE_YOY_JSON = ["Deep subprime","Subprime","Near-prime","Prime","Super-prime"]

# Fixes input text to follow agency guidelines
TEXT_FIXES = {"30 - 44": "Age 30-44",
              "45 - 64": "Age 45-64",
              "65 and older": "Age 65 and older",
              "Deep Subprime": "Deep subprime",
              "Near Prime": "Near-prime",
              "Superprime":"Super-prime",
             }

# Output: "month","date","vol","vol_unadj","<grouptype>_group"
GROUP_VOL_OUTPUT_SCHEMA = ["month","date","vol","vol_unadj","{}_group"]

# Market names - these become directory names
MARKET_NAMES = {"AUT": "auto-loans",     # Auto loans
                "CRC": "credit-cards",   # Credit cards
                "HCE": "heces",          # Home Equity, Closed-End
                "HLC": "helocs",         # Home Equity Line of Credit (HELOC)
                "MTG": "mortgages",      # Mortgages
                "PER": "personal-loans", # Personal loans
                "RET": "retail-loans",   # Retail loans
                "STU": "student-loans",  # Student loans
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

## Methods

def save_csv(filename, content, writemode='wb'):
    """Saves the specified content object into a csv file."""
    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))
        print("Created directories for {}".format(os.path.dirname(filename)))

    # Write output as a csv file
    with open(filename, writemode) as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerows(content)

    return True


def save_json(filename, content, writemode='wb'):
    """Dumps the specified JSON content into a .json file"""
    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))
        print("Created directories for {}".format(os.path.dirname(filename)))

    # Write output as a json file
    import json
    with open(filename, writemode) as fp:
        fp.write(json.dumps(content,
                            sort_keys=True,
                            indent=4,
                            separators=(',', ': ')))

    return True


def load_csv(filename, skipheaderrow=True):
    """Loads CSV data from a file"""
    with open(filename, 'rb') as csvfile:
        reader = csv.reader(csvfile)
        data = list(reader)

    if skipheaderrow:
        return data[1:]
    else:
        return data


def load_paths(inputpath=DEFAULT_INPUT_FOLDER, outputpath=DEFAULT_OUTPUT_FOLDER):
    """Loads the root path and destination paths and performs path checking"""
    inpath = expand_path(inputpath)
    outpath = expand_path(outputpath)

    return inpath, outpath


def expand_path(path):
    rootpath = os.path.abspath(os.path.expanduser(path))

    return rootpath


def get_csv_list(path):
    """Loads a list of files in the specified directory"""
    files = [f for f in os.listdir(path)
             if f.lower().endswith('.csv') 
             and os.path.isfile(os.path.join(path, f))]

    return files


def find_market(input, possible_names=MARKET_NAMES):
    """Uses the input string and a specified dictionary of market names to
    determine which credit market the input string describes."""
    for abbr, name in possible_names.items():
        if abbr in input:
            return name

    return None


def actual_date(month, schema=DATE_SCHEMA):
    """
    Takes a month number and computes an actual date from it.
    January 2000 = month zero
    """
    addl_years = int(month/12)
    addl_months = (month % 12) + 1  # offset for January, as month input is 1-12

    date = datetime.date(BASE_YEAR + addl_years, addl_months, 1)

    return date.strftime(schema)


# Unix Epoch conversion from
# http://stackoverflow.com/questions/11743019/convert-python-datetime-to-epoch-with-strftime
def epochtime(datestring, schema=DATE_SCHEMA):
    """Converts a date string from specified schema to seconds since J70/Unix epoch"""

    date = datetime.datetime.strptime(datestring, schema)

    return int(round((date - datetime.datetime(1970, 1, 1)).total_seconds()))


# Modified from an answer at:
# http://stackoverflow.com/questions/3154460/python-human-readable-large-numbers
def human_numbers(num, decimal_places=1, whole_units_only=1):
    """Given a number, returns a human-modifier (million/billion) number
    Number returned will be to the specified number of decimal places with modifier
    (default: 1) - e.g. 1100000 returns '1.1 million'.
    If whole_units_only is specified, no parts less than one unit will
    be displayed, i.e. 67.012 becomes 67. This has no effect on numbers with modifiers."""
    numnames = ['', '', 'million', 'billion', 'trillion', 'quadrillion', 'quintillion']
    # TODO: Insert commas every 3 if not over millions

    n = float(num)
    idx = max(0,min(len(numnames) - 1,
                    int(math.floor(0 if n == 0 else math.log10(abs(n))/3))))

    # Create the output string with the requested number of decimal places
    # This has to be a separate step from the format() call because otherwise
    # format() gets called on the final fragment only
    outstr = '{:,.' + str(decimal_places) + 'f} {}'

    # Insert commas every 3 numbers if not over millions and only whole units chosen
    if idx < 2:
        if whole_units_only:
            return '{:,}'.format(int(round(n)))
        else:
            return outstr.format(n, numnames[idx]).strip()

    # Calculate the output number by order of magnitude
    outnum = n / 10**(3 * idx)

    return outstr.format(outnum, numnames[idx])


## Program flow

def process_data_files(inputpath,
                       outputpath,
                       data_snapshot_fname=SNAPSHOT_FNAME_KEY,
                       data_snapshot_outname=SNAPSHOT_FNAME_HTML,
                       report_success=False,
                       report_failure=False):
    """Processes raw csv data from the Office of Research"""
    # Get a list of files in the raw data directory
    inputfiles = get_csv_list(inputpath)
    successes = []
    failures  = []

    # For each file, open and munge data
    for filename in inputfiles:
        filepath = os.path.join(inputpath, filename)
        # Check for market in filename
        market = find_market(filename)

        if market is None:
            if data_snapshot_fname not in filename:
                print("Found file '{}' does not specify market".format(filename))
                failures.append(filename)
                continue

            # Check/process Data Snapshot file into HTML snippets
            snippets = process_data_snapshot(filepath)
            successes.append(filename)

            # Determine output directory
            for market, snippet in snippets.iteritems():
                outpath = os.path.join(outputpath, market, data_snapshot_outname)

                # print("{} snapshot snippet saving to {}".format(market, outpath))
                if not os.path.exists(os.path.dirname(outpath)):
                    os.makedirs(os.path.dirname(outpath))
                
                with open(outpath, 'w') as outfile:
                    outfile.write(snippet)

        else:
            # Run file per market-type
            try:
                cond, data, json = FILE_PREFIXES[filename[:MKT_SFX_LEN].lower()](filepath)
            except ValueError, e:
                print("Error occurred during {}".format(filename))
                raise e

            if cond:
                # Determine output directory
                outpath = os.path.join(outputpath, market, filename)
                if len(data) > 0:
                    cond = save_csv(outpath, data)
                    cond &= save_json(outpath.replace(".csv", ".json"), json)
                
                if cond:
                    successes.append(filename)
                else:
                    failures.append(filename)

            else:
                failures.append(filename)

    # Processing complete - perform reporting
    if len(successes) > 0 and report_success:
        print("** Successfully processed files:\n{}\n".format("\n".join(successes)))

    if len(failures) > 0 and report_failure:
        print("** Failed to process files:\n{}\n".format("\n".join(failures)))

    return len(successes), len(inputfiles)


## Process state-by-state map files

def process_map(filename, output_schema=MAP_OUTPUT_SCHEMA):
    """Processes specified map file and outputs data per the schema"""
    # Input  columns: "state","value"
    # Output columns: "fips_code","state_abbr","value"

    # Load specified file as input data
    inputdata = load_csv(filename)

    # Initialize output data with column headers
    data = [output_schema]

    # Process data
    # TODO: Add error handling for unsupported FIPS codes
    # TODO: Make sure all 50 states (or other expected data) is represented
    for row in inputdata:
        data.append([row[0], FIPS_CODES[int(row[0])], row[1]])

    # Check if data exists and JSON-format
    if len(data) > 1:
        json = json_for_tile_map(data[1:])
        return True, data, json
    
    return True, [], []


## Process summary files with loan numbers or volumes

def process_num_summary(filename):
    """Helper function that calls process_file_summary with correct output schema"""
    # Output columns: "month","date","num","num_unadj"
    return process_file_summary(filename, SUMMARY_NUM_OUTPUT_SCHEMA)


def process_vol_summary(filename):
    """Helper function that calls process_file_summary with correct output schema"""
    # Output columns: "month","date","vol","vol_unadj"
    return process_file_summary(filename, SUMMARY_VOL_OUTPUT_SCHEMA)


def process_file_summary(filename, output_schema):
    """Processes specified summary file and outputs data per the schema"""
    # print("Processing summary file '{}'".format(filename))

    # Load specified file as input data
    inputdata = load_csv(filename)

    # Initialize output data with column headers
    data = []
    proc = {}

    # Process data
    for row in inputdata:
        monthstr, value, is_adj_str = row
        monthnum = int(monthstr)
        if not proc.has_key(monthnum):
            proc[monthnum] = {"adj": None, "unadj": None}

        if "unadjust" in is_adj_str.lower():
            proc[monthnum]["unadj"] = value
        elif "seasonal" in is_adj_str.lower():
            proc[monthnum]["adj"] = value
        else:
            raise TypeError("Data row (below) does not specify seasonal " +
                            "adjustment in {}\n{}".format(
                            filename, ",".join(row)))

    # Turn dictionaries into a data list for output
    # This order MUST match the provided schema order
    for monthnum, value in proc.items():
        data.append([monthnum,
                     actual_date(monthnum),
                     value["adj"],
                     value["unadj"]])

    # Prep for output by sorting (by month number) and inserting a header
    data.sort()
    data.insert(0, output_schema)

    # Check if data exists and JSON-format
    if len(data) > 1:
        json = json_for_line_chart(data[1:])
        return True, data, json
    
    return True, [], []


## Process volume files with groups (borrower age, income level, credit score)
# Output columns: "month","date","volume","volume_unadj","<type>_group"

def process_group_age_vol(filename):
    """Helper function that calls process_group_file with correct
    group and output schema"""

    schema = list(GROUP_VOL_OUTPUT_SCHEMA)
    schema[-1] = schema[-1].format(AGE)

    return process_group_file(filename, schema)


def process_group_income_vol(filename):
    """Helper function that calls process_group_file with correct
    group and output schema"""

    schema = list(GROUP_VOL_OUTPUT_SCHEMA)
    schema[-1] = schema[-1].format(INCOME)

    return process_group_file(filename, schema)


def process_group_score_vol(filename):
    """Helper function that calls process_group_file with correct
    group and output schema"""

    schema = list(GROUP_VOL_OUTPUT_SCHEMA)
    schema[-1] = schema[-1].format(SCORE)

    return process_group_file(filename, schema)


def process_group_file(filename, output_schema):
    """Processes specified group volume file and outputs data per the schema"""

    # Load specified file as input data
    inputdata = load_csv(filename)

    # Initialize output data with column headers
    data = []
    proc = {}

    # Process data
    for row in inputdata:
        monthstr, value, group, is_adj_str = row
        monthnum = int(monthstr)
        if not proc.has_key(monthnum):
            proc[monthnum] = {}

        if not proc[monthnum].has_key(group):
            proc[monthnum][group] = {"adj": None, "unadj": None}

        if "unadjust" in is_adj_str.lower():
            proc[monthnum][group]["unadj"] = value
        elif "seasonal" in is_adj_str.lower():
            proc[monthnum][group]["adj"] = value
        else:
            raise TypeError("Data row (below) does not specify seasonal " +
                            "adjustment in {}\n{}".format(
                            filename, ",".join(row)))

    # Turn dictionaries into a data list for output
    # This order MUST match the provided schema order
    for monthnum, group in proc.items():
        for groupname, value in group.items():
            # Parse for any text fixes required
            if groupname in TEXT_FIXES.keys():
                data.append([monthnum,
                             actual_date(monthnum),
                             value["adj"],
                             value["unadj"],
                             TEXT_FIXES[groupname]])
            else:
                data.append([monthnum,
                             actual_date(monthnum),
                             value["adj"],
                             value["unadj"],
                             groupname])

    # Prep for output by sorting (by month number) and inserting a header
    data.sort()
    data.insert(0, output_schema)

    # Check if data exists and JSON-format
    if len(data) > 1:
        json = json_for_group_line_chart(data[1:])
        return True, data, json
    
    return True, [], []


## Process year-over-year files with groups (borrower age, income level, credit score)
# Output columns: "month","date","yoy_<type>","yoy_<type>",...,"yoy_<type>"

def process_group_age_yoy(filename):
    """Helper function that calls process_group_yoy_groups with correct
    group and output schema"""
    # Generate output schema from group YOY column names
    postfix = "{}_yoy"
    output_schema = list(GROUP_YOY_OUTPUT_SCHEMA)
    output_schema += [postfix.format(gname) for gname in AGE_YOY_COLS]

    cond, data = process_group_yoy_groups(filename, AGE_YOY_IN, output_schema)

    # Format for JSON
    json = []
    if len(data) > 1:
        json = json_for_group_bar_chart(data[1:], AGE_YOY_COLS, AGE_YOY_JSON)


    return cond, data, json


def process_group_income_yoy(filename):
    """Helper function that calls process_group_yoy_groups with correct
    group and output schema"""
    # Generate output schema from group YOY column names
    postfix = "{}_yoy"
    output_schema = list(GROUP_YOY_OUTPUT_SCHEMA)
    output_schema += [postfix.format(gname) for gname in INCOME_YOY_COLS]

    cond, data = process_group_yoy_groups(filename, INCOME_YOY_IN, output_schema)

    # Format for JSON
    json = []
    if len(data) > 1:
        json = json_for_group_bar_chart(data[1:], INCOME_YOY_COLS, INCOME_YOY_JSON)

    return cond, data, json


def process_group_score_yoy(filename):
    """Helper function that calls process_group_yoy_groups with correct
    group and output schema"""
    # Generate output schema from group YOY column names
    postfix = "{}_yoy"
    output_schema = list(GROUP_YOY_OUTPUT_SCHEMA)
    output_schema += [postfix.format(gname) for gname in SCORE_YOY_COLS]

    cond, data = process_group_yoy_groups(filename, SCORE_YOY_IN, output_schema)

    # Format for JSON
    json = []
    if len(data) > 1:
        json = json_for_group_bar_chart(data[1:], SCORE_YOY_COLS, SCORE_YOY_JSON)

    return cond, data, json


def process_group_yoy_groups(filename, group_names, output_schema):
    """Processes specified group year-over-year file and outputs data per the schema"""

    # Load specified file as input data
    inputdata = load_csv(filename)

    # Initialize output data with column headers
    data = []
    proc = {}

    # Process data
    for row in inputdata:
        monthstr, value, group = row
        monthnum = int(monthstr)
        
        if not proc.has_key(monthnum):
            proc[monthnum] = {gname: None for gname in group_names}

        if group in group_names:
            proc[monthnum][group] = value
        else:
            raise TypeError("Data row (below) contains illegal group " +
                            "name '{}'\n{}".format(filename, ",".join(row)))

    # Turn dictionaries into a data list for output
    for monthnum, values in proc.items():
        data.append([monthnum, actual_date(monthnum)] +
                    [values[gname] for gname in group_names])

    # Prep for output by sorting (by month number) and inserting a header
    data.sort()
    data.insert(0, output_schema)

    # Check if data exists and JSON-format
    # Unlike other methods, the individual group calls handle the JSON
    if len(data) > 1:
        return True, data
    
    return True, []


## Process summary year-over-year files

def process_yoy_summary(filename, output_schema=YOY_SUMMARY_OUTPUT_SCHEMA):
    """Processes specified summary file and outputs data per the schema"""
    # Output columns: "month","date","yoy_num","yoy_vol"
    # print("Processing summary file '{}'".format(filename))

    # Load specified file as input data
    inputdata = load_csv(filename)

    # Initialize output data
    data = []
    proc = {}

    # Process data
    for row in inputdata:
        monthstr, value, type_str = row
        monthnum = int(monthstr)
        if not proc.has_key(monthnum):
            proc[monthnum] = {"num": None, "vol": None}

        # Input column "group" is "Dollar Volume" or "Number of Loans"
        if "number" in type_str.lower():
            proc[monthnum]["num"] = value
        elif "volume" in type_str.lower():
            proc[monthnum]["vol"] = value
        else:
            raise TypeError("YOY Summary Data row (below) improperly " +
                            "formatted in {}\n{}".format(filename, row))

    # Turn dictionaries into a data list for output
    # This order MUST match the provided schema order
    for monthnum, value in proc.items():
        data.append([monthnum,
                     actual_date(monthnum),
                     value["num"],
                     value["vol"]])

    # Prep for output by sorting (by month number) and inserting a header
    data.sort()
    data.insert(0, output_schema)

    # Check if data exists and JSON-format
    if len(data) > 1:
        json = json_for_bar_chart(data[1:])
        return True, data, json
    
    return True, [], []


## JSON Output calls

def json_for_bar_chart(data):
    """Takes input data and returns formatted values for dumping to a JSON file """
    
    outnum = []
    outvol = []

    for month, date, yoy_num, yoy_vol in data:
        sec = epochtime(date)
        try:
            outnum.append([sec * SEC_TO_MS, float(yoy_num)])
            outvol.append([sec * SEC_TO_MS, float(yoy_vol)])
        except ValueError:
            continue

    return {"Number of Loans": outnum, "Dollar Volume": outvol}


def json_for_group_bar_chart(data, val_cols, out_names):
    """Takes input data and returns formatted values for dumping to a JSON file """
    
    tmp = {}
    for col in val_cols:
        tmp[col] = []

    # Group bar charts (yoy) have variable numbers of columns depending on the groups
    for row in data:
        sec = epochtime(row[1])
        for colnum in range(len(val_cols)):
            try:
                tmp[val_cols[colnum]].append([sec * SEC_TO_MS, float(row[2+colnum])])
            except ValueError:
                continue

    out = {}

    # Translate into JSON output columns
    for col_key in tmp.keys():
        idx = val_cols.index(col_key)
        if idx < 0:
            raise IndexError("Key '{}' does not exist in {}".format(col_key, val_cols))
        out[out_names[idx]] = tmp[col_key][:]
    
    return out


def json_for_line_chart(data):
    """Takes input data and returns formatted values for dumping to a JSON file """
    
    out = {"adjusted": [], "unadjusted": []}

    for monthnum, date, val, val_unadj in data:
        sec = epochtime(date)
        try:
            out["adjusted"].append([sec * SEC_TO_MS, float(val)])
            out["unadjusted"].append([sec * SEC_TO_MS, float(val_unadj)])
        except ValueError:
            continue

    return out
  

def json_for_group_line_chart(data):
    """Takes input data and returns formatted values for dumping to a JSON file"""

    # TODO: Maybe use the known global key groups to init groupname dicts once
    out = {}

    # Group line charts (vol/num) have the group name in the last column
    for month, date, val, val_unadj, groupname in data:
        sec = epochtime(date)

        # JSON fix for age groups - strip off the "Age "
        if groupname.lower().find("age ") == 0:
            groupname = groupname[4:]

        # Initialize if first time groupname is encountered
        if groupname not in out.keys():
            out[groupname] = {"adjusted": [], "unadjusted": []}

        try:
            out[groupname]["adjusted"].append([sec * SEC_TO_MS, float(val)])
            out[groupname]["unadjusted"].append([sec * SEC_TO_MS, float(val_unadj)])
        except ValueError:
            # Discard "NA" values and other non-float-able values
            continue

    return out


def json_for_tile_map(data):
    """Takes input data and returns a list of dicts of state names and percentages
    for dumping to a JSON file:
    Input is a list of lists: [[FIPS code, state abbr, percentages],...]
    Output is list of dicts: [{"name": abbr, "value": percentage},...]
    """

    out = []
    
    for code, state, value in data:
        try:
            value = "{:0.2f}".format(float(value) * 100)
        except ValueError:
            # Leave as NA for states if found
            pass
          
        out.append({"name": state, "value": value})

    return out


## Process data snapshot into separate HTML snippets

def process_data_snapshot(filepath):
    """Process a file that contains data snapshot information for
    all markets"""

    # Load specified file as input data
    inputdata = load_csv(filepath)

    return process_snapshot_inputdata(inputdata)


def process_snapshot_inputdata(inputdata):
    # Initialize output data with column headers
    data = {}

    for row in inputdata:
        market, monthnum, orig, vol, yoy = row
        monthnum = int(monthnum)
        orig = float(orig)
        vol = float(vol)
        yoy = float(yoy)

        # Determine market and month
        output_mkt = find_market(market)
        month = actual_date(monthnum, schema="%B %Y")

        # Retrieve snapshot descriptors
        orig_desc, vol_desc = HTML_MKT_NAMES[market]

        # Parse numbers
        orig_fmt = human_numbers(orig, whole_units_only=1).replace(" ", "&nbsp;")
        vol_fmt = human_numbers(vol).replace(" ", "&nbsp;")
        yoy_fmt = "{:.1f}".format(abs(yoy))
        yoy_desc = HTML_DESC[yoy > 0]

        # Insert into output snippet
        out_html = SNAPSHOT_HTML.format(
                                        orig_fmt, orig_desc,
                                        vol_fmt, vol_desc,
                                        yoy_fmt, yoy_desc
                                        )
        # Save snippet by market
        data[output_mkt] = out_html
    
    return data


###########################################################
# General program flow
###########################################################
# 0. Set up paths and parse args
# 1. Get list of data files in input directory
# 2. Open each file and munge the data as specified by type
# 3. Save each file into the appropriate output directory
# 
###########################################################

# Filenames are formatted as:
# "<prefix>_<market>.csv" 
# NOTE: This global set must come after the methods are defined
FILE_PREFIXES = {"map_data":                process_map,
                 "num_data":                process_num_summary,
                 "vol_data":                process_vol_summary,
                 "volume_data_age_group":   process_group_age_vol,
                 "volume_data_income_level":process_group_income_vol,
                 "volume_data_score_level": process_group_score_vol,
                 "yoy_data_all":            process_yoy_summary,
                 "yoy_data_age_group":      process_group_age_yoy,
                 "yoy_data_income_level":   process_group_income_yoy,
                 "yoy_data_score_level":    process_group_score_yoy,
                 }


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Processes data files from the Office of Research.')
    parser.add_argument('-i', '--input-path', metavar="INPUTDIR", type=str,
                        dest='inputdir', default=DEFAULT_INPUT_FOLDER,
                        help='Specifies path for folder containing input data files (default: "")')
    parser.add_argument('-o', '--output-path', metavar="OUTPUTDIR", type=str,
                        dest='outputdir', default=DEFAULT_OUTPUT_FOLDER,
                        help='Specifies path for root folder to put processed data files (default: "")')

    args = parser.parse_args()

    # Parse the given paths
    inputdir, outputdir = load_paths(args.inputdir, args.outputdir)

    # Process the data
    successes, numfiles = process_data_files(inputdir, outputdir)
    print("** Processed {} of {} input data files successfully".format(successes, numfiles))
