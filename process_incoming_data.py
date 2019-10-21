#!/usr/bin/env python
"""
Processes incoming data from the Office of Research and munges it into
the output formats expected by the CFPB chart display organisms.

Output formats are documented at
www.github.com/cfpb/consumer-credit-trends
"""

# Python library imports
import os
import datetime
import logging

# Local imports
import process_globals as cfg
import process_utils as util


__author__ = "Consumer Financial Protection Bureau"
__credits__ = ["Hillary Jeffrey"]
__license__ = "CC0-1.0"
__version__ = "2.0"
__maintainer__ = "CFPB"
__email__ = "tech@cfpb.gov"
__status__ = "Development"

# Constants
# Market+.csv filename suffix length
MKT_SFX_LEN = -8


# Set up logging
logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)


# Utility Methods
# Generalized utility methods are found in process_utils.py

def load_paths(inpath=cfg.DEFAULT_INPUT_FOLDER,
               outpath=cfg.DEFAULT_OUTPUT_FOLDER):
    """Loads the root path and destination paths and performs path checking"""
    inpath = util.expand_path(inpath)
    outpath = util.expand_path(outpath)

    return inpath, outpath


def find_market(input, possible_names=cfg.MARKET_NAMES):
    """Uses the input string and a specified dictionary of market names to
    determine which credit market the input string describes."""
    for abbr, name in possible_names.items():
        if abbr in input:
            return name

    return None


def actual_date(month, schema=cfg.DATA_FILE_DATE_SCHEMA):
    """
    Takes a month number from Office of Research files and computes
    a date from it.
    January 2000 = month zero
    """
    addl_years = int(month/12)
    addl_months = (month % 12) + 1  # offset for January: month input is 1-12

    date = datetime.date(cfg.BASE_YEAR + addl_years, addl_months, 1)

    return date.strftime(schema)


# Main program functionality

def process_data_files(inputpath,
                       outputpath,
                       data_snapshot_fname=cfg.SNAPSHOT_FNAME_KEY,
                       data_snapshot_path=''):
    """Processes raw csv data from the Office of Research"""
    inputfiles = util.get_csv_list(inputpath)
    logger.debug("Found files:\n{}".format(inputfiles))
    logger.info(
        "Found {} csv files in '{}'".format(
            len(inputfiles),
            inputpath
        )
    )

    if len(inputfiles) == 0:
        logger.warning("No csv data files found in {}".format(inputpath))
        return []

    successes = []
    failures = []

    # For each file, open and munge data
    for filename in inputfiles:
        filepath = os.path.join(inputpath, filename)
        # Check for market in filename
        market = find_market(filename)

        if market is None:
            if data_snapshot_fname in filename:
                if len(data_snapshot_path) <= 0:
                    logger.warning(
                        "Data snapshot output path is not specified."
                    )
                    logger.warning(
                        "To process data snapshot file, specify " +
                        "the --data-snapshot-path command-line " +
                        "argument."
                    )
                    continue

                # Check/process Data Snapshot file into human-readable snippets
                snapshots = process_data_snapshot(filepath)

                # Generate output dictionary
                today = datetime.datetime.today()
                logger.info(
                    "Date published is {}".format(
                        today.strftime(cfg.SNAPSHOT_DATE_SCHEMA)
                    )
                )
                content_updates = {
                    'date_published': today.strftime(cfg.SNAPSHOT_DATE_SCHEMA),
                    'markets': snapshots
                }

                # Save data snapshot info as JSON
                data_snapshot_path = util.expand_path(data_snapshot_path)

                if not os.path.exists(os.path.dirname(data_snapshot_path)):
                    os.makedirs(os.path.dirname(data_snapshot_path))

                util.save_json(data_snapshot_path, content_updates)

                logger.info(
                    "Saved output data snapshot information to '{}'".format(
                        data_snapshot_path
                    )
                )

                successes.append(filename)

            # Doesn't match an expected filename; may not be a CCT file
            else:
                logger.info(
                    "Ignoring file '{}' as not CCT related".format(filename)
                )
                failures.append(filename)
                continue

        else:
            # Run file per market-type
            try:
                current_prefix = filename[:MKT_SFX_LEN].lower()
                cond, data, json = FILE_PREFIXES[current_prefix](filepath)
            except ValueError as e:
                logger.error("Error occurred during {}".format(filename))
                raise e

            if cond:
                # Determine output directory
                outpath = os.path.join(outputpath, market, filename)
                if len(data) > 0:
                    util.save_csv(outpath, data)
                    util.save_json(outpath.replace(".csv", ".json"), json)

                successes.append(filename)

            else:
                failures.append(filename)

    # Summarize processing statistics
    logger.info(
        "** Processed {} input data files successfully".format(
            len(successes)
        )
    )

    if len(failures) > 0:
        logger.warning(
            "** Unable to process {} input data files".format(
                len(failures)
            )
        )

    return


# Process state-by-state map files

def process_map(filename, output_schema=cfg.MAP_OUTPUT_SCHEMA):
    """Processes specified map file and outputs data per the schema"""
    # Input  columns: "state", "value"
    # Output columns: "fips_code", "state_abbr", "value"

    # Load specified file as input data
    inputdata = util.load_csv(filename)

    # Initialize output data with column headers
    data = [output_schema]

    # Process data
    # TODO: Add error handling for unsupported FIPS codes
    # TODO: Make sure all 50 states (or other expected data) is represented
    for row in inputdata:
        data.append([row[0], cfg.FIPS_CODES[int(row[0])], row[1]])

    # Check if data exists and JSON-format
    if len(data) > 1:
        json = json_for_tile_map(data[1:])
        return True, data, json

    return True, [], []


# Process inquiry index file
def process_inquiry_index(filename):
    """Call proess_file_summary on the specified inquiry file and
    returns output data and json per the output_schema"""
    logger.debug("Running process_inquiry_index")
    return process_file_summary(filename, cfg.INQUIRY_INDEX_OUTPUT_SCHEMA)


# Process inferred credit tightness index file
def process_tightness_index(filename):
    """Processes specified credit tightness file and
    returns output data and json per the output_schema"""
    logger.debug("Running process_tightness_index")
    return process_file_summary(filename, cfg.TIGHTNESS_INDEX_OUTPUT_SCHEMA)


# Process summary files with loan numbers or volumes

def process_num_summary(filename):
    """Calls process_file_summary with correct output schema"""
    # Output columns: "month", "date", "num", "num_unadj"
    return process_file_summary(filename, cfg.SUMMARY_NUM_OUTPUT_SCHEMA)


def process_vol_summary(filename):
    """Calls process_file_summary with correct output schema"""
    # Output columns: "month", "date", "vol", "vol_unadj"
    return process_file_summary(filename, cfg.SUMMARY_VOL_OUTPUT_SCHEMA)


def process_file_summary(filename, output_schema):
    """Processes specified summary file and outputs data per the schema"""

    # Load specified file as input data
    try:
        inputdata = util.load_csv(filename)
    except Exception as e:
        logger.error("Make sure you are running Python 2.x!".format(filename))
        raise e

    # Process data
    proc = {}
    for row in inputdata:
        monthstr, value, is_adj_str = row
        monthnum = int(monthstr)
        if monthnum not in proc:
            proc[monthnum] = {"adj": None, "unadj": None}

        if "unadjust" in is_adj_str.lower():
            proc[monthnum]["unadj"] = value
        elif "seasonal" in is_adj_str.lower():
            proc[monthnum]["adj"] = value
        else:
            msg = "Data row (below) does not specify seasonal adjustment " + \
                "in {}\n{}".format(
                    filename, ",".join(row)
                )
            logger.error(msg)
            raise TypeError(msg)

    # Turn dictionaries into a data list for output
    # This order MUST match the provided schema order
    data = []
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


# Process volume files with groups (borrower age, income level, credit score)
# Output columns: "month", "date", "volume", "volume_unadj", "<type>_group"

def process_group_age_vol(filename):
    """Calls process_group_file with correct
    group and output schema"""

    schema = list(cfg.GROUP_VOL_OUTPUT_SCHEMA)
    schema[-1] = schema[-1].format(cfg.AGE)

    return process_group_file(filename, schema)


def process_group_income_vol(filename):
    """Calls process_group_file with correct group and output schema"""

    schema = list(cfg.GROUP_VOL_OUTPUT_SCHEMA)
    schema[-1] = schema[-1].format(cfg.INCOME)

    return process_group_file(filename, schema)


def process_group_score_vol(filename):
    """Calls process_group_file with correct
    group and output schema"""

    schema = list(cfg.GROUP_VOL_OUTPUT_SCHEMA)
    schema[-1] = schema[-1].format(cfg.SCORE)

    return process_group_file(filename, schema)


def process_group_file(filename, output_schema):
    """Processes specified group volume file and outputs data per the schema"""

    # Load specified file as input data
    inputdata = util.load_csv(filename)

    # Initialize output data with column headers
    data = []
    proc = {}

    # Process data
    for row in inputdata:
        monthstr, value, group, is_adj_str = row
        monthnum = int(monthstr)
        if monthnum not in proc:
            proc[monthnum] = {}

        if group not in proc[monthnum]:
            proc[monthnum][group] = {"adj": None, "unadj": None}

        if "unadjust" in is_adj_str.lower():
            proc[monthnum][group]["unadj"] = value
        elif "seasonal" in is_adj_str.lower():
            proc[monthnum][group]["adj"] = value
        else:
            msg = "Data row (below) does not specify seasonal adjustment " + \
                "in {}\n{}".format(
                    filename,
                    ",".join(row)
                )
            logger.error(msg)
            raise TypeError(msg)

    # Turn dictionaries into a data list for output
    # This order MUST match the provided schema order
    for monthnum, group in proc.items():
        for groupname, value in group.items():
            # Parse for any text fixes required
            if groupname in cfg.TEXT_FIXES:
                data.append([monthnum,
                             actual_date(monthnum),
                             value["adj"],
                             value["unadj"],
                             cfg.TEXT_FIXES[groupname]])
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


# Process year-over-year files with groups
# (i.e. borrower age, income level, credit score)
# Output columns: "month", "date", "yoy_<type>", ... , "yoy_<type>"

def process_group_age_yoy(filename):
    """Calls process_group_yoy_groups with correct group and output schema"""
    postfix = "{}_yoy"
    output_schema = list(cfg.GROUP_YOY_OUTPUT_SCHEMA)
    output_schema += [postfix.format(gname) for gname in cfg.AGE_YOY_COLS]

    cond, data = process_group_yoy_groups(
        filename,
        cfg.AGE_YOY_IN,
        output_schema
    )

    # Format for JSON
    json = []
    if len(data) > 1:
        json = json_for_group_bar_chart(
            data[1:],
            cfg.AGE_YOY_COLS,
            cfg.AGE_YOY_JSON
        )

    return cond, data, json


def process_group_income_yoy(filename):
    """Calls process_group_yoy_groups with correct group and output schema"""
    # Generate output schema from group YOY column names
    postfix = "{}_yoy"
    output_schema = list(cfg.GROUP_YOY_OUTPUT_SCHEMA)
    output_schema += [postfix.format(gname) for gname in cfg.INCOME_YOY_COLS]

    cond, data = process_group_yoy_groups(
        filename,
        cfg.INCOME_YOY_IN,
        output_schema
    )

    # Format for JSON
    json = []
    if len(data) > 1:
        json = json_for_group_bar_chart(
            data[1:],
            cfg.INCOME_YOY_COLS,
            cfg.INCOME_YOY_JSON
        )

    return cond, data, json


def process_group_score_yoy(filename):
    """Calls process_group_yoy_groups with correct group and output schema"""
    # Generate output schema from group YOY column names
    postfix = "{}_yoy"
    output_schema = list(cfg.GROUP_YOY_OUTPUT_SCHEMA)
    output_schema += [postfix.format(gname) for gname in cfg.SCORE_YOY_COLS]

    cond, data = process_group_yoy_groups(
        filename,
        cfg.SCORE_YOY_IN,
        output_schema
    )

    # Format for JSON
    json = []
    if len(data) > 1:
        json = json_for_group_bar_chart(
            data[1:],
            cfg.SCORE_YOY_COLS,
            cfg.SCORE_YOY_JSON
        )

    return cond, data, json


def process_group_yoy_groups(filename, group_names, output_schema):
    """Processes specified group year-over-year file and outputs data
    per the provided output schema"""

    # Load specified file as input data
    inputdata = util.load_csv(filename)

    # Initialize output data with column headers
    data = []
    proc = {}

    # Process data
    for row in inputdata:
        monthstr, value, group = row
        monthnum = int(monthstr)

        if monthnum not in proc:
            proc[monthnum] = {name: None for name in group_names}

        if group in group_names:
            proc[monthnum][group] = value
        else:
            msg = "Data row (below) contains illegal group " + \
                  "name '{}'\n{}".format(filename, ",".join(row))
            logger.error(msg)
            raise TypeError(msg)

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


def process_yoy_summary(filename, output_schema=cfg.YOY_SUMMARY_OUTPUT_SCHEMA):
    """Processes specified year-over-year summary file and outputs data
    per the provided output schema"""
    # Output columns: "month", "date", "yoy_num", "yoy_vol"

    # Load specified file as input data
    inputdata = util.load_csv(filename)

    # Initialize output data
    data = []
    proc = {}

    # Process data
    for row in inputdata:
        monthstr, value, type_str = row
        monthnum = int(monthstr)
        if monthnum not in proc:
            proc[monthnum] = {"num": None, "vol": None}

        # Input column "group" is "Dollar Volume" or "Number of Loans"
        if "number" in type_str.lower():
            proc[monthnum]["num"] = value
        elif "volume" in type_str.lower():
            proc[monthnum]["vol"] = value
        elif "inquiry" in type_str.lower():
            # Ignore 'Inquiry Index' entries in current output
            pass
        elif "tightness" in type_str.lower():
            # Ignore 'Credit Tightness Index' entries in current output
            pass
        else:
            msg = "YOY Summary Data row (below) improperly " + \
                  "formatted in {}\n{}".format(filename, row)
            logger.error(msg)
            raise TypeError(msg)

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


# JSON output processing
def json_for_bar_chart(data):
    """Takes input data and returns formatted values for a JSON file"""

    outnum = []
    outvol = []

    for month, date, yoy_num, yoy_vol in data:
        sec = util.epochtime(date, schema=cfg.DATA_FILE_DATE_SCHEMA)
        try:
            outnum.append([util.milliseconds(sec), float(yoy_num)])
            outvol.append([util.milliseconds(sec), float(yoy_vol)])
        except ValueError:
            logger.debug(
                "Ignore ValueError: Discard 'NA' and other non-float values"
            )
            continue
        except TypeError as e:
            logger.warning(
                "Missing value as '{}' in data\n'{}'".format(
                    yoy_num,
                    repr(data)
                )
            )
            # TODO: Raise error with data so filename can be determined
            continue

    return {"Number of Loans": outnum, "Dollar Volume": outvol}


def json_for_group_bar_chart(data, val_cols, out_names):
    """Takes input data and returns formatted values for a JSON file """

    tmp = {}
    for col in val_cols:
        tmp[col] = []

    # Group bar charts (yoy) have a variable numbers of columns by groups
    for row in data:
        sec = util.epochtime(row[1])
        for colnum in range(len(val_cols)):
            try:
                tmp_col = val_cols[colnum]
                tmp[tmp_col].append(
                    [util.milliseconds(sec), float(row[2+colnum])]
                )
            except ValueError:
                logger.debug(
                    "Ignore ValueError: Discard 'NA' and other " +
                    "non-float values"
                )
                continue
            except TypeError as e:
                logger.warning(
                    "Missing value as '{}' in row\n'{}'".format(
                        row[2+colnum],
                        repr(row)
                    )
                )
                # TODO: Raise error with data so filename can be determined
                continue

    out = {}

    # Translate into JSON output columns
    for col_key in tmp:
        idx = val_cols.index(col_key)
        if idx < 0:
            msg = "Key '{}' does not exist in {}".format(col_key, val_cols)
            logger.error(msg)
            raise IndexError(msg)
        out[out_names[idx]] = tmp[col_key][:]

    return out


def json_for_line_chart(data):
    """Takes input data and returns formatted values for a JSON file """

    out = {"adjusted": [], "unadjusted": []}

    for monthnum, date, v_adj, v_unadj in data:
        sec = util.epochtime(date)
        try:
            out["adjusted"].append([util.milliseconds(sec), float(v_adj)])
            out["unadjusted"].append([util.milliseconds(sec), float(v_unadj)])
        except ValueError:
            logger.debug(
                "Ignore ValueError: Discard 'NA' and other non-float values"
            )
            continue
        except TypeError as e:
            logger.warning(
                "Missing value as '{}' in row\n'{}'".format(
                    row[2+colnum],
                    repr(row)
                )
            )
            # TODO: Raise error with data so filename can be determined
            continue

    return out


def json_for_group_line_chart(data):
    """Takes input data and returns formatted values for to a JSON file"""
    # TODO: Maybe use the known global key groups to init groupname dicts once
    out = {}

    # Group line charts (vol/num) have the group name in the last column
    for month, date, v_adj, v_unadj, groupname in data:
        sec = util.epochtime(date)

        # JSON fix for age groups - strip off the "Age "
        if groupname.lower().find("age ") == 0:
            groupname = groupname[4:]

        # Initialize if first time groupname is encountered
        if groupname not in out:
            out[groupname] = {"adjusted": [], "unadjusted": []}

        try:
            out[groupname]["adjusted"].append([
                util.milliseconds(sec),
                float(v_adj)
            ])
            out[groupname]["unadjusted"].append([
                util.milliseconds(sec),
                float(v_unadj)
            ])
        except ValueError:
            logger.debug(
                "Ignore ValueError: Discard 'NA' and other non-float values"
            )
            continue
        except TypeError as e:
            logger.warning(
                "Missing value as '{}' in row\n'{}'".format(
                    row[2+colnum],
                    repr(row)
                )
            )
            # TODO: Raise error with data so filename can be determined
            continue

    return out


def json_for_tile_map(data):
    """Takes input data and returns a list of dicts of state names and
    percentages for dumping to a JSON file:
    Input is a list of lists: [[FIPS code, state abbr, percentages],...]
    Output is list of dicts: [{"name": abbr, "value": percentage},...]
    """

    out = []

    for code, state, value in data:
        try:
            value = "{:0.2f}".format(float(value) * 100)
        except ValueError:
            logger.debug(
                "Ignore ValueError: Leave 'NA' as-is for states if found"
            )

        out.append({"name": state, "value": value})

    return out


def process_data_snapshot(filepath, date_schema=cfg.SNAPSHOT_DATE_SCHEMA):
    """Process a file at filepath that contains data snapshot information
    for all markets and prepare human-readable text for output.
    Returns a list of market-data dictionaries."""

    # Load specified file as input data
    inputdata = util.load_csv(filepath)
    logger.info("Loaded data snapshot file from {}".format(filepath))

    # Initialize output data
    market_info = {}

    for row in inputdata:
        # Unpack the row values
        monthnum, market, var_name, value, value_yoy = row
        monthnum = int(monthnum)
        var_name = var_name.lower()

        # Determine month string from month number
        month = actual_date(monthnum, schema=date_schema)

        # If first time seeing market, create sub-dict
        if market not in market_info:
            market_info[market] = {"market_key": market}

        # Handle the variable type
        # Each variable has value and value_yoy
        if "originations" in var_name:
            # Calculate originations
            orig_fmt = util.human_numbers(float(value), whole_units_only=1)

            # Calculate year-over-year change in originations
            yoy = float(value_yoy)
            yoy_num = "{:.1f}".format(abs(yoy))
            yoy_desc = cfg.PERCENT_CHANGE_DESCRIPTORS[yoy > 0]
            yoy_fmt = "{}% {}".format(yoy_num, yoy_desc)

            # Store data for market
            market_info[market]["data_month"] = month
            market_info[market]["num_originations"] = orig_fmt
            market_info[market]["year_over_year_change"] = yoy_fmt

        elif "volume" in var_name:
            vol_fmt = "${}".format(util.human_numbers(float(value)))
            market_info[market]["value_originations"] = vol_fmt
            # Volume month is the same as origination month

        elif "inquiry" in var_name:
            yoy = float(value_yoy)
            yoy_num = "{:.1f}".format(abs(yoy))
            yoy_desc = cfg.PERCENT_CHANGE_DESCRIPTORS[yoy > 0]
            yoy_fmt = "{}% {}".format(yoy_num, yoy_desc)

            market_info[market]["inquiry_yoy_change"] = yoy_fmt
            market_info[market]["inquiry_month"] = month

        elif "tightness" in var_name:
            yoy = float(value_yoy)
            yoy_num = "{:.1f}".format(abs(yoy))
            yoy_desc = cfg.PERCENT_CHANGE_DESCRIPTORS[yoy > 0]
            yoy_fmt = "{}% {}".format(yoy_num, yoy_desc)

            market_info[market]["tightness_yoy_change"] = yoy_fmt
            market_info[market]["tightness_month"] = month

        else:
            msg = "Data snapshot row (below) contains unknown " + \
                "var_name name '{}'\n{}".format(
                    var_name, ",".join(row)
                )
            logger.error(msg)
            raise ValueError(msg)

    return list(market_info.values())


# Filenames are formatted as:
# "<prefix>_<market>.csv"
# NOTE: This global set must come after the methods are defined
FILE_PREFIXES = {"map_data":                 process_map,
                 "num_data":                 process_num_summary,
                 "vol_data":                 process_vol_summary,
                 "volume_data_age_group":    process_group_age_vol,
                 "volume_data_income_level": process_group_income_vol,
                 "volume_data_score_level":  process_group_score_vol,
                 "yoy_data_all":             process_yoy_summary,
                 "yoy_data_age_group":       process_group_age_yoy,
                 "yoy_data_income_level":    process_group_income_yoy,
                 "yoy_data_score_level":     process_group_score_yoy,
                 "inq_data":                 process_inquiry_index,
                 "crt_data":                 process_tightness_index,
                 }


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Processes data files from the CFPB Office of Research.'
    )
    parser.add_argument(
        '-i',
        '--input-path',
        metavar="INPUTDIR",
        type=str,
        dest='inputdir',
        default=cfg.DEFAULT_INPUT_FOLDER,
        help='Specifies directory path for folder containing input data ' +
             'files (default: "{}")'.format(cfg.DEFAULT_INPUT_FOLDER)
    )
    parser.add_argument(
        '-o',
        '--output-path',
        metavar="OUTPUTDIR",
        type=str,
        dest='outputdir',
        default=cfg.DEFAULT_OUTPUT_FOLDER,
        help='Specifies directory path for root folder to put processed ' +
             'data (default: "{}")'.format(cfg.DEFAULT_OUTPUT_FOLDER)
    )
    parser.add_argument(
        '-d',
        '--data-snapshot-path',
        type=str,
        default='',
        dest='output_data_snapshot_file',
        help='Specifies path and filename for where to save data snapshot ' +
             'updates as json; if blank (default), no file will be saved'
    )

    args = parser.parse_args()

    # Parse the given paths
    inputdir, outputdir = load_paths(args.inputdir, args.outputdir)

    # Process the data
    snapshot_updates = process_data_files(
        inputdir,
        outputdir,
        data_snapshot_path=args.output_data_snapshot_file
    )
