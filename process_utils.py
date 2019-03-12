#!/usr/bin/env python
"""
Processes incoming data from the Office of Research and munges it into
the output formats expected by the CFPB chart display organisms.

Output formats are documented at
www.github.com/cfpb/consumer-credit-trends
"""

# Python library imports
import os
import csv
import datetime
import math
import logging
import json


__author__ = "Consumer Financial Protection Bureau"
__credits__ = ["Hillary Jeffrey"]
__license__ = "CC0-1.0"
__version__ = "2.0"
__maintainer__ = "CFPB"
__email__ = "tech@cfpb.gov"
__status__ = "Development"

# Constants

SEC_TO_MS = 1000


# Set up logging
logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)


# Utility Methods

def save_csv(filename, content, writemode='w'):
    """Saves the specified content object into a csv file."""
    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))
        logger.info(
            "Created directories for {}".format(os.path.dirname(filename))
        )

    # Write output as a csv file
    with open(filename, writemode) as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerows(content)

    logger.debug("Wrote file '{}'".format(filename))


def save_json(filename, json_content, writemode='w'):
    """Dumps the specified JSON content into a .json file"""
    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))
        logger.info(
            "Created directories for {}".format(os.path.dirname(filename))
        )

    # Write output as a json file
    with open(filename, writemode) as fp:
        json.dump(
            json_content,
            fp,
            sort_keys=True,
            indent=4,
            separators=(',', ': ')
        )

    logger.debug("Wrote file '{}'".format(filename))


def load_csv(filename, readmode='r', skipheaderrow=True):
    """Loads CSV data from a file"""
    with open(filename, readmode) as csvfile:
        reader = csv.reader(csvfile)
        data = list(reader)

    if skipheaderrow:
        return data[1:]
    else:
        return data


def expand_path(path):
    """Expands a relative path into an absolute path"""
    rootpath = os.path.abspath(os.path.expanduser(path))

    return rootpath


def get_csv_list(path):
    """Loads a list of files in the specified directory"""
    files = [f for f in os.listdir(path)
             if f.lower().endswith('.csv')
             and os.path.isfile(os.path.join(path, f))]

    return files


def milliseconds(sec):
    """Convert seconds to milliseconds"""
    return sec * SEC_TO_MS


# Unix Epoch conversion from http://stackoverflow.com/questions/11743019/
def epochtime(datestring, schema="%Y-%m"):
    """Converts a date string from specified schema to seconds since
    J70/Unix epoch"""
    date = datetime.datetime.strptime(datestring, schema)

    return int(round((date - datetime.datetime(1970, 1, 1)).total_seconds()))


# Modified from an answer at:
# http://stackoverflow.com/questions/3154460/
def human_numbers(num, decimal_places=1, whole_units_only=1):
    """Given a number, returns a human-modifier (million/billion) number
    Number returned will be to the specified number of decimal places with
    modifier (default: 1) - e.g. 1100000 returns '1.1 million'.
    If whole_units_only is specified, no parts less than one unit will
    be displayed, i.e. 67.012 becomes 67.
    whole_units_only has no effect on numbers with modifiers (>1 million)."""
    numnames = [
        '',
        '',
        'million',
        'billion',
        'trillion',
        'quadrillion',
        'quintillion'
    ]

    n = float(num)
    idx = max(0,
              min(len(numnames) - 1,
                  int(math.floor(0 if n == 0 else math.log10(abs(n))/3))
                  )
              )

    # Create the output string with the requested number of decimal places
    # This has to be a separate step from the format() call because otherwise
    # format() gets called on the final fragment only
    outstr = '{:,.' + str(decimal_places) + 'f} {}'

    # Insert commas every 3 numbers
    if idx < 2:
        if whole_units_only:
            return '{:,}'.format(int(round(n)))
        else:
            return outstr.format(n, numnames[idx]).strip()

    # Calculate the output number by order of magnitude
    outnum = n / 10**(3 * idx)

    return outstr.format(outnum, numnames[idx])
