#!/usr/bin/env python3
"""
Load a list of lat/lons from a CSV file and reverse-geocode them to GSP / GNode.

- Jamie Taylor <jamie.taylor@sheffield.ac.uk>
- Ethan Jones <ejones18@sheffield.ac.uk>
- First Authored: 2020-05-29
"""

import sys
import argparse
import time as TIME
from pathlib import Path

import pandas as pd

from geocode import Geocoder
from utilities import query_yes_no, GenericException

def parse_options():
    """Parse command line options."""
    parser = argparse.ArgumentParser(description=("This is a command line interface (CLI) for "
                                                  "the latlons2gsp.py module"),
                                     epilog="Jamie Taylor & Ethan Jones, 2020-05-29")
    parser.add_argument("-f", "--input-file", dest="infile", action="store", type=Path,
                        required=True, metavar="</path/to/file>",
                        help="Specify a CSV file containing a list of latitudes and longitudes to "
                             "be reverse-geocoded. The file must contain the columns 'latitude' "
                             "and 'longitude' (it can contain others, all of which will be kept).")
    parser.add_argument("-o", "--output-file", dest="outfile", action="store", type=Path,
                        required=True, metavar="</path/to/file>", help="Specify an output file.")
    options = parser.parse_args()
    if not options.infile.is_file():
        raise GenericException(f"The input file '{options.infile}' does not exist.")
    if options.outfile.is_file():
        check = query_yes_no(f"The outfile '{options.outfile}' already exists, are you sure you "
                             "wish to overwrite?", "no")
        if not check:
            print("Quitting...")
            sys.exit(0)
    return options

def main():
    timerstart = TIME.time()
    options = parse_options()
    with open(options.infile, "r") as fid:
        df = pd.read_csv(fid)
    with Geocoder() as geo:
        results_ = geo.reverse_geocode_gsp(df[["latitude", "longitude"]].to_numpy())
    results_ = [(None, None) if gsp_pair is None else gsp_pair for gsp_pair in results_]
    gsps, gsp_groups = [x[0] for x in results_], [x[1] for x in results_]
    output = pd.DataFrame({'gsp': gsps, 'gsp_group': gsp_groups})
    output.to_csv(options.outfile, index=False)
    print(f"Finished, time taken: {TIME.time() - timerstart:.1f} seconds")

if __name__ == "__main__":
    main()
