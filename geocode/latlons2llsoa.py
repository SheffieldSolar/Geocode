#!/usr/bin/env python3
"""
Load a list of lat/lons from a CSV file and reverse-geocode them to LLSOA.

- Jamie Taylor <jamie.taylor@sheffield.ac.uk>
- First Authored: 2020-04-16
"""

import sys
import os
import argparse
import time as TIME
import pandas as pd

from geocode import Geocoder, query_yes_no

def parse_options():
    """Parse command line options."""
    parser = argparse.ArgumentParser(description=("This is a command line interface (CLI) for "
                                                  "the latlons2llsoa.py module"),
                                     epilog="Jamie Taylor, 2020-04-16")
    parser.add_argument("-f", "--input-file", dest="infile", action="store", type=str,
                        required=True, metavar="</path/to/file>",
                        help="Specify a CSV file containing a list of latitudes and longitudes to "
                             "be reverse-geocoded. The file must contain the columns 'latitude' "
                             "and 'longitude' (it can contain others, all of which will be kept).")
    parser.add_argument("-o", "--output-file", dest="outfile", action="store", type=str,
                        required=True, metavar="</path/to/file>", help="Specify an output file.")
    parser.add_argument("--datazones", dest="datazones", action="store_true",
                        required=False, help="Specify to use Data Zones in Scotland.")
    options = parser.parse_args()
    if not os.path.isfile(options.infile):
        raise Exception(f"The input file '{options.infile}' does not exist.")
    if os.path.isfile(options.outfile):
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
    with Geocoder(progress_bar=True) as geo:
        # import pdb; pdb.set_trace()
        df["llsoacd"] = geo.reverse_geocode_llsoa(df[["latitude", "longitude"]].to_numpy(),
                                                  options.datazones)
    df.to_csv(options.outfile, index=False)
    print(f"Finished, time taken: {TIME.time() - timerstart} seconds")

if __name__ == "__main__":
    main()
