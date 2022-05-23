#!/usr/bin/env python3
"""
Load a list of postcodes from a CSV file and geocode them to latitude/longitude.

- Jamie Taylor <jamie.taylor@sheffield.ac.uk>
- First Authored: 2020-06-12
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
                                                  "the postcodes2latlon.py module"),
                                     epilog="Jamie Taylor, 2020-06-12")
    parser.add_argument("-f", "--input-file", dest="infile", action="store", type=str,
                        required=True, metavar="</path/to/file>",
                        help="Specify a CSV file containing a list of postcodes to be geocoded. "
                             "The file must contain the column 'postcode' (it can contain others, "
                             "all of which will be kept).")
    parser.add_argument("-o", "--output-file", dest="outfile", action="store", type=str,
                        required=True, metavar="</path/to/file>", help="Specify an output file.")
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
        df = pd.read_csv(fid).iloc[:100]
    with Geocoder(progress_bar=True) as geo:
        results = geo.geocode(postcodes=df["postcode"].to_numpy())
        lats = [r[0] if r[0] is not None else pd.NA for r in results]
        lons = [r[1] if r[1] is not None else pd.NA for r in results]
        status = [geo.status_codes[r[2]] for r in results]
    df["latitude"] = lats
    df["longitude"] = lons
    df["geocode_status"] = status
    df.to_csv(options.outfile, index=False)
    print(f"Finished, time taken: {TIME.time() - timerstart:.1f} seconds")

if __name__ == "__main__":
    main()
