#!/usr/bin/env python3
"""
Load a list of lat/lons from a CSV file and reverse-geocode them to GSP / GNode.

- Jamie Taylor <jamie.taylor@sheffield.ac.uk>
- First Authored: 2020-05-29
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
                                                  "the latlons2gsp.py module"),
                                     epilog="Jamie Taylor, 2020-05-29")
    parser.add_argument("-f", "--input-file", dest="infile", action="store", type=str,
                        required=True, metavar="</path/to/file>",
                        help="Specify a CSV file containing a list of latitudes and longitudes to "
                             "be reverse-geocoded. The file must contain the columns 'latitude' "
                             "and 'longitude' (it can contain others, all of which will be kept).")
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
        df = pd.read_csv(fid)
    with Geocoder(progress_bar=True) as geo:
        region_ids, extra = geo.reverse_geocode_gsp(df[["latitude", "longitude"]].to_numpy())
    # Following code is a bit clunky and performance needs improving
    output = []
    for i, row in df.iterrows():
        if extra[i] is not None:
            for l in extra[i]: # There might be more than one match e.g. where GSP:GNODE is ONE:MANY
                output.append(row.tolist() + list(l.values()))
        else:
            output.append(row.tolist() + [None] * len(list(l.values())))
    columns = row.index.to_list() + list(l.keys())
    output = pd.DataFrame(output, columns=columns)
    output.to_csv(options.outfile, index=False)
    print(f"Finished, time taken: {TIME.time() - timerstart:.1f} seconds")

if __name__ == "__main__":
    main()
