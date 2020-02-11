#!/usr/bin/env python3

"""
A lightweight geocoder that uses OS Code Point Open where possible for postcodes and GMaps API for
everything else.

- Jamie Taylor <jamie.taylor@sheffield.ac.uk>
- First Authored: 2019-10-08
"""

__version__ = "0.5.0"

import os
import sys
import pickle
import time as TIME
import argparse
import zipfile
import json
import glob
import warnings
import requests
from numpy import isnan
import pandas as pd
import googlemaps
from convertbng.util import convert_lonlat

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

class Geocoder:
    """Use Code Point Open and GMaps to geocode addresses and postcodes."""
    def __init__(self, quiet=False, progress_bar=False, skip_setup=False):
        self.quiet = quiet
        version_string = __version__.replace(".", "-")
        self.gmaps_dir = os.path.join(SCRIPT_DIR, "google_maps")
        self.gmaps_cache_file = os.path.join(self.gmaps_dir,
                                             "gmaps_cache_{}.p".format(version_string))
        self.cpo_dir = os.path.join(SCRIPT_DIR, "code_point_open")
        self.cache_dir = os.path.join(SCRIPT_DIR, "cache")
        self.cpo_zipfile = os.path.join(self.cpo_dir, "codepo_gb.zip")
        self.cpo_cache_file = os.path.join(self.cpo_dir,
                                           "code_point_open_{}.p".format(version_string))
        self.ons_dir = os.path.join(SCRIPT_DIR, "ons")
        if not os.path.isdir(self.ons_dir):
            os.mkdir(self.ons_dir)
        self.llsoa_cache_file = os.path.join(self.ons_dir,
                                             "llsoa_centroids_{}.p".format(version_string))
        self.gov_dir = os.path.join(SCRIPT_DIR, "gov")
        self.constituency_lookup_file = os.path.join(self.gov_dir,
                                                     "constituency_centroids.psv")
        self.constituency_cache_file = os.path.join(self.gov_dir,
                                                    "constituency_centroids_{}.p"
                                                    .format(version_string))
        self.gmaps_key_file = os.path.join(self.gmaps_dir, "key.txt")
        self.gmaps_cache = None
        self.llsoa_lookup = None
        self.constituency_lookup = None
        if not skip_setup:
            self.cpo = self.load_code_point_open()
            self.gmaps_key = self.load_gmaps_key()
            if self.gmaps_key is not None:
                self.gmaps = googlemaps.Client(key=self.gmaps_key)
            self.gmaps_cache = self.load_gmaps_cache()
        self.timer = TIME.time()
        self.progress_bar = progress_bar
        self.cache_file = os.path.join(self.cache_dir, "cache_{}.p".format(version_string))
        self.cache = self.load_cache()
        self.status_codes = {
            0: "Failed",
            1: "Full match with Code Point Open",
            2: "Partial match with Code Point Open",
            3: "Full match with GMaps",
            4: "Partial match with GMaps",
        }

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.flush_gmaps_cache()
        self.flush_cache()

    def clear_cache(self):
        cache_files = glob.glob(os.path.join(self.cache_dir, "cache_*.p"))
        for cache_file in cache_files:
            os.remove(cache_file)
        gmaps_cache_files = glob.glob(os.path.join(self.gmaps_dir, "gmaps_cache_*.p"))
        for gmaps_cache_file in gmaps_cache_files:
            os.remove(gmaps_cache_file)
        cpo_cache_files = glob.glob(os.path.join(self.cpo_dir, "code_point_open_*.p"))
        for cpo_cache_file in cpo_cache_files:
            os.remove(cpo_cache_file)
        llsoa_cache_files = glob.glob(os.path.join(self.ons_dir, "llsoa_centroids_*.p"))
        for llsoa_cache_file in llsoa_cache_files:
            os.remove(llsoa_cache_file)
        constituency_cache_files = glob.glob(os.path.join(self.gov_dir,
                                                          "constituency_centroids_*.p"))
        for constituency_cache_file in constituency_cache_files:
            os.remove(constituency_cache_file)

    def load_gmaps_key(self):
        try:
            with open(self.gmaps_key_file) as fid:
                key = fid.read().strip()
        except FileNotFoundError:
            warnings.warn("Failed to load Google Maps API key - you will not be able to make new "
                          "queries to the Google Maps API!")
            return None
        return key

    def load_gmaps_cache(self):
        if os.path.isfile(self.gmaps_cache_file):
            with open(self.gmaps_cache_file, "rb") as pickle_fid:
                return pickle.load(pickle_fid)
        else:
            return {}

    def load_cache(self):
        if os.path.isfile(self.cache_file):
            with open(self.cache_file, "rb") as pickle_fid:
                return pickle.load(pickle_fid)
        else:
            return {}

    def flush_gmaps_cache(self):
        if self.gmaps_cache is not None:
            with open(self.gmaps_cache_file, "wb") as pickle_fid:
                pickle.dump(self.gmaps_cache, pickle_fid)

    def flush_cache(self):
        if self.cache is not None:
            with open(self.cache_file, "wb") as pickle_fid:
                pickle.dump(self.cache, pickle_fid)

    def load_code_point_open(self, force_reload=False):
        if os.path.isfile(self.cpo_cache_file) and not force_reload:
            with open(self.cpo_cache_file, "rb") as pickle_fid:
                return pickle.load(pickle_fid)
        self.myprint("Extracting the Code Point Open data (this only needs to be done once)...")
        if not zipfile.is_zipfile(self.cpo_zipfile):
            raise Exception(f"Could not find the OS Code Point Open data: '{self.cpo_zipfile}'")
        columns = ["Postcode", "Positional_quality_indicator", "Eastings", "Northings",
                   "Country_code", "NHS_regional_HA_code", "NHS_HA_code", "Admin_county_code",
                   "Admin_district_code", "Admin_ward_code"]
        dtypes = {"Postcode": str, "Eastings": int, "Northings": int,
                  "Positional_quality_indicator": int}
        cpo = None
        with zipfile.ZipFile(self.cpo_zipfile, "r") as cpo_zip:
            for cpo_file in cpo_zip.namelist():
                if "Data/CSV/" not in cpo_file:
                    continue
                with cpo_zip.open(cpo_file) as cpo_file_part:
                    data = pd.read_csv(cpo_file_part, names=columns, dtype=dtypes,
                                       usecols=["Postcode", "Positional_quality_indicator",
                                                "Eastings", "Northings"])
                    cpo = pd.concat([cpo, data]) if cpo is not None else data
        cpo["Postcode"] = cpo["Postcode"].str.replace(" ", "", regex=False)
        cpo["Postcode"] = cpo["Postcode"].str.upper()
        nn_indices = cpo["Eastings"].notnull() & cpo["Positional_quality_indicator"] < 90
        lons, lats = convert_lonlat(cpo["Eastings"][nn_indices], cpo["Northings"][nn_indices])
        cpo.loc[nn_indices, "longitude"] = lons
        cpo.loc[nn_indices, "latitude"] = lats
        cpo["outward_postcode"] = cpo["Postcode"].str.slice(0, -3).str.strip()
        cpo["inward_postcode"] = cpo["Postcode"].str.slice(-3).str.strip()
        with open(self.cpo_cache_file, "wb") as pickle_fid:
            pickle.dump(cpo, pickle_fid)
        self.myprint(f"    -> Extracted and pickled to '{self.cpo_cache_file}'")
        return cpo

    def load_llsoa_lookup(self):
        if os.path.isfile(self.llsoa_cache_file):
            with open(self.llsoa_cache_file, "rb") as pickle_fid:
                return pickle.load(pickle_fid)
        self.myprint("Extracting the ONS LLSOA Centroids data (this only needs to be done once)...")
        ons_url = "https://opendata.arcgis.com/datasets/b7c49538f0464f748dd7137247bbc41c_0.geojson"
        success = False
        retries = 0
        while not success and retries < 3:
            try:
                response = requests.get(ons_url)
                response.raise_for_status()
                if response.status_code != 200:
                    success = False
                    retries += 1
                    continue
                raw = json.loads(response.text)
                llsoa_lookup = {f["properties"]["lsoa11cd"]:
                                tuple(f["geometry"]["coordinates"][::-1])
                                for f in raw["features"]}
                success = True
            except:
                success = False
                retries += 1
        if not success:
            raise Exception("Encountered an error while extracting LLSOA data from ONS API.")
        with open(self.llsoa_cache_file, "wb") as pickle_fid:
            pickle.dump(llsoa_lookup, pickle_fid)
        self.myprint(f"    -> Extracted and pickled to '{self.llsoa_cache_file}'")
        return llsoa_lookup

    def load_constituency_lookup(self):
        if os.path.isfile(self.constituency_cache_file):
            with open(self.constituency_cache_file, "rb") as pickle_fid:
                return pickle.load(pickle_fid)
        self.myprint("Extracting the Constituency Centroids data (this only needs to be done "
                     "once)...")
        constituency_lookup = {}
        with open(self.constituency_lookup_file) as fid:
            for line in fid:
                code, name, longitude, latitude = line.strip().split("|")
                match_str = name.strip().replace(" ", "").replace(",", "").lower()
                constituency_lookup[match_str] = (float(latitude), float(longitude))
        with open(self.constituency_cache_file, "wb") as pickle_fid:
            pickle.dump(constituency_lookup, pickle_fid)
        self.myprint(f"    -> Extracted and pickled to '{self.constituency_cache_file}'")
        return constituency_lookup

    def cpo_geocode(self, postcode):
        postcode = postcode.upper()
        match = self.cpo[self.cpo["Postcode"] == postcode.replace(" ", "")].agg("mean")
        if not isnan(match.latitude):
            return match.latitude, match.longitude, 1
        if " " in postcode:
            outward, inward = postcode.split(" ", 1)
            myfilter = (self.cpo["outward_postcode"] == outward) & \
                       (self.cpo["inward_postcode"].str.slice(0, len(inward)) == inward)
        else:
            outward = postcode
            myfilter = self.cpo["outward_postcode"] == outward
        match = self.cpo[myfilter].agg("mean")
        if not isnan(match.latitude):
            return match.latitude, match.longitude, 2
        return None, None, 0

    def gmaps_geocode(self, postcode, address):
        sep = ", " if address else ""
        address = address if address is not None else ""
        search_term = f"{address}{sep}{postcode}"
        if search_term in self.gmaps_cache:
            geocode_result = self.gmaps_cache[search_term]
        else:
            if self.gmaps_key is None:
                return None, None, 0
            geocode_result = self.gmaps.geocode(search_term, region="uk")
            self.gmaps_cache[search_term] = geocode_result
        if not geocode_result or len(geocode_result) > 1:
            return None, None, 0
        geometry = geocode_result[0]["geometry"]
        if geometry["location_type"] == "ROOFTOP" or geocode_result[0]["types"] == ["postal_code"]:
            return geometry["location"]["lat"], geometry["location"]["lng"], 3
        return None, None, 0

    def geocode(self, postcodes=None, addresses=None):
        results = []
        if postcodes is None and addresses is None:
            raise Exception("You must pass either postcodes or addresses, or both.")
        postcodes = [None for a in addresses] if postcodes is None else list(postcodes)
        addresses = [None for p in postcodes] if addresses is None else list(addresses)
        tot = len(postcodes)
        for i, (postcode, address) in enumerate(zip(postcodes, addresses)):
            if (postcode, address) in self.cache:
                results.append(self.cache[(postcode, address)])
            else:
                results.append(self.geocode_one(postcode, address))
                self.cache[(postcode, address)] = results[-1]
            if self.progress_bar and (i % 10 == 0 or i == tot - 1):
                print_progress(i+1, tot, prefix="[Geocode]     POSTCODE", suffix="", decimals=2,
                               bar_length=100)
        return results

    def geocode_one(self, postcode=None, address=None):
        if postcode is None and address is None:
            raise Exception("You must specify either a postcode or an address, or both.")
        if address is None:
            lat, lon, status = self.cpo_geocode(postcode)
            if status > 0:
                return lat, lon, status
        return self.gmaps_geocode(postcode, address)

    def geocode_llsoa(self, llsoa):
        if self.llsoa_lookup is None:
            self.llsoa_lookup = self.load_llsoa_lookup()
        if isinstance(llsoa, str):
            return self.llsoa_centroid(llsoa)
        results = []
        tot = len(llsoa)
        for i, llsoa_ in enumerate(llsoa):
            results.append(self.llsoa_centroid(llsoa_))
            if self.progress_bar and (i % 10 == 0 or i == tot - 1):
                print_progress(i+1, tot, prefix="[Geocode]     LLSOA", suffix="", decimals=2,
                               bar_length=100)
        return results

    def llsoa_centroid(self, llsoa):
        try:
            return self.llsoa_lookup[llsoa]
        except KeyError:
            return None, None

    def geocode_constituency(self, constituency):
        if self.constituency_lookup is None:
            self.constituency_lookup = self.load_constituency_lookup()
        if isinstance(constituency, str):
            return self.constituency_centroid(constituency)
        results = []
        tot = len(constituency)
        for i, constituency_ in enumerate(constituency):
            results.append(self.constituency_centroid(constituency_))
            if self.progress_bar and (i % 10 == 0 or i == tot - 1):
                print_progress(i+1, tot, prefix="[Geocode]     CONSTITUENCY", suffix="", decimals=2,
                               bar_length=100)
        return results

    def constituency_centroid(self, constituency):
        try:
            match_str = constituency.strip().replace(" ", "").replace(",", "").lower()
            return self.constituency_lookup[match_str]
        except KeyError:
            return None, None

    def myprint(self, msg, time_section=None):
        """Use this function to print updates unless class attribute quiet is set to True."""
        if not self.quiet:
            if time_section == "stop":
                msg += " ({:.1f} seconds)".format(TIME.time() - self.timer)
            print("[Geocode] " + msg)
            if time_section == "start":
                self.timer = TIME.time()

def print_progress(iteration, total, prefix="", suffix="", decimals=2, bar_length=100):
    """
    Call in a loop to create terminal progress bar.

    Parameters
    ----------
    `iteration` : int
        current iteration (required)
    `total` : int
        total iterations (required)
    `prefix` : string
        prefix string (optional)
    `suffix` : string
        suffix string (optional)
    `decimals` : int
        number of decimals in percent complete (optional)
    `bar_length` : int
        character length of bar (optional)
    Notes
    -----
    Taken from `Stack Overflow <http://stackoverflow.com/a/34325723>`_.
    """
    filled_length = int(round(bar_length * iteration / float(total)))
    percents = round(100.00 * (iteration / float(total)), decimals)
    progress_bar = "#" * filled_length + "-" * (bar_length - filled_length)
    sys.stdout.write("\r%s |%s| %s%s %s" % (prefix, progress_bar, percents, "%", suffix))
    sys.stdout.flush()
    if iteration == total:
        sys.stdout.write("\n")
        sys.stdout.flush()

def parse_options():
    """Parse command line options."""
    parser = argparse.ArgumentParser(description=("This is a command line interface (CLI) for "
                                                  f"the Geocode module version {__version__}."),
                                     epilog="Jamie Taylor, 2019-10-08")
    parser.add_argument("--clear-cache", dest="clear_cache", action="store_true",
                        required=False, help="Specify to delete the cache files.")
    parser.add_argument("--debug", dest="debug", action="store_true",
                        required=False, help="Geocode some sample postcodes/addresses/LLSOAs.")
    parser.add_argument("--load-cpo-zip", dest="cpo_zip", action="store", type=str,
                        required=False, default=None, metavar="</path/to/zip-file>",
                        help="Load the Code Point Open data from a local zip file.")
    parser.add_argument("--load-gmaps-key", dest="gmaps_key", action="store", type=str,
                        required=False, default=None, metavar="</path/to/zip-file>",
                        help="Load a Google Maps API key.")
    options = parser.parse_args()
    return options

def main():
    options = parse_options()
    if options.clear_cache:
        with Geocoder(skip_setup=True) as geocoder:
            geocoder.clear_cache()
    if options.cpo_zip is not None:
        from shutil import copyfile
        with Geocoder(skip_setup=True) as geocoder:
            copyfile(options.cpo_zip, geocoder.cpo_zipfile)
            geocoder.load_code_point_open(force_reload=True)
    if options.gmaps_key is not None:
        print(f"[Geocode] Loading GMaps key...")
        with Geocoder(skip_setup=True) as geocoder:
            with open(geocoder.gmaps_key_file, "w") as fid:
                fid.write(options.gmaps_key)
            if geocoder.load_gmaps_key() == options.gmaps_key:
                print(f"[Geocode]     -> GMaps key saved to '{geocoder.gmaps_key_file}'")
    if options.debug:
        sample_llsoas = ["E01025397", "E01003065", "E01017548", "E01023301", "E01021142",
                         "E01019037", "E01019037", "E01013873"]
        with Geocoder(progress_bar=True) as geocoder:
            results = geocoder.geocode_llsoa(sample_llsoas)
        for llsoa, (lat, lon) in zip(sample_llsoas, results):
            print(f"[Geocode] {llsoa} :    {lat}, {lon}")
        sample_constituencies = ["Berwickshire Roxburgh and Selkirk", "Argyll and Bute",
                                 "Inverness Nairn Badenoch and Strathspey", # missing commas :(
                                 "Dumfries and Galloway"]
        with Geocoder(progress_bar=True) as geocoder:
            results = geocoder.geocode_constituency(sample_constituencies)
        for constituency, (lat, lon) in zip(sample_constituencies, results):
            print(f"[Geocode] {constituency} :    {lat}, {lon}")
        sample_file = os.path.join(SCRIPT_DIR, "sample_postcodes.txt")
        with open(sample_file) as fid:
            postcodes = [line.strip() for line in fid if line.strip()]
        timerstart = TIME.time()
        with Geocoder(progress_bar=True) as geocoder:
            results = geocoder.geocode(postcodes=postcodes[:100])
            for postcode, (lat, lon, status) in zip(postcodes, results):
                print(f"[Geocode] {postcode} :    {lat}, {lon}    ->  "
                      f"{geocoder.status_codes[status]}")
        print("[Geocode] Time taken: {:.1f} seconds".format(TIME.time() - timerstart))

if __name__ == "__main__":
    main()
