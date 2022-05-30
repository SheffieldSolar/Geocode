#!/usr/bin/env python3

"""
A lightweight geocoder that uses OS Code Point Open where possible for postcodes and GMaps API for
everything else.
- Jamie Taylor <jamie.taylor@sheffield.ac.uk>
- Ethan Jones <ejones18@sheffield.ac.uk>
- First Authored: 2019-10-08
"""

__version__ = "0.10.0"

import os
import sys
from typing import Optional, Iterable, Tuple, Union, List, Dict
import logging
import pickle
import time as TIME
import argparse
import zipfile
from shutil import copyfile
from io import StringIO
import json
import csv
import glob
import requests
import numpy as np
import pandas as pd
import geopandas as gpd
import googlemaps
import pyproj
import shapefile
try:
    from shapely.geometry import shape, Point
    from shapely.ops import unary_union
    SHAPELY_AVAILABLE = True
except ImportError:
    logging.warning("Failed to import Shapely library - you will not be able to reverse-geocode! "
                    "See notes in the README about installing Shapely on Windows machines.")
    SHAPELY_AVAILABLE = False

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

class GenericException(Exception):
    """A generic exception for anticipated errors."""
    def __init__(self, msg, err=None):
        self.msg = msg
        if err is not None:
            self.msg += f"\n{repr(err)}"
        logging.exception(self.msg)

    def __str__(self):
        return self.msg

class Geocoder:
    """
    Geocode addresses, postcodes, LLSOAs or Constituencies or reverse-geocode latitudes and
    longitudes.
    """
    def __init__(self,
                 progress_bar: bool = False,
                 prefix: Optional[str] = None,
                 gmaps_key_file: Optional[str] = None) -> None:
        """
        Geocode addresses, postcodes, LLSOAs or Constituencies or reverse-geocode latitudes and
        longitudes.

        Parameters
        ----------
        `progress_bar` : boolean
            Optionally print a progress bar to stdout during iterations. Defaults to False.
        `prefix` : string
            Optionally add a prefix string to the progress bar.
        `gmaps_key_file` : string
            Path to an API key file for Google Maps Geocode API.
        """
        self.prefix = prefix if prefix is not None else ""
        version_string = __version__.replace(".", "-")
        self.gmaps_dir = os.path.join(SCRIPT_DIR, "google_maps")
        self.gmaps_cache_file = os.path.join(self.gmaps_dir, "gmaps_cache.p")
        self.cpo_dir = os.path.join(SCRIPT_DIR, "code_point_open")
        self.cache_dir = os.path.join(SCRIPT_DIR, "cache")
        self.cpo_zipfile = os.path.join(self.cpo_dir, "codepo_gb.zip")
        self.cpo_cache_file = os.path.join(self.cpo_dir,
                                           f"code_point_open_{version_string}.p")
        self.ons_dir = os.path.join(SCRIPT_DIR, "ons_nrs")
        self.eso_dir = os.path.join(SCRIPT_DIR, "ngeso")
        if not os.path.isdir(self.ons_dir):
            os.mkdir(self.ons_dir)
        if not os.path.isdir(self.eso_dir):
            os.mkdir(self.eso_dir)
        self.llsoa_cache_file = os.path.join(self.ons_dir,
                                             f"llsoa_centroids_{version_string}.p")
        self.llsoa_boundaries_cache_file = os.path.join(
            self.ons_dir, f"llsoa_boundaries_{version_string}.p"
        )
        self.gsp_boundaries_20181031_cache_file = os.path.join(
            self.eso_dir, f"gsp_boundaries_20181031_{version_string}.p"
        )
        self.gsp_boundaries_20220314_cache_file = os.path.join(
            self.eso_dir, f"gsp_boundaries_20220314_{version_string}.p"
        )
        self.dno_boundaries_cache_file = os.path.join(
            self.eso_dir, f"dno_boundaries_{version_string}.p"
        )
        self.gsp_lookup_20181031_cache_file = os.path.join(
            self.eso_dir, f"gsp_lookup_20181031_{version_string}.p"
        )
        self.dz_lookup_cache_file = os.path.join(self.ons_dir,
                                                 f"datazone_lookup_{version_string}.p")
        self.nrs_zipfile = os.path.join(self.ons_dir, "nrs.zip")
        self.gov_dir = os.path.join(SCRIPT_DIR, "gov")
        self.constituency_lookup_file = os.path.join(self.gov_dir,
                                                     "constituency_centroids.psv")
        self.constituency_cache_file = os.path.join(self.gov_dir,
                                                    f"constituency_centroids_{version_string}.p")
        self.gmaps_key_file = gmaps_key_file if gmaps_key_file is not None \
                                  else os.path.join(self.gmaps_dir, "key.txt")
        self.llsoa_lookup = None
        self.llsoa_regions = None
        self.gsp_regions = None
        self.gsp_regions_20181031 = None
        self.dno_regions = None
        self.gsp_lookup_20181031 = None
        self.llsoa_reverse_lookup = None
        self.constituency_lookup = None
        self.dz_lookup = None
        self.cpo = None
        self.gmaps_key = None
        self.gmaps = None
        self.gmaps_cache = None
        self.timer = TIME.time()
        self.progress_bar = progress_bar
        self.cache_file = os.path.join(self.cache_dir, f"cache_{version_string}.p")
        self.clear_cache(delete_gmaps_cache=False, old_versions_only=True)
        self.cache = self._load_cache()
        self.status_codes = {
            0: "Failed",
            1: "Full match with Code Point Open",
            2: "Partial match with Code Point Open",
            3: "Full match with GMaps",
            4: "Partial match with GMaps",
        }

    def __enter__(self):
        """Context manager."""
        return self

    def __exit__(self, *args):
        """Context manager - flush GMaps cache on exit."""
        self._flush_gmaps_cache()
        self._flush_cache()

    def clear_cache(self,
                    delete_gmaps_cache: Optional[bool] = None,
                    old_versions_only: bool = False) -> None:
        """
        Clear any cache files from the installation directory including from old versions.

        Parameters
        ----------
        delete_gmaps_cache : boolean
            Optional boolean deciding whether or not to clear the Google Maps data in cache.
            Defaults to None, resulting in stdout/stdin prompts.
        old_versions_only : boolean
            Optional boolean deciding whether or not to clear all cache files or just those
            corresponding to previous versions of the Geocode library. Defaults to False.
        """
        logging.debug("Deleting cache files (delete_gmaps_cache=%s, old_versions_only=%s)",
                      delete_gmaps_cache, old_versions_only)
        if delete_gmaps_cache is None:
            delete_gmaps_cache = query_yes_no("Do you want to delete the GMaps cache?", "no")
        if delete_gmaps_cache:
            gmaps_cache_files = glob.glob(os.path.join(self.gmaps_dir, "gmaps_cache.p"))
            for gmaps_cache_file in gmaps_cache_files:
                os.remove(gmaps_cache_file)
                logging.debug("Deleted '%s'", gmaps_cache_file)
        cache_files = glob.glob(os.path.join(self.cache_dir, "cache_*.p")) + \
                      glob.glob(os.path.join(self.cpo_dir, "code_point_open_*.p")) + \
                      glob.glob(os.path.join(self.ons_dir, "llsoa_centroids_*.p")) + \
                      glob.glob(os.path.join(self.ons_dir, "llsoa_boundaries_*.p")) + \
                      glob.glob(os.path.join(self.eso_dir, "gsp_boundaries_*.p")) + \
                      glob.glob(os.path.join(self.eso_dir, "dno_boundaries_*.p")) + \
                      glob.glob(os.path.join(self.eso_dir, "gsp_lookup_*.p")) + \
                      glob.glob(os.path.join(self.ons_dir, "datazone_lookup_*.p")) + \
                      glob.glob(os.path.join(self.gov_dir, "constituency_centroids_*.p"))
        for cache_file in cache_files:
            if old_versions_only and __version__.replace(".", "-") in cache_file:
                continue
            os.remove(cache_file)
            logging.debug("Deleted '%s'", cache_file)

    def force_setup(self):
        """Download all data and setup caches."""
        self._load_gsp_boundaries_20220314()
        self._load_code_point_open(force_reload=False)
        self._load_llsoa_lookup()
        self._load_llsoa_boundaries()
        self._load_datazone_lookup()
        self._load_constituency_lookup()
        self._load_dno_boundaries()

    def _load_gmaps_key(self):
        """Load the user's GMaps API key from installation directory."""
        try:
            with open(self.gmaps_key_file) as fid:
                key = fid.read().strip()
        except FileNotFoundError:
            logging.warning("Failed to load Google Maps API key from '{self.gmaps_key_file}' - you "
                            "will not be able to make new queries to the Google Maps API!")
            return None
        return key

    def _load_gmaps_cache(self):
        """Load the cache of prior GMaps queries to avoid repeated API queries."""
        if os.path.isfile(self.gmaps_cache_file):
            with open(self.gmaps_cache_file, "rb") as pickle_fid:
                return pickle.load(pickle_fid)
        else:
            return {}

    def _load_cache(self):
        """Load the cache of prior addresses/postcodes for better performance."""
        if os.path.isfile(self.cache_file):
            with open(self.cache_file, "rb") as pickle_fid:
                return pickle.load(pickle_fid)
        else:
            return pd.DataFrame({"input_postcode": [], "input_address": [], "latitude": [],
                                 "longitude": [], "match_status": []})

    def _flush_gmaps_cache(self):
        """Flush any new GMaps API queries to the GMaps cache."""
        if self.gmaps_cache is not None:
            with open(self.gmaps_cache_file, "wb") as pickle_fid:
                pickle.dump(self.gmaps_cache, pickle_fid)

    def _flush_cache(self):
        """Flush any new address/postcode queries to the query cache."""
        if self.cache is not None:
            with open(self.cache_file, "wb") as pickle_fid:
                pickle.dump(self.cache, pickle_fid)

    def _load_code_point_open(self, force_reload=False):
        """Load the OS Code Point Open Database, either from raw zip file or local cache."""
        if os.path.isfile(self.cpo_cache_file) and not force_reload:
            logging.debug("Loading Code Point Open data from cache ('%s')", self.cpo_cache_file)
            with open(self.cpo_cache_file, "rb") as pickle_fid:
                return pickle.load(pickle_fid)
        logging.info("Extracting the Code Point Open data (this only needs to be done once)")
        if not zipfile.is_zipfile(self.cpo_zipfile):
            raise GenericException("Could not find the OS Code Point Open data: "
                                   f"'{self.cpo_zipfile}'")
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
        lons, lats = self._bng2latlon(cpo.loc[nn_indices, ("Eastings")].to_numpy(),
                                      cpo.loc[nn_indices, ("Northings")].to_numpy())
        cpo.loc[nn_indices, "longitude"] = lons
        cpo.loc[nn_indices, "latitude"] = lats
        cpo["outward_postcode"] = cpo["Postcode"].str.slice(0, -3).str.strip()
        cpo["inward_postcode"] = cpo["Postcode"].str.slice(-3).str.strip()
        with open(self.cpo_cache_file, "wb") as pickle_fid:
            pickle.dump(cpo, pickle_fid)
        logging.info("Code Point Open extracted and pickled to '%s'", self.cpo_cache_file)
        return cpo

    def _fetch_from_api(self, url):
        """Generic function to GET data from web API with retries."""
        retries = 0
        while retries < 3:
            try:
                response = requests.get(url)
                response.raise_for_status()
                if response.status_code != 200:
                    retries += 1
                    continue
                return 1, response
            except:
                retries += 1
        return 0, None

    def _load_llsoa_lookup(self):
        """Load the lookup of LLSOA -> Population Weighted Centroid."""
        if os.path.isfile(self.llsoa_cache_file):
            logging.debug("Loading LLSOA lookup from cache ('%s')", self.llsoa_cache_file)
            with open(self.llsoa_cache_file, "rb") as pickle_fid:
                return pickle.load(pickle_fid)
        logging.info("Extracting the ONS and NRS LLSOA centroids data (this only needs to be done "
                     "once)")
        ons_url = "https://opendata.arcgis.com/datasets/b7c49538f0464f748dd7137247bbc41c_0.geojson"
        success, api_response = self._fetch_from_api(ons_url)
        if success:
            raw = json.loads(api_response.text)
            engwales_lookup = {f["properties"]["lsoa11cd"]:
                               tuple(f["geometry"]["coordinates"][::-1])
                               for f in raw["features"]}
        else:
            raise GenericException("Encountered an error while extracting LLSOA data from ONS API.")
        codes, eastings, northings = [], [], []
        datazones, dzeastings, dznorthings = [], [], []
        with zipfile.ZipFile(self.nrs_zipfile, "r") as nrs_zip:
            with nrs_zip.open("OutputArea2011_PWC_WGS84.csv", "r") as fid:
                next(fid)
                for line in fid:
                    _, _, code, _, easting, northing = line.decode('UTF-8').strip().split(",")
                    codes.append(code)
                    eastings.append(float(easting))
                    northings.append(float(northing))
            with nrs_zip.open("SG_DataZone_Cent_2011.csv") as fid:
                next(fid)
                contents = [l.decode('UTF-8') for l in fid.readlines()]
                for line in csv.reader(contents, quotechar="\"", delimiter=",",
                                       quoting=csv.QUOTE_ALL, skipinitialspace=True):
                    datazone, _, _, _, _, dzeast, dznorth = line
                    datazones.append(datazone)
                    dzeastings.append(float(dzeast.strip("\"")))
                    dznorthings.append(float(dznorth.strip("\"")))
        lons, lats = self._bng2latlon(eastings, northings)
        dzlons, dzlats = self._bng2latlon(dzeastings, dznorthings)
        scots_lookup = {code: (lat, lon) for code, lon, lat in zip(codes, lons, lats)}
        scots_dz_lookup = {dz: (lat, lon) for dz, lon, lat in zip(datazones, dzlons, dzlats)}
        llsoa_lookup = {**engwales_lookup, **scots_lookup, **scots_dz_lookup}
        with open(self.llsoa_cache_file, "wb") as pickle_fid:
            pickle.dump(llsoa_lookup, pickle_fid)
        logging.info("LSOA centroids extracted and pickled to '%s'", self.llsoa_cache_file)
        return llsoa_lookup

    def _load_llsoa_boundaries(self):
        """
        Load the LLSOA boundaries, either from local cache if available, else fetch from raw API
        (England and Wales) and packaged data (Scotland).
        """
        if os.path.isfile(self.llsoa_boundaries_cache_file):
            logging.debug("Loading LLSOA boundaries from cache ('%s')",
                          self.llsoa_boundaries_cache_file)
            with open(self.llsoa_boundaries_cache_file, "rb") as pickle_fid:
                return pickle.load(pickle_fid)
        logging.info("Extracting the LLSOA boundary data from ONS and NRS (this only needs to be "
                     "done once)")
        # ons_url = "https://opendata.arcgis.com/datasets/007577eeb8e34c62a1844df090a93128_0.geojson"
        # ons_url = "https://opendata.arcgis.com/datasets/f213065139e3441195803b4155e71e00_0.geojson"
        ons_url = "https://opendata.arcgis.com/datasets/e0b761d78e51491d84a3df33dff044c7_0.geojson"
        # Loading NRS data from URL abandoned due to need for re-projection
        # nrs_url = "https://www.nrscotland.gov.uk/files/geography/output-area-2011-eor.zip"
        nrs_shp_file = "OutputArea2011_EoR_WGS84.shp"
        nrs_dbf_file = "OutputArea2011_EoR_WGS84.dbf"
        success, api_response = self._fetch_from_api(ons_url)
        if success:
            raw = json.loads(api_response.text)
            engwales_regions = {f["properties"]["LSOA11CD"]: shape(f["geometry"]).buffer(0)
                                for f in raw["features"]}
        else:
            raise GenericException("Encountered an error while extracting LLSOA data from ONS API.")
        with zipfile.ZipFile(self.nrs_zipfile, "r") as nrs_zip:
            with nrs_zip.open(nrs_shp_file, "r") as shp:
                with nrs_zip.open(nrs_dbf_file, "r") as dbf:
                    sf = shapefile.Reader(shp=shp, dbf=dbf)
                    scots_regions = {sr.record[1]: shape(sr.shape.__geo_interface__).buffer(0)
                                     for sr in sf.shapeRecords()}
        llsoa_regions = {**engwales_regions, **scots_regions}
        llsoa_regions = {llsoacd: (llsoa_regions[llsoacd], llsoa_regions[llsoacd].bounds)
                         for llsoacd in llsoa_regions}
        with open(self.llsoa_boundaries_cache_file, "wb") as pickle_fid:
            pickle.dump(llsoa_regions, pickle_fid)
        logging.info("LSOA boundaries extracted and pickled to '%s'",
                     self.llsoa_boundaries_cache_file)
        return llsoa_regions

    def _load_gsp_boundaries_20181031(self):
        """
        Load the 20181031 GSP / GNode boundaries, either from local cache if available, else fetch
        from ESO Data Portal API.
        """
        if os.path.isfile(self.gsp_boundaries_20181031_cache_file):
            logging.debug("Loading 20181031 GSP boundaries from cache ('%s')",
                          self.gsp_boundaries_20181031_cache_file)
            with open(self.gsp_boundaries_20181031_cache_file, "rb") as pickle_fid:
                return pickle.load(pickle_fid)
        logging.info("Extracting the 20181031 GSP boundary data from NGESO's Data Portal API (this "
                     "only needs to be done once)")
        eso_url = "http://data.nationalgrideso.com/backend/dataset/2810092e-d4b2-472f-b955-d8bea01f9ec0/resource/a3ed5711-407a-42a9-a63a-011615eea7e0/download/gsp_regions_20181031.geojson"
        success, api_response = self._fetch_from_api(eso_url)
        if success:
            raw = json.loads(api_response.text)
            gsp_regions = {}
            for f in raw["features"]:
                region_id = f["properties"]["RegionID"]
                if region_id not in gsp_regions:
                    gsp_regions[region_id] = shape(f["geometry"]).buffer(0)
                else: # Sometimes a region is in multiple pieces due to PES boundary e.g. Axminster
                    gsp_regions[region_id] = unary_union([gsp_regions[region_id],
                                                          shape(f["geometry"]).buffer(0)])
        else:
            raise GenericException("Encountered an error while extracting GSP region data from ESO "
                                   "API.")
        gsp_regions = {region_id: (gsp_regions[region_id], gsp_regions[region_id].bounds)
                       for region_id in gsp_regions}
        with open(self.gsp_boundaries_20181031_cache_file, "wb") as pickle_fid:
            pickle.dump(gsp_regions, pickle_fid)
        logging.info("20181031 GSP boundaries extracted and pickled to '%s'",
                     self.gsp_boundaries_20181031_cache_file)
        return gsp_regions

    def _load_gsp_boundaries_20220314(self):
        """
        Load the 20220314 GSP / GNode boundaries, either from local cache if available, else fetch
        from ESO Data Portal API.
        """
        if os.path.isfile(self.gsp_boundaries_20220314_cache_file):
            logging.debug("Loading 20220314 GSP boundaries from cache ('%s')",
                          self.gsp_boundaries_20220314_cache_file)
            with open(self.gsp_boundaries_20220314_cache_file, "rb") as pickle_fid:
                return pickle.load(pickle_fid)
        logging.info("Extracting the 20220314 GSP boundary data from NGESO's Data Portal API (this "
                     "only needs to be done once)")
        eso_url = "https://data.nationalgrideso.com/backend/dataset/2810092e-d4b2-472f-b955-d8bea01f9ec0/resource/08534dae-5408-4e31-8639-b579c8f1c50b/download/gsp_regions_20220314.geojson"
        success, api_response = self._fetch_from_api(eso_url)
        if success:
            raw = json.loads(api_response.text)
            gsp_regions = gpd.GeoDataFrame.from_features(raw["features"],
                                                         crs=raw["crs"]["properties"]["name"])
            gsp_regions.geometry = gsp_regions.buffer(0)
        else:
            raise GenericException("Encountered an error while extracting GSP region data from ESO "
                                   "API.")
        ### For backwards compatibility pending https://github.com/SheffieldSolar/Geocode/issues/6
        gsp_regions["bounds"] = gsp_regions.bounds.apply(tuple, axis=1)
        gsp_regions_dict = gsp_regions.set_index(["GSPs", "GSPGroup"]).to_dict("index")
        for r in gsp_regions_dict:
            gsp_regions_dict[r] = tuple(gsp_regions_dict[r].values())
        ######
        with open(self.gsp_boundaries_20220314_cache_file, "wb") as pickle_fid:
            pickle.dump(gsp_regions_dict, pickle_fid)
        logging.info("20220314 GSP boundaries extracted and pickled to '%s'",
                     self.gsp_boundaries_20220314_cache_file)
        return gsp_regions_dict

    def _load_dno_boundaries(self):
        """
        Load the DNO License Area boundaries, either from local cache if available, else fetch from
        ESO Data Portal API.
        """
        if os.path.isfile(self.dno_boundaries_cache_file):
            logging.debug("Loading DNO boundaries from cache ('%s')",
                          self.dno_boundaries_cache_file)
            with open(self.dno_boundaries_cache_file, "rb") as pickle_fid:
                return pickle.load(pickle_fid)
        logging.info("Extracting the DNO License Area boundary data from NGESO's Data Portal API "
                     "(this only needs to be done once)")
        eso_url = "http://data.nationalgrideso.com/backend/dataset/0e377f16-95e9-4c15-a1fc-49e06a39cfa0/resource/e96db306-aaa8-45be-aecd-65b34d38923a/download/dno_license_areas_20200506.geojson"
        success, api_response = self._fetch_from_api(eso_url)
        if success:
            raw = json.loads(api_response.text)
            dno_regions = {}
            dno_names = {}
            for f in raw["features"]:
                region_id = f["properties"]["ID"]
                dno_regions[region_id] = shape(f["geometry"]).buffer(0)
                dno_names[region_id] = (f["properties"]["Name"], f["properties"]["LongName"])
        else:
            raise GenericException("Encountered an error while extracting DNO License Area "
                                   "boundary data from ESO API.")
        dno_regions = {region_id: (dno_regions[region_id], dno_regions[region_id].bounds)
                       for region_id in dno_regions}
        with open(self.dno_boundaries_cache_file, "wb") as pickle_fid:
            pickle.dump((dno_regions, dno_names), pickle_fid)
        logging.info("DNO License Area boundaries extracted and pickled to '%s'",
                     self.dno_boundaries_cache_file)
        return dno_regions, dno_names

    def get_dno_regions(self):
        """
        Get the DNO License Area Boundaries from the ESO Data Portal.

        Returns
        -------
        `dno_regions` : dict
            Dict whose keys are the region IDs and whose values are a tuple containing:
            (region_boundary, region_bounds). The region boundary is a Shapely
            Polygon/MultiPolygon and the bounds are a tuple containing (xmin, ymin, xmax, ymax).
        `dno_names` : dict
            Dict whose keys are the region IDs and whose values are a tuple containing:
            (Name, LongName).
        """
        if self.dno_regions is None:
            self.dno_regions = self._load_dno_boundaries()
        return self.dno_regions

    def _load_gsp_lookup_20181031(self):
        """Load the lookup of Region <-> GSP <-> GNode."""
        if os.path.isfile(self.gsp_lookup_20181031_cache_file):
            with open(self.gsp_lookup_20181031_cache_file, "rb") as pickle_fid:
                return pickle.load(pickle_fid)
        logging.info("Extracting the GSP lookup data from NGESO's Data Portal API (this only needs "
                     "to be done once)")
        eso_url = "http://data.nationalgrideso.com/backend/dataset/2810092e-d4b2-472f-b955-d8bea01f9ec0/resource/bbe2cc72-a6c6-46e6-8f4e-48b879467368/download/gsp_gnode_directconnect_region_lookup.csv"
        success, api_response = self._fetch_from_api(eso_url)
        if success:
            f = StringIO(str(api_response.text).replace("\ufeff", "")) # Remove BOM character
            gsp_lookup = pd.read_csv(f)
            gsp_lookup = gsp_lookup.loc[gsp_lookup.region_id.notnull()].convert_dtypes()
        else:
            raise GenericException("Encountered an error while extracting GSP lookup data from ESO "
                                   "API.")
        with open(self.gsp_lookup_20181031_cache_file, "wb") as pickle_fid:
            pickle.dump(gsp_lookup, pickle_fid)
        logging.info("GSP lookup extracted and pickled to '%s'", self.gsp_lookup_20181031_cache_file)
        return gsp_lookup

    def _load_datazone_lookup(self):
        """Load a lookup of Scottish LLSOA <-> Datazone."""
        if os.path.isfile(self.dz_lookup_cache_file):
            logging.debug("Loading LLSOA<->Datazone lookup from cache ('%s')",
                          self.dz_lookup_cache_file)
            with open(self.dz_lookup_cache_file, "rb") as pickle_fid:
                return pickle.load(pickle_fid)
        with zipfile.ZipFile(self.nrs_zipfile, "r") as nrs_zip:
            with nrs_zip.open("OA_DZ_IZ_2011.csv", "r") as fid:
                dz_lookup = pd.read_csv(fid)
        dz_lookup.set_index("OutputArea2011Code", inplace=True)
        dz_lookup.drop(columns=["IntermediateZone2011Code"], inplace=True)
        dz_lookup = dz_lookup.to_dict()["DataZone2011Code"]
        with open(self.dz_lookup_cache_file, "wb") as pickle_fid:
            pickle.dump(dz_lookup, pickle_fid)
        return dz_lookup

    def _load_constituency_lookup(self):
        """Load a lookup of UK constituency -> Geospatial Centroid."""
        if os.path.isfile(self.constituency_cache_file):
            logging.debug("Loading Constituency lookup from cache ('%s')",
                          self.constituency_cache_file)
            with open(self.constituency_cache_file, "rb") as pickle_fid:
                return pickle.load(pickle_fid)
        logging.info("Extracting the Constituency Centroids data (this only needs to be done "
                     "once)")
        constituency_lookup = {}
        with open(self.constituency_lookup_file) as fid:
            for line in fid:
                _, name, longitude, latitude = line.strip().split("|")
                match_str = name.strip().replace(" ", "").replace(",", "").lower()
                constituency_lookup[match_str] = (float(latitude), float(longitude))
        with open(self.constituency_cache_file, "wb") as pickle_fid:
            pickle.dump(constituency_lookup, pickle_fid)
        logging.info("Constituency lookup extracted and pickled to '%s'",
                     self.constituency_cache_file)
        return constituency_lookup

    def cpo_geocode(self, pcs: pd.DataFrame) -> pd.DataFrame:
        """
        Geocode multiple postcodes using Code Point Open.

        Parameters
        ----------
        `pcs` : Pandas.DataFrame
            A Pandas DataFrame containing the postcodes (or partial postcodes) to geocode. Must
            contain the column 'input_postcode'.

        Returns
        -------
        Pandas.DataFrame
            A Pandas DataFrame with columns: input_postcode (str), latitude (float), longitude
            (float), match_status (int). The `match_status` shows the quality of the postcode lookup
            - use the class attribute *self.status_codes* (a dict) to get a string representation.
        """
        pcs["id"] = np.arange(pcs.shape[0])
        pcs["postcode_to_match"] = pcs.input_postcode.str.strip().str.upper().str.replace(" ", "")
        if self.cpo is None:
            self.cpo = self._load_code_point_open()
        pcs = pcs.merge(self.cpo[["Postcode", "latitude", "longitude"]], how="left",
                        left_on="postcode_to_match", right_on="Postcode")
        pcs = pcs.groupby("id").agg(input_postcode=("input_postcode", "first"),
                                    postcode_to_match=("postcode_to_match", "first"),
                                    postcode_matched=("Postcode", "first"),
                                    latitude=("latitude", np.nanmean),
                                    longitude=("longitude", np.nanmean)).reset_index()
        success = pcs.postcode_matched.notnull()
        pcs.loc[success, "match_status"] = 1
        if not pcs.postcode_matched.isnull().values.any():
            return pcs[["input_postcode", "latitude", "longitude", "match_status"]]
        cpo_geocode_one = lambda p: self.cpo_geocode_one(p.input_postcode)
        pcs.loc[~success, ["latitude", "longitude", "match_status"]] = \
            pcs.loc[~success].apply(cpo_geocode_one, axis=1)
        return pcs[["input_postcode", "latitude", "longitude", "match_status"]]

    def cpo_geocode_one(self, postcode: str) -> pd.Series:
        """
        Use the OS Code Point Open Database to geocode a postcode (or partial postcode using as much
        accuracy as the partial code permits).

        Parameters
        ----------
        `postcode` : string
            The postcode (or partial postcode) to geocode.

        Returns
        -------
        Pandas.Series
            A Pandas Series with fields: input_postcode (str), latitude (float), longitude (float),
            match_status (int). The status code shows the quality of the postcode lookup - use the
            class attribute *self.status_codes* (a dict) to get a string representation.
        """
        if self.cpo is None:
            self.cpo = self._load_code_point_open()
        postcode = postcode.upper()
        # match = self.cpo[self.cpo["Postcode"] == postcode.replace(" ", "")].agg("mean",
                                                                                # numeric_only=True)
        # if not np.isnan(match.latitude):
            # match["match_status"] = 1
            # return match[["latitude", "longitude", "match_status"]]
        if " " in postcode:
            outward, inward = postcode.split(" ", 1)
            myfilter = (self.cpo["outward_postcode"] == outward) & \
                       (self.cpo["inward_postcode"].str.slice(0, len(inward)) == inward)
        else:
            outward = postcode
            myfilter = self.cpo["outward_postcode"] == outward
        match = self.cpo[myfilter].agg("mean", numeric_only=True)
        if not np.isnan(match.latitude):
            match["match_status"] = 2
            return match[["latitude", "longitude", "match_status"]]
        return pd.Series({"latitude": np.nan, "longitude": np.nan, "match_status": 0})

    def gmaps_geocode_one(self, postcode: str, address: Optional[str] = None) -> pd.Series:
        """
        Use the GMaps API to geocode a postcode (or partial postcode) and/or an address.

        Parameters
        ----------
        `postcode` : string
            The postcode (or partial postcode) to geocode.
        `address` : string
            The address to geocode.

        Returns
        -------
        Pandas.Series
            A Pandas Series with fields: input_postcode (str), latitude (float), longitude (float),
            match_status (int). The status code shows the quality of the postcode lookup - use the
            class attribute *self.status_codes* (a dict) to get a string representation.
        """
        if postcode is None and address is None:
            raise GenericException("You must pass either postcode or address, or both.")
        if self.gmaps_key is None:
            self.gmaps_key = self._load_gmaps_key()
            if self.gmaps_key is not None:
                self.gmaps = googlemaps.Client(key=self.gmaps_key)
        if self.gmaps_cache is None:
            self.gmaps_cache = self._load_gmaps_cache()
        sep = ", " if address and postcode else ""
        postcode = postcode if postcode is not None else ""
        address = address if address is not None else ""
        search_term = f"{address}{sep}{postcode}"
        logging.debug("Querying Google Maps Geocoder API for '%s'", search_term)
        if search_term in self.gmaps_cache:
            geocode_result = self.gmaps_cache[search_term]
        else:
            if self.gmaps_key is None:
                return pd.Series({"latitude": np.nan, "longitude": np.nan, "match_status": 0})
            geocode_result = self.gmaps.geocode(search_term, region="uk")
            self.gmaps_cache[search_term] = geocode_result
        if not geocode_result or len(geocode_result) > 1:
            return pd.Series({"latitude": np.nan, "longitude": np.nan, "match_status": 0})
        geometry = geocode_result[0]["geometry"]
        ok_loc_types = ["ROOFTOP", "GEOMETRIC_CENTER"]
        if geometry["location_type"] in ok_loc_types or \
            geocode_result[0]["types"] == ["postal_code"]:
            return pd.Series({"latitude": geometry["location"]["lat"],
                              "longitude": geometry["location"]["lng"],
                              "match_status": 3})
        return pd.Series({"latitude": np.nan, "longitude": np.nan, "match_status": 0})

    def geocode(self,
                postcodes: Optional[Iterable[str]] = None,
                addresses: Optional[Iterable[str]] = None) -> List[Tuple[float, float, int]]:
        """
        Geocode several postcodes and/or addresses.

        Parameters
        ----------
        `postcodes` : iterable of strings
            The postcodes (or partial postcodes) to geocode. Must align with *addresses* if both are
            passed.
        `addresses` : iterable of strings
            The addresses to geocode. Must align with *postcodes* if both are passed. If addresses
            are passed, the GMaps API will be used since OS Code Point Open does not provide address
            lookup.

        Returns
        -------
        list of tuples
            The output list will align with the input postcodes/addresses, with each element being a
            tuple containing: latitude (float), longitude (float), status code (int). The status
            code shows the quality of the postcode lookup - use the class attribute
            *self.status_codes* (a dict) to get a string representation.

        Notes
        -----
        The input iterables can be any Python object which can be interpreted by Pandas.DataFrame()
        e.g. a list, tuple, Numpy array etc.
        If you pass only postcodes, this method will priotiise OS Code Point Open as the geocoder.
        If a postcode fails to geocode using OS CPO, it will be geocoded with GMaps.
        """
        results = []
        if postcodes is None and addresses is None:
            raise GenericException("You must pass either postcodes or addresses, or both.")
        postcodes = [None for a in addresses] if postcodes is None else list(postcodes)
        addresses = [None for p in postcodes] if addresses is None else list(addresses)
        logging.debug("Geocoding %s postcodes (%s addresses)", len(postcodes), len(addresses))
        inputs = pd.DataFrame({"input_postcode": postcodes, "input_address": addresses})
        inputs["id"] = np.arange(inputs.shape[0])
        results = inputs.merge(self.cache, how="left", on=["input_postcode", "input_address"])
        results_cols = ["latitude", "longitude", "match_status"]
        if not results.latitude.isnull().any():
            return results[results_cols].to_records(index=False)
        use_cpo = results.latitude.isnull() & results.input_address.isnull()
        if use_cpo.any():
            logging.debug("Geocoding %s postcodes using Code Point Open", use_cpo.sum())
            results.loc[use_cpo, results_cols] = \
                self.cpo_geocode(inputs.loc[use_cpo, ["input_postcode"]])[results_cols]
            logging.debug("Successfully geocoded %s postcodes using Code Point Open",
                          (results.latitude.notnull()).sum())
        use_gmaps = results.latitude.isnull()
        if use_gmaps.any():
            logging.debug("Geocoding %s postcodes/addresses using Google Maps", use_gmaps.sum())
            gmaps_geocode = lambda i: self.gmaps_geocode_one(i.input_postcode, i.input_address)
            results.loc[use_gmaps, results_cols] = \
                inputs.loc[use_gmaps].apply(gmaps_geocode, axis=1)[results_cols]
            logging.debug("Successfully geocoded %s postcodes/addresses using Google Maps",
                          (use_gmaps & results.latitude.notnull()).sum())
        results["match_status"] = results.match_status.astype(int)
        logging.debug("Adding postcodes/addresses to cache")
        cache = pd.concat([self.cache, results[results.latitude.notnull()].drop(columns="id")],
                          ignore_index=True)
        self.cache = cache.drop_duplicates(subset=["input_postcode", "input_address"], keep="last",
                                           ignore_index=True)
        return results[results_cols].to_records(index=False)

    def geocode_one(self,
                    postcode: Optional[str] = None,
                    address: Optional[str] = None) -> Tuple[float, float, int]:
        """
        Geocode a single postcode and/or address.

        Parameters
        ----------
        `postcode` : string
            The postcode (or partial postcode) to geocode.
        `address` : string
            The address to geocode. If address is passed, the GMaps API will be used since OS Code
            Point Open does not provide address lookup.

        Returns
        -------
        tuple
            A tuple containing: latitude (float), longitude (float), status code (int). The status
            code shows the quality of the postcode lookup - use the class attribute
            *self.status_codes* (a dict) to get a string representation.

        Notes
        -----
        If you pass only postcode, this method will priotiise OS Code Point Open as the geocoder.
        If a postcode fails to geocode using OS CPO, it will be geocoded with GMaps.
        """
        if postcode is None and address is None:
            raise GenericException("You must specify either a postcode or an address, or both.")
        if address is None:
            lat, lon, status = self.cpo_geocode_one(postcode)
            if status > 0:
                return lat, lon, status
        return self.gmaps_geocode_one(postcode, address).to_records(index=False)

    def geocode_llsoa(self,
                      llsoa: Union[str, Iterable[str]]
                     ) -> Union[Tuple[float, float], List[Tuple[float, float]]]:
        """
        Geocode an LLSOA using the Population Weighted Centroid.

        Parameters
        ----------
        `llsoa` : string or iterable of strings
            The LLSOA(s) to geocode.

        Returns
        -------
        Tuple or List of Tuples
            If a single LLSOA was passed (i.e. a string), the output be a tuple containing: latitude
            (float), longitude (float). If several LLSOAs are passed (i.e. an iterable of strings),
            the output will be a list of tuples which aligns with the input iterable.

        Notes
        -----
        The input *llsoa* iterable can be any Python object which can be looped over with a for loop
        e.g. a list, tuple, Numpy array etc.
        LLSOAs are identified by their LLSOA Code, sometimes abbreviated to `llsoacd`.
        In Scotland, one can use either LLSOA codes or Datazones, this method supports both.
        """
        if self.llsoa_lookup is None:
            self.llsoa_lookup = self._load_llsoa_lookup()
        if isinstance(llsoa, str):
            return self._llsoa_centroid(llsoa)
        results = []
        tot = len(llsoa)
        for i, llsoa_ in enumerate(llsoa):
            results.append(self._llsoa_centroid(llsoa_))
            if self.progress_bar and (i % 10 == 0 or i == tot - 1):
                print_progress(i+1, tot, prefix=self.prefix+"[Geocode]     LLSOA", suffix="",
                               decimals=2, bar_length=100)
        return results

    def reverse_geocode_llsoa(self,
                              latlons: List[Tuple[float, float]],
                              datazones: bool = False) -> List[str]:
        """
        Reverse-geocode latitudes and longitudes to LLSOA.

        Parameters
        ----------
        `latlons` : list of tuples
            A list of tuples containing (latitude, longitude).
        `datazones` : bool
            Set this to True to return Datazones rather than LLSOA codes in Scotland (default is
            False).

        Returns
        -------
        list of strings
            The LLSOA codes that the input latitudes and longitudes fall within. Any lat/lons which
            do not fall inside an LLSOA boundary will return None.
        """
        if not SHAPELY_AVAILABLE:
            raise GenericException("Geocode was unable to import the Shapely library, follow the "
                                   "installation instructions at "
                                   "https://github.com/SheffieldSolar/Geocode")
        if self.llsoa_regions is None:
            self.llsoa_regions = self._load_llsoa_boundaries()
        results = self._reverse_geocode(latlons, self.llsoa_regions)
        if datazones:
            if self.dz_lookup is None:
                self.dz_lookup = self._load_datazone_lookup()
            results = [llsoacd if llsoacd not in self.dz_lookup else self.dz_lookup[llsoacd]
                       for llsoacd in results]
        return results

    def _reverse_geocode(self,
                         coords: List[Tuple[float, float]],
                         regions: Dict) -> List:
        """
        Generic method to reverse-geocode x, y coordinates to regions.

        Parameters
        ----------
        `coords` : list of tuples
            A list of tuples containing (y, x).
        `regions` : dict
            Dict whose keys are the region IDs and whose values are a tuple containing:
            (region_boundary, region_bounds). The region boundary must be a Shapely
            Polygon/MultiPolygon and the bounds should be a tuple containing (xmin, ymin, xmax,
            ymax).

        Returns
        -------
        list
            The region IDs that the input coords fall within. Any coords which do not fall inside an
            LLSOA boundary will return None.

        Notes
        -----
        The region bounds are used to improve performance by first scanning for potential region
        candidates using a simple inequality, since the performance of
        `Shapely.MultiPolygon.contains()` is not great.
        """
        results = []
        tot = len(coords)
        for i, (y, x) in enumerate(coords):
            success = False
            possible_matches = []
            for reg_id in regions:
                bounds = regions[reg_id][1]
                if bounds[0] <= x <= bounds[2] and bounds[1] <= y <= bounds[3]:
                    possible_matches.append(reg_id)
            for reg_id in possible_matches:
                if regions[reg_id][0].contains(Point(x, y)):
                    results.append(reg_id)
                    if self.progress_bar and (i % 10 == 0 or i == tot - 1):
                        print_progress(i+1, tot, prefix=self.prefix+"[Geocode]     REVERSE-GEOCODE",
                                       suffix="", decimals=2, bar_length=100)
                    success = True
                    break
            if not success:
                results.append(None)
        return results

    def reverse_geocode_gsp(self,
                            latlons: List[Tuple[float, float]]
                           ) -> Tuple[List[int], List[List[Dict]]]:
        """
        Reverse-geocode latitudes and longitudes to GSP using the 20220314 definitions.

        Parameters
        ----------
        `latlons` : list of tuples
            A list of tuples containing (latitude, longitude).

        Returns
        -------
        `results` : list of ints
            A list of tuples containing (<GSPs>, <GSPGroup>), aligned with the input *latlons*.

        Notes
        -----
        Return format needs some work, maybe switch to DataFrames in future release.
        """
        if not SHAPELY_AVAILABLE:
            raise GenericException("Geocode was unable to import the Shapely library, follow the "
                                   "installation instructions at "
                                   "https://github.com/SheffieldSolar/Geocode")
        logging.debug("Reverse geocoding %s latlons to 20220314 GSP", len(latlons))
        if self.gsp_regions is None:
            self.gsp_regions = self._load_gsp_boundaries_20220314()
        lats = [l[0] for l in latlons]
        lons = [l[1] for l in latlons]
        # Rather than re-project the region boundaries, re-project the input lat/lons
        # (easier, but slightly slower if reverse-geocoding a lot)
        logging.debug("Converting latlons to BNG")
        eastings, northings = self._latlon2bng(lons, lats)
        logging.debug("Reverse geocoding")
        results = self._reverse_geocode(list(zip(northings, eastings)), self.gsp_regions)
        return results

    def reverse_geocode_gsp_20181031(self,
                                     latlons: List[Tuple[float, float]]
                                    ) -> Tuple[List[int], List[List[Dict]]]:
        """
        Reverse-geocode latitudes and longitudes to GSP using the 20181031 definitions.

        Parameters
        ----------
        `latlons` : list of tuples
            A list of tuples containing (latitude, longitude).

        Returns
        -------
        `results` : list of ints
            A list of region IDs, aligned with the input *latlons*.
        `results_more` : list of lists of dicts
            The full MANY:MANY lookup giving the Region <-> GSP <-> GNode which the lat/lon falls
            within. The relationship between GSP: GNode is MANY:MANY, so each element of the outer
            list is another list of matches, each element of which is a dictionary giving the
            matched GSP / GNode.

        Notes
        -----
        Return format needs some work, maybe switch to DataFrames in future release.
        """
        if not SHAPELY_AVAILABLE:
            raise GenericException("Geocode was unable to import the Shapely library, follow the "
                                   "installation instructions at "
                                   "https://github.com/SheffieldSolar/Geocode")
        if self.gsp_regions_20181031 is None:
            self.gsp_regions_20181031 = self._load_gsp_boundaries_20181031()
        lats = [l[0] for l in latlons]
        lons = [l[1] for l in latlons]
        # Rather than re-project the region boundaries, re-project the input lat/lons
        # (easier, but slightly slower if reverse-geocoding a lot)
        eastings, northings = self._latlon2bng(lons, lats)
        results = self._reverse_geocode(list(zip(northings, eastings)), self.gsp_regions_20181031)
        if self.gsp_lookup_20181031 is None:
            self.gsp_lookup_20181031 = self._load_gsp_lookup_20181031()
        lookup = self.gsp_lookup_20181031
        reg_lookup = {r: lookup[lookup.region_id == r].to_dict(orient="records")
                      for r in list(set(results))}
        results_more = [reg_lookup[r] if r is not None else None for r in results]
        return results, results_more

    def _llsoa_centroid(self, llsoa):
        """Lookup the PWC for a given LLSOA code."""
        try:
            return self.llsoa_lookup[llsoa]
        except KeyError:
            return None, None

    def geocode_constituency(self,
                             constituency: Union[str, Iterable[str]]
                            ) -> Union[Tuple[float, float], List[Tuple[float, float]]]:
        """
        Geocode a UK Constituency using the geospatial centroid.

        Parameters
        ----------
        `constituency` : string or iterable of strings
            The constituency names to geocode.

        Returns
        -------
        tuple or list of tuples
            If a single constituency name was passed (i.e. a string), the output be a tuple
            containing: latitude (float), longitude (float). If several constituency names are
            passed (i.e. an iterable of strings), the output will be a list of tuples which aligns
            with the input iterable.

        Notes
        -----
        The input *constituency* iterable can be any Python object which can be looped over with a
        for loop e.g. a list, tuple, Numpy array etc.
        Constituencies are identified by their full name, case-insensitive, ignoring spaces.
        """
        if self.constituency_lookup is None:
            self.constituency_lookup = self._load_constituency_lookup()
        if isinstance(constituency, str):
            return self._constituency_centroid(constituency)
        results = []
        tot = len(constituency)
        for i, constituency_ in enumerate(constituency):
            results.append(self._constituency_centroid(constituency_))
            if self.progress_bar and (i % 10 == 0 or i == tot - 1):
                print_progress(i+1, tot, prefix=self.prefix+"[Geocode]     CONSTITUENCY", suffix="",
                               decimals=2, bar_length=100)
        return results

    def _constituency_centroid(self, constituency):
        """Lookup the GC for a given constituency."""
        try:
            match_str = constituency.strip().replace(" ", "").replace(",", "").lower()
            return self.constituency_lookup[match_str]
        except KeyError:
            return None, None

    @staticmethod
    def _bng2latlon(eastings: Iterable[Union[float, int]],
                    northings: Iterable[Union[float, int]]) -> Tuple[List[float], List[float]]:
        """
        Convert Eastings and Northings (a.k.a British National Grid a.k.a OSGB 1936) to latitudes
        and longitudes (WGS 1984).

        Parameters
        ----------
        `eastings` : iterable of floats or ints
            Easting co-ordinates.
        `northings` : iterable of floats or ints
            Northing co-ordinates.

        Returns
        -------
        `lons` : list of floats
            Corresponding longitude co-ordinates in WGS 1984 CRS.
        `lats` : list of floats
            Corresponding latitude co-ordinates in WGS 1984 CRS.

        Notes
        -----
        Be careful! This method uses the same convention of ordering (eastings, northings) and
        (lons, lats) as pyproj i.e. (x, y). Elsewhere in this module the convention is typically
        (lats, lons) due to personal preference.
        """
        proj = pyproj.Transformer.from_crs(27700, 4326, always_xy=True)
        lons, lats = proj.transform(eastings, northings)
        return lons, lats

    @staticmethod
    def _latlon2bng(lons: List[float], lats: List[float]) -> Tuple[List[float], List[float]]:
        """
        Convert latitudes and longitudes (WGS 1984) to Eastings and Northings (a.k.a British
        National Grid a.k.a OSGB 1936).

        Parameters
        ----------
        `lons` : list of floats
            Corresponding longitude co-ordinates in WGS 1984 CRS.
        `lats` : list of floats
            Corresponding latitude co-ordinates in WGS 1984 CRS.

        Returns
        -------
        `eastings` : list of floats or ints
            Easting co-ordinates.
        `northings` : list of floats or ints
            Northing co-ordinates.

        Notes
        -----
        Be careful! This method uses the same convention of ordering (eastings, northings) and
        (lons, lats) as pyproj i.e. (x, y). Elsewhere in this module the convention is typically
        (lats, lons) due to personal preference.
        """
        proj = pyproj.Transformer.from_crs(4326, 27700, always_xy=True)
        eastings, northings = proj.transform(lons, lats)
        return eastings, northings

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

def query_yes_no(question, default="yes"):
    """
    Ask a yes/no question via input() and return the answer as boolean.

    Parameters
    ----------
    `question` : string
        The question presented to the user.
    `default` : string
        The presumed answer if the user just hits <Enter>. It must be "yes" (the default), "no" or
        None (meaning an answer is required of the user).

    Returns
    -------
    boolean
        Return value is True for "yes" or False for "no".

    Notes
    -----
    See http://stackoverflow.com/a/3041990
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)
    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        if choice in valid:
            return valid[choice]
        sys.stdout.write("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")

def parse_options():
    """Parse command line options."""
    parser = argparse.ArgumentParser(description=("This is a command line interface (CLI) for "
                                                  f"the Geocode module version {__version__}."),
                                     epilog="Jamie Taylor, 2019-10-08")
    parser.add_argument("--clear-cache", dest="clear_cache", action="store_true",
                        required=False, help="Specify to delete the cache files.")
    parser.add_argument("--debug", dest="debug", action="store_true",
                        required=False, help="Geocode some sample postcodes/addresses/LLSOAs.")
    parser.add_argument("--setup", dest="setup", action="store_true",
                        required=False, help="Force download all datasets to local cache (useful "
                                             "if running inside a Docker container i.e. run this "
                                             "as part of image build).")
    parser.add_argument("--load-cpo-zip", dest="cpo_zip", action="store", type=str,
                        required=False, default=None, metavar="</path/to/zip-file>",
                        help="Load the Code Point Open data from a local zip file.")
    parser.add_argument("--load-gmaps-key", dest="gmaps_key", action="store", type=str,
                        required=False, default=None, metavar="<gmaps-api-key>",
                        help="Load a Google Maps API key.")
    options = parser.parse_args()
    return options

def debug():
    """Useful for debugging code (runs each public method in turn with sample inputs)."""
    logging.info("Running some example code (`--debug`)")
    timerstart = TIME.time()
    sample_llsoas = ["E01025397", "E01003065", "E01017548", "E01023301", "E01021142", "E01019037",
                     "E01013873", "S00092417", "S01012390"]
    logging.info("Geocoding some LSOAs")
    with Geocoder(progress_bar=False) as geocoder:
        results = geocoder.geocode_llsoa(sample_llsoas)
    logging.info("Time taken: {:.1f} seconds".format(TIME.time() - timerstart))
    for llsoa, (lat, lon) in zip(sample_llsoas, results):
        logging.info("%s :    %s, %s", llsoa, lat, lon)
    sample_latlons = [
        (53.705, -2.328), (51.430, -0.093), (52.088, -0.457), (51.706, -0.036), (50.882, 0.169),
        (50.409, -4.672), (52.940, -1.146), (57.060, -2.874), (56.31, -4.)
    ]
    timerstart = TIME.time()
    logging.info("Reverse geocoding some latlons to LSOAs")
    with Geocoder(progress_bar=False) as geocoder:
        results = geocoder.reverse_geocode_llsoa(sample_latlons, datazones=True)
    logging.info("Time taken: %s seconds", round(TIME.time() - timerstart, 1))
    for (lat, lon), llsoa in zip(sample_latlons, results):
        logging.info("%s, %s :    %s", lat, lon, llsoa)
    sample_file = os.path.join(SCRIPT_DIR, "sample_latlons.txt")
    with open(sample_file) as fid:
        sample_latlons = [tuple(map(float, line.strip().split(",")))
                          for line in fid if line.strip()][:10]
    timerstart = TIME.time()
    logging.info("Reverse geocoding some latlons to GSPs")
    with Geocoder(progress_bar=False) as geocoder:
        results = geocoder.reverse_geocode_gsp(sample_latlons)
    logging.info("Time taken: %s seconds", round(TIME.time() - timerstart, 1))
    for (lat, lon), region_id in zip(sample_latlons, results):
        logging.info("%s, %s :    %s", lat, lon, region_id)
    sample_constituencies = ["Berwickshire Roxburgh and Selkirk", "Argyll and Bute",
                             "Inverness Nairn Badenoch and Strathspey", # missing commas :(
                             "Dumfries and Galloway"]
    timerstart = TIME.time()
    logging.info("Geocoding some constituencies")
    with Geocoder(progress_bar=False) as geocoder:
        results = geocoder.geocode_constituency(sample_constituencies)
    logging.info("Time taken: %s seconds", round(TIME.time() - timerstart, 1))
    for constituency, (lat, lon) in zip(sample_constituencies, results):
        logging.info("%s :    %s, %s", constituency, lat, lon)
    sample_file = os.path.join(SCRIPT_DIR, "sample_postcodes.txt")
    with open(sample_file) as fid:
        postcodes = [line.strip() for line in fid if line.strip()][:10]
    timerstart = TIME.time()
    logging.info("Geocoding some postcodes")
    with Geocoder(progress_bar=False) as geocoder:
        results = geocoder.geocode(postcodes=postcodes)
    logging.info("Time taken: %s seconds", round(TIME.time() - timerstart, 1))
    for postcode, (lat, lon, status) in zip(postcodes, results):
        logging.info("%s :    %s, %s    -> %s", postcode, lat, lon, geocoder.status_codes[status])

def main():
    """Run the Command Line Interface."""
    options = parse_options()
    if options.clear_cache:
        geocoder = Geocoder()
        geocoder.clear_cache()
    if options.cpo_zip is not None:
        logging.info("Updating Code Point Open data")
        with Geocoder() as geocoder:
            copyfile(options.cpo_zip, geocoder.cpo_zipfile)
            logging.debug("Copied file '%s' to '%s'", options.cpo_zip, geocoder.cpo_zipfile)
            geocoder._load_code_point_open(force_reload=True)
        logging.info("Finished updating Code Point Open data")
    if options.gmaps_key is not None:
        logging.info("Loading GMaps key")
        with Geocoder() as geocoder:
            with open(geocoder.gmaps_key_file, "w") as fid:
                fid.write(options.gmaps_key)
            if geocoder._load_gmaps_key() == options.gmaps_key:
                logging.info("GMaps key saved to '%s'", geocoder.gmaps_key_file)
    if options.setup:
        logging.info("Running forced setup")
        with Geocoder() as geocoder:
            geocoder.force_setup()
    if options.debug:
        debug()

if __name__ == "__main__":
    DEFAULT_FMT = "%(asctime)s [%(levelname)s] [%(filename)s:%(funcName)s] - %(message)s"
    FMT = os.environ.get("GEOCODE_LOGGING_FMT", DEFAULT_FMT)
    DATEFMT = os.environ.get("GEOCODE_LOGGING_DATEFMT", "%Y-%m-%dT%H:%M:%SZ")
    logging.basicConfig(format=FMT, datefmt=DATEFMT, level=os.environ.get("LOGLEVEL", "DEBUG"))
    main()
