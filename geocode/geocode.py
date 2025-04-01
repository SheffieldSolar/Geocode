#!/usr/bin/env python3

"""
Geocode various geographical entities including postcodes and LLSOAs. Reverse-geocode to LLSOA or GSP.

- Jamie Taylor <jamie.taylor@sheffield.ac.uk>
- Ethan Jones <ejones18@sheffield.ac.uk>
- First Authored: 2019-10-08
"""

import os
import sys
import logging
from pathlib import Path
import time as TIME
import argparse
from shutil import copyfile
from typing import Optional, Iterable, Tuple, Union, List, Dict, Literal

import pyproj

from . utilities import GenericException
from . cpo import CodePointOpen
from . ngeso import NationalGrid
from . ons_nrs import ONS_NRS
from . eurostat import Eurostat
from . gmaps import GMaps
from . cache_manager import CacheManager
from . version import __version__

SCRIPT_DIR = Path(os.path.dirname(os.path.realpath(__file__)))

class Geocoder:
    """
    Geocode addresses, postcodes, LLSOAs or Constituencies or reverse-geocode latitudes and
    longitudes.
    """
    def __init__(self,
                 cache_dir: Optional[Path] = None,
                 gmaps_key_file: Optional[Path] = None,
                 proxies: Optional[Dict] = None,
                 ssl_verify: bool = True) -> None:
        """
        Geocode addresses, postcodes, LLSOAs or Constituencies or reverse-geocode latitudes and
        longitudes.

        Parameters
        ----------
        `cache_dir` : string
            Optionally specify a directory to use for caching.
        `gmaps_key_file` : string
            Path to an API key file for Google Maps Geocode API.
        `proxies` : Optional[Dict]
            Optionally specify a Dict of proxies for http and https requests in the format:
            {"http": "<address>", "https": "<address>"}
        `ssl_verify` : Boolean
            Set to False to disable SSL verification when downloading data from APIs. Defaults to
            True.
        """
        self.cache_manager = CacheManager(cache_dir)
        self.cache_manager.clear(delete_gmaps_cache=False, old_versions_only=True)
        self.cpo = CodePointOpen(self.cache_manager)
        self.ngeso = NationalGrid(self.cache_manager, proxies=proxies, ssl_verify=ssl_verify)
        self.ons_nrs = ONS_NRS(self.cache_manager, proxies=proxies, ssl_verify=ssl_verify)
        self.eurostat = Eurostat(self.cache_manager, proxies=proxies, ssl_verify=ssl_verify)
        self.gmaps = GMaps(
            self.cache_manager, gmaps_key_file, proxies=proxies, ssl_verify=ssl_verify
        )
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

    def __exit__(self, type, value, traceback):
        """Context manager."""
        self.gmaps.flush_cache()

    def force_setup(self, ngeso_setup=True, cpo_setup=True, ons_setup=True, eurostat_setup=True):
        """Download all data and setup caches."""
        if ngeso_setup:
            self.ngeso.force_setup()
        if cpo_setup:
            self.cpo.force_setup()
        if ons_setup:
            self.ons_nrs.force_setup()
        if eurostat_setup:
            self.eurostat.force_setup()

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
        return self.ngeso._load_dno_boundaries()

    def get_gsp_regions(self, **kwargs):
        """
        Get the GSP / GNode boundaries from the ESO Data Portal API.
        """
        version = kwargs.get("version", "20250109")
        return self.ngeso.load_gsp_boundaries(version)

    def get_llsoa_boundaries(self):
        """
        Load the LLSOA boundaries, either from local cache if available, else fetch from raw API
        (England and Wales) and packaged data (Scotland).
        """
        return self.ons_nrs._load_llsoa_boundaries()

    def geocode_llsoa(self, llsoa_boundaries):
        """
        Function to geocode a collection of llsoa boundaries into latlons.
        
        Parameters
        ----------
        `llsoa_boundaries` : iterable of strings
            Specific llsoa boundaries to geocode to latlons
        """
        return self.geocode(llsoa_boundaries, "llsoa")

    def reverse_geocode_llsoa(self, latlons, dz=True):
        """
        Function to reverse geocode a collection of latlons into llsoa boundaries.
        
        Parameters
        ----------
        `latlons` : iterable of strings
            Specific latlons to geocode to llsoa boundaries.
        `dz` : Boolean
            Indication whether to consider datazones
        """
        return self.reverse_geocode(latlons, "llsoa", datazones=dz)

    def reverse_geocode_nuts(self, latlons: List[Tuple[float, float]],
                             level: Literal[0, 1, 2, 3],
                             year: Literal[2003, 2006, 2010, 2013, 2016, 2021] = 2021
                            ) -> List[str]:
        """
        Function to reverse geocode a collection of latlons into NUTS boundaries.
        
        Parameters
        ----------
        `latlons` : iterable of strings
            Specific latlons to geocode to llsoa boundaries.
        `level` : int
            Specify the NUTS level, must be one of [0,1,2,3].
        `year` : int
            Specify the year of NUTS regulation, must be one of [2003,2006,2010,2013,2016,2021],
            defaults to 2021.
        """
        return self.reverse_geocode(latlons, "nuts", level=level, year=year)

    def geocode_constituency(self, constituencies):
        """
        Function to geocode a collection of constituencies into latlons.
        
        Parameters
        ----------
        `constituencies` : iterable of strings
            Specific constituencies to geocode to latlons
        """
        return self.geocode(constituencies, "constituency")

    def geocode_local_authority(self, lads):
        """
        Function to geocode a collection of LADs (Local Authority Districts) into latlons.
        
        Parameters
        ----------
        `lads` : iterable of strings
            Specific LADs to geocode to latlons
        """
        return self.geocode(lads, "lad")

    def reverse_geocode_gsp(self, latlons, **kwargs):
        """
        Function to reverse geocode a collection of latlons into gsp regions.
        
        Parameters
        ----------
        `latlons` : iterable of strings
            Specific latlons to geocode to gsp regions.
        `**kwargs`
            Options to pass to the underlying reverse_geocode_gsp method.
        """
        return self.reverse_geocode(latlons, "gsp", **kwargs)

    def geocode_postcode(self, postcodes, method="cpo"):
        """
        Function to geocode a collection of postcodes into latlons.
        
        Parameters
        ----------
        `postcodes` : iterable of strings
            Specific postcodes to geocode to latlons
        """
        return self.geocode(postcodes, "postcode", method=method)

    def geocode(self, entity_ids, entity, **kwargs):
        """
        Geocode a selection of GSP regions, llsoa boundaries, constituencies, LADs, postcodes or
        addresses to latitudes and longitudes.

        Parameters
        ----------
        `entity_ids` : iterable of strings
            The specific entities to Geocode.
        `entity` : string
            Specify the entity type to Geocode from i.e., lad or postcode.
        `**kwargs`
            Options to pass to the underlying geocode method.
        """
        entity = entity.lower()
        if entity == "gsp":
            raise GenericException(f"Entity '{entity}' is not supported.")
        elif entity == "llsoa":
            return self.ons_nrs.geocode_llsoa(llsoa=entity_ids)
        elif entity == "constituency":
            return self.ons_nrs.geocode_constituency(constituency=entity_ids)
        elif entity == "lad":
            return self.ons_nrs.geocode_local_authority(local_authority=entity_ids)
        elif entity == "postcode":
            method = kwargs.get("method", "cpo").lower()
            address = kwargs.get("address", None)
            if address is None:
                if method.replace(" ", "") in ["cpo", "codepointopen"]:
                    return self.cpo.geocode_postcode(postcodes=entity_ids)
                elif method.replace(" ", "") in ["gmaps", "googlemaps"]:
                    return self.gmaps.geocode_postcode(postcode=entity_ids)
            else:
                return self.gmaps.geocode_postcode(postcode=entity_ids, address=address)
        else:
            raise GenericException(f"Entity '{entity}' is not supported.")

    def reverse_geocode(self, latlons, entity, **kwargs):
        """
        Reverse geocode a set of latitudes and longitudes to either GSP regions or llsoa boundaries.

        Parameters
        ----------
        `latlons` : list of tuples
            A list of tuples containing (latitude, longitude).
        `entity` : string
            Specify the entity type to Geocode from i.e., gsp or llsoa.
        `**kwargs`
            Options to pass to the underlying reverse-geocode method.
        """
        entity = entity.lower()
        if entity == "gsp":
            version = kwargs.get("version", "20250109")
            return self.ngeso.reverse_geocode_gsp(latlons, version)
        elif entity == "llsoa":
            datazones = kwargs.get("datazones", False)
            return self.ons_nrs.reverse_geocode_llsoa(latlons=latlons, datazones=datazones)
        elif entity == "nuts":
            level = kwargs.get("level")
            year = kwargs.get("year", 2021)
            return self.eurostat.reverse_geocode_nuts(latlons=latlons, level=level, year=year)
        else:
            raise GenericException(f"Entity '{entity}' is not supported.")

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

def parse_options():
    """Parse command line options."""
    parser = argparse.ArgumentParser(description=("This is a command line interface (CLI) for "
                                                  f"the Geocode module version {__version__}."),
                                     epilog="Jamie Taylor & Ethan Jones, 2019-10-08")
    parser.add_argument("--clear-cache", dest="clear_cache", action="store_true",
                        required=False, help="Specify to delete the cache files.")
    parser.add_argument("--debug", dest="debug", action="store_true",
                        required=False, help="Geocode some sample postcodes/addresses/LLSOAs.")
    parser.add_argument("--setup", dest="setup", action="store", nargs="+", default=None,
                        required=False, help="Force download all datasets to local cache (useful "
                                             "if running inside a Docker container i.e. run this "
                                             "as part of image build). Possible values are "
                                             "'ngeso', 'cpo', 'ons', 'eurostat' or 'all'.")
    parser.add_argument("--load-cpo-zip", dest="cpo_zip", action="store", type=str,
                        required=False, default=None, metavar="</path/to/zip-file>",
                        help="Load the Code Point Open data from a local zip file.")
    parser.add_argument("--load-gmaps-key", dest="gmaps_key", action="store", type=str,
                        required=False, default=None, metavar="<gmaps-api-key>",
                        help="Load a Google Maps API key.")
    options = parser.parse_args()
    def handle_options(options):
        if options.setup is not None:
            valid_options = ["ngeso", "cpo", "ons", "eurostat", "all"]
            options.setup = list(map(str.lower, options.setup))
            if any(s not in valid_options for s in options.setup):
                raise ValueError(f"Invalid value for `--setup` - valid values are {valid_options}")
        return options
    return handle_options(options)

def debug():
    """Useful for debugging code (runs each public method in turn with sample inputs)."""
    logging.info("Running some example code (`--debug`)")
    timerstart = TIME.time()
    sample_llsoas = ["E01025397", "E01003065", "E01017548", "E01023301", "E01021142", "E01019037",
                     "E01013873", "S00092417", "S01012390"]
    logging.info("Geocoding some LSOAs")
    with Geocoder() as geocoder:
        results = geocoder.geocode(entity="llsoa", entity_ids=sample_llsoas)
    logging.info("Time taken: {:.1f} seconds".format(TIME.time() - timerstart))
    for llsoa, (lat, lon) in zip(sample_llsoas, results):
        logging.info("%s :    %s, %s", llsoa, lat, lon)
    sample_latlons = [
        (53.705, -2.328), (51.430, -0.093), (52.088, -0.457), (51.706, -0.036), (50.882, 0.169),
        (50.409, -4.672), (52.940, -1.146), (57.060, -2.874), (56.31, -4.)
    ]
    timerstart = TIME.time()
    logging.info("Reverse geocoding some latlons to LSOAs")
    with Geocoder() as geocoder:
        results = geocoder.reverse_geocode(latlons=sample_latlons, entity="llsoa", datazones=True)
    logging.info("Time taken: %s seconds", round(TIME.time() - timerstart, 1))
    for (lat, lon), llsoa in zip(sample_latlons, results):
        logging.info("%s, %s :    %s", lat, lon, llsoa)
    sample_file = SCRIPT_DIR.joinpath("sample_latlons.txt")
    with open(sample_file) as fid:
        sample_latlons = [tuple(map(float, line.strip().split(",")))
                          for line in fid if line.strip()][:10]
    timerstart = TIME.time()
    logging.info("Reverse geocoding some latlons to GSPs")
    with Geocoder() as geocoder:
        results = geocoder.reverse_geocode(latlons=sample_latlons, entity="gsp")
    logging.info("Time taken: %s seconds", round(TIME.time() - timerstart, 1))
    for (lat, lon), region_id in zip(sample_latlons, results):
        logging.info("%s, %s :    %s", lat, lon, region_id)
    sample_constituencies = ["Berwickshire Roxburgh and Selkirk", "Argyll and Bute",
                             "Inverness Nairn Badenoch and Strathspey",
                             "Dumfries and Galloway"]
    timerstart = TIME.time()
    logging.info("Geocoding some constituencies")
    with Geocoder() as geocoder:
        results = geocoder.geocode(entity="constituency", entity_ids=sample_constituencies)
    logging.info("Time taken: %s seconds", round(TIME.time() - timerstart, 1))
    for constituency, (lat, lon) in zip(sample_constituencies, results):
        logging.info("%s :    %s, %s", constituency, lat, lon)
    sample_file = SCRIPT_DIR.joinpath("sample_postcodes.txt")
    with open(sample_file) as fid:
        postcodes = [line.strip() for line in fid if line.strip()][:10]
    timerstart = TIME.time()
    logging.info("Geocoding some postcodes")
    with Geocoder() as geocoder:
        results = geocoder.geocode(entity="postcode", entity_ids=postcodes)
    logging.info("Time taken: %s seconds", round(TIME.time() - timerstart, 1))
    for postcode, (lat, lon, status) in zip(postcodes, results):
        logging.info("%s :    %s, %s    -> %s", postcode, lat, lon, geocoder.status_codes[status])

def main():
    """Run the Command Line Interface."""
    options = parse_options()
    if options.clear_cache:
        geocoder = Geocoder()
        geocoder.cache_manager.clear()
    if options.cpo_zip is not None:
        logging.info("Updating Code Point Open data")
        with Geocoder() as geocoder:
            copyfile(options.cpo_zip, geocoder.cpo.cpo_zipfile)
            logging.debug("Copied file '%s' to '%s'", options.cpo_zip, geocoder.cpo.cpo_zipfile)
            geocoder.cpo._load(force_reload=True)
        logging.info("Finished updating Code Point Open data")
    if options.gmaps_key is not None:
        logging.info("Loading GMaps key")
        with Geocoder() as geocoder:
            with open(geocoder.gmaps.gmaps_key_file, "w") as fid:
                fid.write(options.gmaps_key)
            if geocoder.gmaps._load_key() == options.gmaps_key:
                logging.info("GMaps key saved to '%s'", geocoder.gmaps.gmaps_key_file)
    if options.setup is not None:
        ngeso_setup =  "ngeso" in options.setup or "all" in options.setup
        cpo_setup = "cpo" in options.setup or "all" in options.setup
        ons_setup = "ons" in options.setup or "all" in options.setup
        eurostat_setup = "eurostat" in options.setup or "all" in options.setup
        logging.info("Running forced setup")
        with Geocoder() as geocoder:
            geocoder.force_setup(ngeso_setup=ngeso_setup, cpo_setup=cpo_setup, ons_setup=ons_setup,
                                 eurostat_setup=eurostat_setup)
    if options.debug:
        debug()

if __name__ == "__main__":
    DEFAULT_FMT = "%(asctime)s [%(levelname)s] [%(filename)s:%(funcName)s] - %(message)s"
    FMT = os.environ.get("GEOCODE_LOGGING_FMT", DEFAULT_FMT)
    DATEFMT = os.environ.get("GEOCODE_LOGGING_DATEFMT", "%Y-%m-%dT%H:%M:%SZ")
    logging.basicConfig(format=FMT, datefmt=DATEFMT, level=os.environ.get("LOGLEVEL", "INFO"))
    main()
