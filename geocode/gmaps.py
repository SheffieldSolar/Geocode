"""
Manage data from Google Maps.

- Ethan Jones <ejones18@sheffield.ac.uk>
- Jamie Taylor <jamie.taylor@sheffield.ac.uk>
- First Authored: 2022-10-19
"""

import os
import sys
import logging
import pickle
from pathlib import Path
from typing import Optional, Iterable, Tuple, Union, List, Dict

import numpy as np
import pandas as pd
import googlemaps

from . utilities import GenericException

SCRIPT_DIR = Path(os.path.dirname(os.path.realpath(__file__)))

class GMaps:
    """The Gmaps data manager for the Geocode class."""
    def __init__(self, cache_manager, gmaps_key_file=None, proxies=None, ssl_verify=True):
        """The Gmaps data manager for the Geocode class."""
        self.cache_manager = cache_manager
        self.gmaps_key = None
        self.gmaps_client = None
        self.cache_file = "gmaps_cache"
        self.gmaps_key_file = gmaps_key_file if gmaps_key_file is not None \
                                  else self.cache_manager.cache_dir.joinpath("key.txt")
        self._load_cache()
        self.cache_modified = False
        self.proxies = proxies
        self.ssl_verify = ssl_verify

    def __enter__(self):
        """Context manager."""
        return self

    def __exit__(self, type, value, traceback):
        """Context manager - flush GMaps cache on exit."""
        self.flush_cache()

    def _load_key(self):
        """Load the user's GMaps API key from installation directory."""
        try:
            with open(self.gmaps_key_file) as fid:
                key = fid.read().strip()
        except FileNotFoundError:
            logging.warning("Failed to load Google Maps API key from '%s' - you "
                            "will not be able to make new queries to the Google Maps API!",
                            self.gmaps_key_file)
            return None
        return key

    def flush_cache(self):
        """Flush any new GMaps API queries to the GMaps cache."""
        if self.cache_modified:
            self.cache_manager.write(self.cache_file, self.cache)

    def geocode_postcode(self, postcode: [str],
                         address: Optional[str] = None) -> Union[Tuple[float, float], List[Tuple[float, float]]]:
        """
        Geocode several postcodes and/or addresses.

        Parameters
        ----------
        `postcodes` : iterable of strings
            The postcodes (or partial postcodes) to geocode. Must align with *addresses* if both are
            passed.
        `addresses` : iterable of strings
            The addresses to geocode. Must align with *postcodes* if both are passed.

        Returns
        -------
        tuple or list of tuples
            If a single constituency name was passed (i.e. a string), the output be a tuple
            containing: latitude (float), longitude (float). If several constituency names are
            passed (i.e. an iterable of strings), the output will be a list of tuples which aligns
            with the input iterable.
        """
        address = [None for a in address] if address is None else list(address)
        logging.debug("Geocoding %s postcodes (%s addresses)", len(postcode), len(address))
        results = []
        for pc, addr in zip(postcode, address):
            results.append(self.geocode_one(postcode=pc, address=addr))
        return results

    def geocode_one(self, postcode: str, address: Optional[str] = None) -> pd.Series:
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
            raise utils.GenericException("You must pass either postcode or address, or both.")
        if self.gmaps_key is None:
            self.gmaps_key = self._load_key()
            if self.gmaps_key is not None:
                self.gmaps_client = googlemaps.Client(
                    key=self.gmaps_key,
                    proxies=self.proxies,
                    ssl_verify=self.ssl_verify
                )
        if self.cache is None:
            self._load_cache()
        sep = ", " if address and postcode else ""
        postcode = postcode if postcode is not None else ""
        address = address if address is not None else ""
        search_term = f"{address}{sep}{postcode}"
        if search_term in self.cache:
            logging.debug("Loading GMaps Geocoder API result from cache: '%s'", search_term)
            geocode_result = self.cache[search_term]
        else:
            logging.debug("Querying Google Maps Geocoder API for '%s'", search_term)
            if self.gmaps_key is None:
                return pd.Series({"latitude": np.nan, "longitude": np.nan, "match_status": 0})
            geocode_result = self.gmaps_client.geocode(search_term, region="uk")
            self.cache[search_term] = geocode_result
            self.cache_modified = True
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

    def _load_cache(self):
        """Load the cache of prior addresses/postcodes for better performance."""
        self.cache = self.cache_manager.retrieve(self.cache_file)
        if self.cache is None:
            self.cache = {}
        return

