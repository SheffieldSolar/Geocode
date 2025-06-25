"""
Manage data from Ordnance Survey's Code Point Open dataset.

- Ethan Jones <ejones18@sheffield.ac.uk>
- Jamie Taylor <jamie.taylor@sheffield.ac.uk>
- First Authored: 2022-10-19
"""

import os
import sys
import pickle
import zipfile
import logging
from pathlib import Path
from typing import Optional, Iterable, Tuple, Union, List, Dict

import pandas as pd
import numpy as np

from . utilities import GenericException, bng2latlon

SCRIPT_DIR = Path(os.path.dirname(os.path.realpath(__file__)))

class CodePointOpen:
    """The Code Point Open data manager for the Geocode class."""
    def __init__(self, cache_manager):
        """The Code Point Open data manager for the Geocode class."""
        self.cache_manager = cache_manager
        self.cpo_zipfile = SCRIPT_DIR.joinpath("code_point_open", "codepo_gb.zip")
        self.cpo = None
        self.cache = None

    def force_setup(self):
        """
        Function to setup the OS Code Point Open Database.
        """
        self._load(force_reload=True)

    def _load(self, force_reload : bool = False):
        """Load the OS Code Point Open Database, either from raw zip file or local cache."""
        if self.cpo is not None and not force_reload:
            return
        cpo_cache_name = "code_point_open"
        cpo_cache_contents = self.cache_manager.retrieve(cpo_cache_name)
        if cpo_cache_contents is not None and not force_reload:
            logging.debug("Loading Code Point Open data from cache ('%s')", cpo_cache_name)
            self.cpo = cpo_cache_contents
            return
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
        # remove all rows with 0 Eastings or Northings
        cpo = cpo[~((cpo["Eastings"] == 0) & (cpo["Northings"] == 0))]
        nn_indices = cpo["Eastings"].notnull() & cpo["Positional_quality_indicator"] < 90
        lons, lats = bng2latlon(cpo.loc[nn_indices, ("Eastings")].to_numpy(),
                                cpo.loc[nn_indices, ("Northings")].to_numpy())
        cpo.loc[nn_indices, "longitude"] = lons
        cpo.loc[nn_indices, "latitude"] = lats
        cpo["outward_postcode"] = cpo["Postcode"].str.slice(0, -3).str.strip()
        cpo["inward_postcode"] = cpo["Postcode"].str.slice(-3).str.strip()
        self.cache_manager.write(cpo_cache_name, cpo)
        logging.info("Code Point Open extracted and pickled to cache")
        self.cpo = cpo
        return
    
    def geocode_postcode(self,
                         postcodes: Iterable[str]) -> List[Tuple[float, float, int]]:
        """
        Geocode several postcodes using CodePointOpen.

        Parameters
        ----------
        `postcode` : iterable of strings
            The postcodes (or partial postcodes) to geocode.

        Returns
        -------
        list of tuples
            The output list will align with the input postcodes, with each element being a
            tuple containing: latitude (float), longitude (float), status code (int). The status
            code shows the quality of the postcode lookup - use the class attribute
            *Geocode.status_codes* (a dict) to get a string representation.

        Notes
        -----
        The input iterables can be any Python object which can be interpreted by Pandas.DataFrame()
        e.g. a list, tuple, Numpy array etc.
        """
        if self.cache is None:
            self._load_cache()
        results = []
        logging.debug("Geocoding %s postcodes using Code Point Open", len(postcodes))
        inputs = pd.DataFrame({"input_postcode": postcodes})
        inputs["id"] = np.arange(inputs.shape[0])
        results = inputs.merge(self.cache, how="left", on=["input_postcode"])
        results_cols = ["latitude", "longitude", "match_status"]
        if not results.latitude.isnull().any():
            return results[results_cols].to_records(index=False)
        new_postcodes = results.latitude.isnull()
        inputs_ = inputs.loc[new_postcodes, ["input_postcode", "id"]]
        inputs_["postcode_to_match"] = inputs_.input_postcode.str.strip().str.upper().str.replace(" ", "")
        self._load()
        results_ = inputs_.merge(self.cpo[["Postcode", "latitude", "longitude"]], how="left",
                                 left_on="postcode_to_match", right_on="Postcode")
        results_ = results_.groupby("id").agg(input_postcode=("input_postcode", "first"),
                                              postcode_to_match=("postcode_to_match", "first"),
                                              postcode_matched=("Postcode", "first"),
                                              latitude=("latitude", np.nanmean),
                                              longitude=("longitude", np.nanmean)).reset_index()
        success = results_.postcode_matched.notnull()
        results_.loc[success, "match_status"] = 1
        results = pd.concat((results.loc[~new_postcodes], results_))
        if results.latitude.isnull().any():
            cpo_geocode_one = lambda p: self.geocode_one(p.input_postcode)
            results.loc[~success, ["latitude", "longitude", "match_status"]] = \
                results.loc[~success].apply(cpo_geocode_one, axis=1)
        results["match_status"] = results.match_status.astype(int)
        logging.debug("Adding postcodes to lookup")
        cache = pd.concat([self.cache, 
                           results[results.latitude.notnull()].drop(columns="id")],
                           ignore_index=True)
        self.cache = cache.drop_duplicates(subset=["input_postcode"], 
                                           keep="last", ignore_index=True)
        return results.sort_values("id")[results_cols].to_records(index=False)

    def geocode_one(self, postcode: str) -> pd.Series:
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
        self._load()
        try:
            postcode = postcode.upper()
        except:
            return pd.Series({"latitude": np.nan, "longitude": np.nan, "match_status": 0})
        
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

    def _load_cache(self):
        """Load the cache of prior postcodes for better performance."""
        if self.cache is None:
            self.cache = pd.DataFrame({"input_postcode": [], "input_address": [], "latitude": [],
                                       "longitude": [], "match_status": []})
        return
    