"""
Manage data from the NGESO Data Portal.

- Ethan Jones <ejones18@sheffield.ac.uk>
- Jamie Taylor <jamie.taylor@sheffield.ac.uk>
- First Authored: 2022-10-19
"""

import io
import os
import sys
import pickle
import logging
import json
from pathlib import Path
from zipfile import ZipFile
from typing import Optional, Iterable, Tuple, Union, List, Dict

import pandas as pd
import geopandas as gpd
try:
    from shapely.geometry import shape, Point
    from shapely.ops import unary_union
except ImportError:
    logging.warning("Failed to import Shapely library - you will not be able to reverse-geocode! "
                    "See notes in the README about installing Shapely on Windows machines.")

from . import utilities as utils

SCRIPT_DIR = Path(os.path.dirname(os.path.realpath(__file__)))

class NationalGrid:
    """The NGESO data manager for the Geocode class."""
    def __init__(self, cache_manager, proxies=None, ssl_verify=True):
        """The NGESO data manager for the Geocode class."""
        self.cache_manager = cache_manager
        self.gsp_lookup_20181031_cache_file = "gsp_lookup_20181031"
        self.gsp_boundaries_20250109_cache_file = "gsp_boundaries_20250109"
        self.gsp_boundaries_20220314_cache_file = "gsp_boundaries_20220314"
        self.gsp_boundaries_20181031_cache_file = "gsp_boundaries_20181031"
        self.dno_boundaries_cache_file = "dno_boundaries"
        self.gsp_regions_dict = None
        self.gsp_regions_20181031 = None
        self.dno_regions = None
        self.gsp_lookup_20181031 = None
        self.proxies = proxies
        self.ssl_verify = ssl_verify

    def force_setup(self):
        """
        Function to setup lookup files.
        """
        self.load_gsp_boundaries("20220314")
        self.load_gsp_boundaries("20250109")
        self._load_dno_boundaries()

    def _load_gsp_lookup_20181031(self):
        """Load the lookup of Region <-> GSP <-> GNode."""
        gsp_lookup_cache_contents = self.cache_manager.retrieve(self.gsp_lookup_20181031_cache_file)
        if gsp_lookup_cache_contents is not None:
            logging.debug("Loading GSP lookup data from cache ('%s')",
                          self.gsp_lookup_20181031_cache_file)
            return gsp_lookup_cache_contents
        logging.info("Extracting the GSP lookup data from NGESO's Data Portal API (this only needs "
                     "to be done once)")
        eso_url = "https://api.neso.energy/dataset/2810092e-d4b2-472f-b955-d8bea01f9ec0/resource/bbe2cc72-a6c6-46e6-8f4e-48b879467368/download/gsp_gnode_directconnect_region_lookup.csv"
        success, api_response = utils.fetch_from_api(
            eso_url,
            proxies=self.proxies,
            ssl_verify=self.ssl_verify
        )
        if success:
            f = StringIO(str(api_response.text).replace("\ufeff", "")) # Remove BOM character
            gsp_lookup = pd.read_csv(f)
            gsp_lookup = gsp_lookup.loc[gsp_lookup.region_id.notnull()].convert_dtypes()
        else:
            raise utils.GenericException("Encountered an error while extracting GSP lookup data from ESO "
                                   "API.")
        self.cache_manager.write(self.gsp_lookup_20181031_cache_file, gsp_lookup)
        logging.info("GSP lookup extracted and pickled to '%s'", self.gsp_lookup_20181031_cache_file)
        return gsp_lookup
    
    def _load_gsp_boundaries_20250109(self):
        """
        Load the 20250109 GSP / GNode boundaries, either from local cache if available, else fetch
        from ESO Data Portal API.

        Returns
        -------
        gsp_regions: GeoPandas.GeoDataFrame
            A geodataframe of MultiPolygons for the GSP boundaries.
        gsp_regions_dict: Dict
            GSP boundaries as a dictionary for backwards compatibility with utilities methods.
        """
        gsp_boundaries_cache_contents = self.cache_manager.retrieve(self.gsp_boundaries_20250109_cache_file)
        if gsp_boundaries_cache_contents is not None:
            logging.debug("Loading 20250109 GSP boundaries from cache ('%s')",
                          self.gsp_boundaries_20250109_cache_file)
            return gsp_boundaries_cache_contents
        logging.info("Extracting the 20250109 GSP boundary data from NGESO's Data Portal API (this "
                     "only needs to be done once)")
        eso_url = "https://api.neso.energy/dataset/2810092e-d4b2-472f-b955-d8bea01f9ec0/resource/d95e8c1b-9cd9-41dd-aacb-4b53b8c07c20/download/gsp_regions_20250109.zip"
        success, api_response = utils.fetch_from_api(
            eso_url,
            proxies=self.proxies,
            ssl_verify=self.ssl_verify
        )
        if success:
            zip_file = io.BytesIO(api_response.content)
            target_file = "Proj_27700/GSP_regions_27700_20250109.geojson"
            with ZipFile(zip_file, "r") as zip_ref:  
                if target_file in zip_ref.namelist():
                    with zip_ref.open(target_file) as file:
                        raw = json.loads(file.read())
                        gsp_regions = gpd.GeoDataFrame.from_features(raw["features"],
                                                                    crs=raw["crs"]["properties"]["name"])
                        gsp_regions.geometry = gsp_regions.buffer(0)
        else:
            raise utils.GenericException("Encountered an error while extracting GSP region data from ESO "
                                   "API.")
        ### For backwards compatibility pending https://github.com/SheffieldSolar/Geocode/issues/6
        gsp_regions_ = gsp_regions.dissolve(by=["GSPs", "GSPGroup"])
        gsp_regions_["bounds"] = gsp_regions_.bounds.apply(tuple, axis=1)
        gsp_regions_dict = gsp_regions_.to_dict(orient="index")
        for r in gsp_regions_dict:
            gsp_regions_dict[r] = tuple(gsp_regions_dict[r].values())
        ######
        self.cache_manager.write(self.gsp_boundaries_20250109_cache_file, (gsp_regions, gsp_regions_dict))
        logging.info("20250109 GSP boundaries extracted and pickled to '%s'",
                     self.gsp_boundaries_20250109_cache_file)
        return gsp_regions, gsp_regions_dict

    def _load_gsp_boundaries_20220314(self):
        """
        Load the 20220314 GSP / GNode boundaries, either from local cache if available, else fetch
        from ESO Data Portal API.

        Returns
        -------
        gsp_regions: GeoPandas.GeoDataFrame
            A geodataframe of MultiPolygons for the GSP boundaries.
        gsp_regions_dict: Dict
            GSP boundaries as a dictionary for backwards compatibility with utilities methods.
        """
        gsp_boundaries_cache_contents = self.cache_manager.retrieve(self.gsp_boundaries_20220314_cache_file)
        if gsp_boundaries_cache_contents is not None:
            logging.debug("Loading 20220314 GSP boundaries from cache ('%s')",
                          self.gsp_boundaries_20220314_cache_file)
            return gsp_boundaries_cache_contents
        logging.info("Extracting the 20220314 GSP boundary data from NGESO's Data Portal API (this "
                     "only needs to be done once)")
        eso_url = "https://api.neso.energy/dataset/2810092e-d4b2-472f-b955-d8bea01f9ec0/resource/08534dae-5408-4e31-8639-b579c8f1c50b/download/gsp_regions_20220314.geojson"
        success, api_response = utils.fetch_from_api(
            eso_url,
            proxies=self.proxies,
            ssl_verify=self.ssl_verify
        )
        if success:
            raw = json.loads(api_response.text)
            gsp_regions = gpd.GeoDataFrame.from_features(raw["features"],
                                                         crs=raw["crs"]["properties"]["name"])
            gsp_regions.geometry = gsp_regions.buffer(0)
        else:
            raise utils.GenericException("Encountered an error while extracting GSP region data from ESO "
                                   "API.")
        ### For backwards compatibility pending https://github.com/SheffieldSolar/Geocode/issues/6
        gsp_regions["bounds"] = gsp_regions.bounds.apply(tuple, axis=1)
        gsp_regions_dict = gsp_regions.set_index(["GSPs", "GSPGroup"]).to_dict("index")
        for r in gsp_regions_dict:
            gsp_regions_dict[r] = tuple(gsp_regions_dict[r].values())
        ######
        self.cache_manager.write(self.gsp_boundaries_20220314_cache_file, (gsp_regions, gsp_regions_dict))
        logging.info("20220314 GSP boundaries extracted and pickled to '%s'",
                     self.gsp_boundaries_20220314_cache_file)
        return gsp_regions, gsp_regions_dict

    def load_gsp_boundaries(self, version: str):
        """
        Load the GSP boundaries.

        Parameters
        ----------
        `version` : str
            The version of the GSP boundaries to load.

        Returns
        -------
        gsp_regions: GeoPandas.GeoDataFrame
            A geodataframe of MultiPolygons for the GSP boundaries.
        gsp_regions_dict: Dict
            GSP boundaries as a dictionary for backwards compatibility with utilities methods.
        """
        if version == "20250109":
            return self._load_gsp_boundaries_20250109()
        elif version == "20220314":
            return self._load_gsp_boundaries_20220314()
        else:
            raise ValueError(f"GSP boundaries version {version} is not supported.")

    def _load_dno_boundaries(self):
        """
        Load the DNO License Area boundaries, either from local cache if available, else fetch from
        ESO Data Portal API.
        """
        dno_boundaries_cache_contents = self.cache_manager.retrieve(self.dno_boundaries_cache_file)
        if dno_boundaries_cache_contents is not None:
            logging.debug("Loading DNO boundaries from cache ('%s')",
                          self.dno_boundaries_cache_file)
            return dno_boundaries_cache_contents
        logging.info("Extracting the DNO License Area boundary data from NGESO's Data Portal API "
                     "(this only needs to be done once)")
        eso_url = "https://api.neso.energy/dataset/0e377f16-95e9-4c15-a1fc-49e06a39cfa0/resource/e96db306-aaa8-45be-aecd-65b34d38923a/download/dno_license_areas_20200506.geojson"
        success, api_response = utils.fetch_from_api(
            eso_url,
            proxies=self.proxies,
            ssl_verify=self.ssl_verify
        )
        if success:
            raw = json.loads(api_response.text)
            dno_regions = {}
            dno_names = {}
            for f in raw["features"]:
                region_id = f["properties"]["ID"]
                dno_regions[region_id] = shape(f["geometry"]).buffer(0)
                dno_names[region_id] = (f["properties"]["Name"], f["properties"]["LongName"])
        else:
            raise utils.GenericException("Encountered an error while extracting DNO License Area "
                                   "boundary data from ESO API.")
        dno_regions = {region_id: (dno_regions[region_id], dno_regions[region_id].bounds)
                       for region_id in dno_regions}
        self.cache_manager.write(self.dno_boundaries_cache_file, (dno_regions, dno_names))
        logging.info("DNO License Area boundaries extracted and pickled to '%s'",
                     self.dno_boundaries_cache_file)
        return dno_regions, dno_names

    def reverse_geocode_gsp(self,
                            latlons: List[Tuple[float, float]],
                            version: str
                           ) -> Tuple[List[int], List[List[Dict]]]:
        """
        Reverse-geocode latitudes and longitudes to GSP using the 20220314 definitions.

        Parameters
        ----------
        `latlons` : list of tuples
            A list of tuples containing (latitude, longitude).
        `version` : string

        Returns
        -------
        `results` : list of ints
            A list of tuples containing (<GSPs>, <GSPGroup>), aligned with the input *latlons*.

        Notes
        -----
        Return format needs some work, maybe switch to DataFrames in future release.
        """
        logging.debug(f"Reverse geocoding {len(latlons)} latlons to {version} GSP")
        if self.gsp_regions_dict is None:
            _, self.gsp_regions_dict = self.load_gsp_boundaries(version=version)
        lats = [l[0] for l in latlons]
        lons = [l[1] for l in latlons]
        # Rather than re-project the region boundaries, re-project the input lat/lons
        # (easier, but slightly slower if reverse-geocoding a lot)
        logging.debug("Converting latlons to BNG")
        eastings, northings = utils.latlon2bng(lons, lats)
        logging.debug("Reverse geocoding")
        results = utils.reverse_geocode(list(zip(northings, eastings)), self.gsp_regions_dict)
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
        if self.gsp_regions_20181031 is None:
            self.gsp_regions_20181031 = self.load_gsp_boundaries_20181031()
        lats = [l[0] for l in latlons]
        lons = [l[1] for l in latlons]
        eastings, northings = utils.latlon2bng(lons, lats)
        results = utils.reverse_geocode(list(zip(northings, eastings)), self.gsp_regions_20181031)
        if self.gsp_lookup_20181031 is None:
            self.gsp_lookup_20181031 = self.load_gsp_lookup_20181031()
        lookup = self.gsp_lookup_20181031
        reg_lookup = {r: lookup[lookup.region_id == r].to_dict(orient="records")
                      for r in list(set(results))}
        results_more = [reg_lookup[r] if r is not None else None for r in results]
        return results, results_more
