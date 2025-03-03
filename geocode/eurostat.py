"""
Manage data from Eurostat, primarily Nomenclature of territorial units for statistics (NUTS)
regions.

- Jamie Taylor <jamie.taylor@sheffield.ac.uk>
- First Authored: 2023-03-14
"""

import os
import sys
import zipfile
import json
import csv
import logging
from pathlib import Path
from typing import Optional, Iterable, Tuple, Union, List, Dict, Literal

import pandas as pd
import geopandas as gpd
import shapefile

from . import utilities as utils

SCRIPT_DIR = Path(os.path.dirname(os.path.realpath(__file__)))

class Eurostat:
    """
    Manage data from Eurostat.
    """
    def __init__(self, cache_manager, proxies=None, ssl_verify=True):
        """
        Manage data from Eurostat.
        """
        self.cache_manager = cache_manager
        self.data_dir = SCRIPT_DIR.joinpath("eurostat")
        self.nuts_regions = {
            (l, y): None for y in [2003, 2006, 2010, 2013, 2016, 2021] for l in range(4)
        }
        self.proxies = proxies
        self.ssl_verify = ssl_verify

    def force_setup(self):
        """
        Function to setup all lookup files.
        """
        for l in range(0, 4):
            self._load_nuts_boundaries(l)

    def load_nuts_boundaries(self,
                             level: Literal[0, 1, 2, 3],
                             year: Literal[2003, 2006, 2010, 2013, 2016, 2021] = 2021
                             ) -> gpd.GeoDataFrame:
        """
        Load the NUTS boundaries, either from local cache if available, else fetch from Eurostat
        API.

        Parameters
        ----------
        `level` : int
            Specify the NUTS level, must be one of [0,1,2,3].
        `year` : int
            Specify the year of NUTS regulation, must be one of [2003,2006,2010,2013,2016,2021],
            defaults to 2021.

        Returns
        -------
        Geopandas GeoDataFrame
            Contains columns NUTS_ID, LEVL_CODE, CNTR_CODE, NAME_LATN, NUTS_NAME, MOUNT_TYPE,
            URBN_TYPE, COAST_TYPE, FID, id, geometry
        """
        cache_label = f"nuts_{year}_{level}"
        nuts_boundaries_cache_contents = self.cache_manager.retrieve(cache_label)
        if nuts_boundaries_cache_contents is not None:
            logging.debug("Loading %s NUTS%s boundaries from cache ('%s')",
                          year, level, cache_label)
            return nuts_boundaries_cache_contents
        logging.info("Extracting the %s NUTS%s boundary data from Eurostat (this only needs to be "
                     "done once)", year, level)
        eurostat_url = ("https://gisco-services.ec.europa.eu/distribution/v2/nuts/geojson/"
                        f"NUTS_RG_01M_{year}_4326_LEVL_{level}.geojson")
        success, api_response = utils.fetch_from_api(
            eurostat_url,
            proxies=self.proxies,
            ssl_verify=self.ssl_verify
        )
        if success:
            raw = json.loads(api_response.text)
            nuts_regions = gpd.GeoDataFrame.from_features(raw["features"],
                                                          crs=raw["crs"]["properties"]["name"])
            nuts_regions.geometry = nuts_regions.buffer(0)
        else:
            raise utils.GenericException(f"Encountered an error while extracting {year} NUTS{level} "
                                         "region data from Eurostat API.")
        self.cache_manager.write(cache_label, nuts_regions)
        logging.info("%s NUTS%s boundaries extracted and pickled to file ('%s')",
                     year, level, cache_label)
        return nuts_regions

    def _load_nuts_boundaries(self, level, year=2021):
        """
        For backwards compatibility pending https://github.com/SheffieldSolar/Geocode/issues/6

        Load the NUTS boundaries, either from local cache if available, else fetch from Eurostat
        API.
        """
        nuts_gdf = self.load_nuts_boundaries(level, year)
        nuts_gdf["bounds"] = nuts_gdf.bounds.apply(tuple, axis=1)
        nuts_dict = nuts_gdf[["NUTS_ID", "geometry", "bounds"]].set_index("NUTS_ID")\
                                                               .to_dict("index")
        for r in nuts_dict:
            nuts_dict[r] = tuple(nuts_dict[r].values())
        return nuts_dict

    def reverse_geocode_nuts(self,
                             latlons: List[Tuple[float, float]],
                             level: Literal[0, 1, 2, 3],
                             year: Literal[2003, 2006, 2010, 2013, 2016, 2021] = 2021
                            ) -> List[str]:
        """
        Reverse-geocode latitudes and longitudes to NUTS regions.

        Parameters
        ----------
        `latlons` : list of tuples
            A list of tuples containing (latitude, longitude).
        `level` : int
            Specify the NUTS level, must be one of [0,1,2,3].
        `year` : int
            Specify the year of NUTS regulation, must be one of [2003,2006,2010,2013,2016,2021],
            defaults to 2021.

        Returns
        -------
        list of strings
            The NUTS_ID codes that the input latitudes and longitudes fall within. Any lat/lons which
            do not fall inside a NUTS boundary will return None.
        """
        if self.nuts_regions[(level, year)] is None:
            self.nuts_regions[(level, year)] = self._load_nuts_boundaries(level=level, year=year)
        results = utils.reverse_geocode(latlons, self.nuts_regions[(level, year)])
        return results

