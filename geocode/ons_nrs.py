"""
Manage data from the Office for National Statistics (ONS) and National Records Scotland (NRS).

- Ethan Jones <ejones18@sheffield.ac.uk>
- Jamie Taylor <jamie.taylor@sheffield.ac.uk>
- First Authored: 2022-10-19
"""

import os
import sys
import zipfile
import json
import csv
import logging
from pathlib import Path
from typing import Literal, Optional, Iterable, Tuple, Union, List, Dict

import pandas as pd
import geopandas as gpd
import shapefile

try:
    from shapely.geometry import shape, Point
    from shapely.ops import unary_union
except ImportError:
    logging.warning(
        "Failed to import Shapely library - you will not be able to reverse-geocode! "
        "See notes in the README about installing Shapely on Windows machines."
    )

from . import utilities as utils

SCRIPT_DIR = Path(os.path.dirname(os.path.realpath(__file__)))


class ONS_NRS:
    """
    Manage data from the Office for National Statistics (ONS) and National Records Scotland (NRS).
    """

    def __init__(self, cache_manager, proxies=None, ssl_verify=True):
        """
        Manage data from the Office for National Statistics (ONS) and National Records Scotland
        (NRS).
        """
        self.cache_manager = cache_manager
        self.data_dir = SCRIPT_DIR.joinpath("ons")
        self.nrs_zipfile = self.data_dir.joinpath("nrs_2011.zip")
        self.constituency_lookup_file = self.data_dir.joinpath(
            "constituency_centroids_Dec2020.psv"
        )
        self.lad_lookup_file = self.data_dir.joinpath("lad_centroids_May2021.psv")
        self.pc_llsoa_zipfile = self.data_dir.joinpath(
            "PCD_OA_LSOA_MSOA_LAD_MAY22_UK_LU.zip"
        )
        self.llsoa_lookup = None
        self.llsoa_regions = None
        self.llsoa_reverse_lookup = None
        self.constituency_lookup = None
        self.dz_lookup = None
        self.lad_lookup = None
        self.pc_llsoa_lookup = None
        self.proxies = proxies
        self.ssl_verify = ssl_verify

    def force_setup(self):
        """
        Function to setup all lookup files.
        """
        self._load_llsoa_lookup()
        self._load_constituency_lookup()
        self._load_lad_lookup()
        self._load_postcode_llsoa_lookup()
        for version in ["2011", "2021"]:
            self._load_llsoa_boundaries(version)
            self._load_datazone_lookup(version)

    def _load_llsoa_lookup(self):
        """Load the lookup of LLSOA -> Population Weighted Centroid."""
        cache_label = "llsoa_centroids"
        llsoa_lookup_cache_contents = self.cache_manager.retrieve(cache_label)
        if llsoa_lookup_cache_contents is not None:
            logging.debug("Loading LLSOA lookup from cache ('%s')", cache_label)
            return llsoa_lookup_cache_contents
        logging.info(
            "Extracting the ONS and NRS LLSOA centroids data (this only needs to be done "
            "once)"
        )
        ons_url = {
            "2011": "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LSOA_Dec_2011_PWC_in_England_and_Wales_2022/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=geojson",
            "2021": "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LSOA_PopCentroids_EW_2021_V4/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=geojson",
        }
        pages = utils._fetch_from_ons_api(
            ons_url["2011"], proxies=self.proxies, ssl_verify=self.ssl_verify
        )
        engwales_lookup_2011 = {
            f["properties"]["lsoa11cd"]: tuple(f["geometry"]["coordinates"][::-1])
            for page in pages
            for f in page["features"]
        }
        engwales_lookup_2011 = pd.DataFrame(engwales_lookup_2011).transpose()
        engwales_lookup_2011.columns = ["latitude", "longitude"]
        pages = utils._fetch_from_ons_api(
            ons_url["2021"], proxies=self.proxies, ssl_verify=self.ssl_verify
        )
        engwales_lookup_2021 = {
            f["properties"]["LSOA21CD"]: tuple(f["geometry"]["coordinates"][::-1])
            for page in pages
            for f in page["features"]
        }
        engwales_lookup_2021 = pd.DataFrame(engwales_lookup_2021).transpose()
        engwales_lookup_2021.columns = ["latitude", "longitude"]
        engwales_lookup = pd.concat([engwales_lookup_2011, engwales_lookup_2021])
        # keep latest version of LLSOA where duplicates exist
        engwales_lookup = engwales_lookup[
            ~engwales_lookup.index.duplicated(keep="last")
        ]
        engwales_lookup.reset_index(names="code", inplace=True)

        zip_path_2011 = self.data_dir.joinpath("nrs_2011.zip")
        zip_path_2021 = self.data_dir.joinpath("nrs_2021.zip")

        OA_2011_centroids = gpd.read_file(
            f"zip://{zip_path_2011}!OutputArea2011_PWC_WGS84.csv",
            columns=["code", "easting", "northing"],
        )
        OA_2011_centroids = utils.add_latlon(OA_2011_centroids, "easting", "northing")
        scots_lookup_2011 = OA_2011_centroids[["code", "latitude", "longitude"]]
        OA_2021_centroids = gpd.read_file(
            f"zip://{zip_path_2021}!OutputArea2022_PWC_WGS84.csv",
            columns=["code", "easting", "northing"],
        )
        OA_2021_centroids = utils.add_latlon(OA_2021_centroids, "easting", "northing")
        scots_lookup_2021 = OA_2021_centroids[["code", "latitude", "longitude"]]
        scots_lookup = pd.concat([scots_lookup_2011, scots_lookup_2021]).reset_index(
            drop=True
        )

        DZ_2011_centroids = gpd.read_file(
            f"zip://{zip_path_2011}!SG_DataZone_Cent_2011.csv",
            columns=["DataZone", "Easting", "Northing"],
        )
        DZ_2011_centroids = utils.add_latlon(DZ_2011_centroids, "Easting", "Northing")
        scots_dz_lookup_2011 = DZ_2011_centroids[
            ["DataZone", "latitude", "longitude"]
        ].rename(columns={"DataZone": "code"})
        DZ_2021_centroids = gpd.read_file(
            f"zip://{zip_path_2021}!SG_DataZone_Cent_2022.csv",
            columns=["DataZone", "Easting", "Northing"],
        )
        DZ_2021_centroids = utils.add_latlon(DZ_2021_centroids, "Easting", "Northing")
        scots_dz_lookup_2021 = DZ_2021_centroids[
            ["DataZone", "latitude", "longitude"]
        ].rename(columns={"DataZone": "code"})
        scots_dz_lookup = pd.concat(
            [scots_dz_lookup_2011, scots_dz_lookup_2021]
        ).reset_index(drop=True)

        llsoa_lookup = pd.concat(
            [engwales_lookup, scots_lookup, scots_dz_lookup]
        ).reset_index(drop=True)
        llsoa_lookup.set_index("code", inplace=True)
        self.cache_manager.write(cache_label, llsoa_lookup)
        logging.info(
            "LLSOA centroids extracted and pickled to file ('%s')", cache_label
        )
        return llsoa_lookup

    def _load_llsoa_boundaries_engwales_regions(self, version: Literal["2011", "2021"]):
        """
        Load the LLSOA boundaries, either from local cache if available, else fetch from raw API.

        Parameters
        ----------
        `version` : Literal["2011", "2021"]
            The version of the LLSOA boundaries to load.
        """
        logging.info(
            "Extracting the LLSOA boundary data from ONS (this only needs to be "
            "done once)"
        )
        ons_url = {
            "2011": "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Lower_Layer_Super_Output_Areas_Dec_2011_Boundaries_Full_Extent_BFE_EW_V3_2022/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson",
            "2021": "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BFC_V10/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson",
        }
        pages = utils._fetch_from_ons_api(
            ons_url[version], proxies=self.proxies, ssl_verify=self.ssl_verify
        )
        return gpd.GeoDataFrame(
            {
                "llsoa11cd": [
                    feature["properties"][f"LSOA{version[-2:]}CD"]
                    for page in pages
                    for feature in page["features"]
                ],
                "geometry": [
                    shape(feature["geometry"]).buffer(0)
                    for page in pages
                    for feature in page["features"]
                ],
            },
            crs="EPSG:4326",
        )

    def _load_llsoa_boundaries_scots_regions(self, version: Literal["2011", "2021"]):
        """
        Load the LLSOA boundaries for Scotland from the NRS zipfile.

        Parameters
        ----------
        `version` : Literal["2011", "2021"]
            The version of the LLSOA boundaries to load.
        """
        zip_path = self.data_dir.joinpath(f"nrs_{version}.zip")
        llsoa_filename = {
            "2011": "OutputArea2011_EoR_WGS84.shp",
            "2021": "OutputArea2022_EoR.shp",
        }
        gdf = gpd.read_file(f"zip://{zip_path}!{llsoa_filename[version]}")
        if version == "2021":
            gdf.set_crs("EPSG:27700", inplace=True)
            gdf.to_crs("EPSG:4326", inplace=True)
        gdf.set_crs("EPSG:4326", inplace=True)
        return gdf[["code", "geometry"]].rename(columns={"code": "llsoa11cd"})

    def _load_llsoa_boundaries(self, version: Literal["2011", "2021"]):
        """
        Load the LLSOA boundaries.

        Parameters
        ----------
        `version` : Literal["2011", "2021"]
            The version of the LLSOA boundaries to load.
        """
        if version not in ["2011", "2021"]:
            raise ValueError(
                f"LLSOA boundaries version {version} is not supported. Supported versions are '2011' and '2021'."
            )
        cache_label = f"llsoa_boundaries_{version}"
        llsoa_boundaries_cache_contents = self.cache_manager.retrieve(cache_label)
        if llsoa_boundaries_cache_contents is not None:
            logging.debug("Loading LLSOA boundaries from cache ('%s')", cache_label)
            return llsoa_boundaries_cache_contents
        llsoa_regions_engwales = self._load_llsoa_boundaries_engwales_regions(version)
        llsoa_regions_scots = self._load_llsoa_boundaries_scots_regions(version)
        llsoa_regions = pd.concat(
            [llsoa_regions_engwales, llsoa_regions_scots]
        ).reset_index()

        self.cache_manager.write(cache_label, llsoa_regions)
        logging.info(
            "LSOA boundaries extracted and pickled to file ('%s')", cache_label
        )
        return llsoa_regions

    def _load_datazone_lookup(self, version: Literal["2011", "2021"]):
        """
        Load a lookup of Scottish LLSOA <-> Datazone.

        Parameters
        ----------
        `version` : Literal["2011", "2021"]
            The version of the LLSOA boundaries to load.
        """
        if version not in ["2011", "2021"]:
            raise ValueError(
                f"LLSOA boundaries version {version} is not supported. Supported versions are '2011' and '2021'."
            )
        cache_label = f"datazone_lookup_{version}"
        datazone_lookup_cache_contents = self.cache_manager.retrieve(cache_label)
        if datazone_lookup_cache_contents is not None:
            logging.debug(
                f"Loading {version} LLSOA<->Datazone lookup from cache {cache_label}"
            )
            return datazone_lookup_cache_contents
        zip_path = self.data_dir.joinpath(f"nrs_{version}.zip")
        dz_lookup_filename = {"2011": "OA_DZ_IZ_2011.csv", "2021": "OA22_DZ22_IZ22.csv"}
        with zipfile.ZipFile(zip_path, "r") as nrs_zip:
            with nrs_zip.open(dz_lookup_filename[version], "r") as fid:
                dz_lookup = pd.read_csv(fid)
        if version == "2011":
            dz_lookup.set_index("OutputArea2011Code", inplace=True)
            dz_lookup.drop(columns=["IntermediateZone2011Code"], inplace=True)
            dz_lookup = dz_lookup.to_dict()["DataZone2011Code"]
        elif version == "2021":
            dz_lookup.set_index("OA22", inplace=True)
            dz_lookup.drop(columns=["IZ22"], inplace=True)
            dz_lookup = dz_lookup.to_dict()["DZ22"]
        self.cache_manager.write(f"datazone_lookup_{version}", dz_lookup)
        return dz_lookup

    def _load_constituency_lookup(self):
        """Load a lookup of UK constituency -> Geospatial Centroid."""
        constituency_lookup_cache_contents = self.cache_manager.retrieve(
            "constituency_centroids"
        )
        if constituency_lookup_cache_contents is not None:
            logging.debug(
                "Loading Constituency lookup from cache ('%s')",
                "constituency_centroids",
            )
            return constituency_lookup_cache_contents
        logging.info(
            "Extracting the Constituency Centroids data (this only needs to be done "
            "once)"
        )
        constituency_lookup = {}
        with open(self.constituency_lookup_file) as fid:
            for line in fid:
                _, name, longitude, latitude = line.strip().split("|")
                match_str = name.strip().replace(" ", "").replace(",", "").lower()
                constituency_lookup[match_str] = (float(latitude), float(longitude))
        self.cache_manager.write("constituency_centroids", constituency_lookup)
        logging.info(
            "Constituency lookup extracted and pickled to '%s'",
            "constituency_centroids",
        )
        return constituency_lookup

    def geocode_llsoa(
        self, llsoa: Union[str, Iterable[str]]
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
        return results

    def reverse_geocode_llsoa(
        self,
        latlons: List[Tuple[float, float]],
        version: str,
        datazones: bool = False,
        **kwargs,
    ) -> List[str]:
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
        if self.llsoa_regions is None:
            self.llsoa_regions = self._load_llsoa_boundaries(version)
        results = utils.reverse_geocode(
            latlons,
            self.llsoa_regions.rename({"llsoa11cd": "region_id"}, axis=1),
            **kwargs,
        )
        if datazones:
            if self.dz_lookup is None:
                self.dz_lookup = self._load_datazone_lookup(version)
            results = [
                llsoacd if llsoacd not in self.dz_lookup else self.dz_lookup[llsoacd]
                for llsoacd in results
            ]
        return results

    def geocode_constituency(
        self, constituency: Union[str, Iterable[str]]
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
        for constituency_ in constituency:
            results.append(self._constituency_centroid(constituency_))
        return results

    def geocode_local_authority(
        self, local_authority: Union[str, Iterable[str]]
    ) -> Union[Tuple[float, float], List[Tuple[float, float]]]:
        """
        Geocode a UK Local Authority using the geospatial centroid.
        Parameters
        ----------
        `local_authority` : string or iterable of strings
            The Local Authority names to geocode.
        Returns
        -------
        tuple or list of tuples
            If a single Local Authority name was passed (i.e. a string), the output be a tuple
            containing: latitude (float), longitude (float). If several Local Authority names are
            passed (i.e. an iterable of strings), the output will be a list of tuples which aligns
            with the input iterable.
        Notes
        -----
        The input *local_authority* iterable can be any Python object which can be looped over with
        a for loop e.g. a list, tuple, Numpy array etc.
        Local Authorities are identified by their full name, case-insensitive, ignoring spaces.
        """
        if self.lad_lookup is None:
            self.lad_lookup = self._load_lad_lookup()
        if isinstance(local_authority, str):
            return self._lad_centroid(local_authority)
        results = []
        tot = len(local_authority)
        for i, local_authority_ in enumerate(local_authority):
            results.append(self._lad_centroid(local_authority_))
        return results

    def postcode2llsoa(self, pcs: pd.DataFrame) -> pd.DataFrame:
        """
        Reverse-geocode multiple postcodes to LLSOA.
        Parameters
        ----------
        `pcs` : Pandas.DataFrame
            A Pandas DataFrame containing the postcodes (or partial postcodes) to geocode. Must
            contain the column 'input_postcode'.
        Returns
        -------
        Pandas.DataFrame
            A Pandas DataFrame with columns: input_postcode (str), lsoa11cd (str), plus any other
            cols that were in `pcs`.
        """
        pcs["postcode_to_match"] = (
            pcs.input_postcode.str.strip().str.upper().str.replace(" ", "")
        )
        if self.pc_llsoa_lookup is None:
            self.pc_llsoa_lookup = self._load_postcode_llsoa_lookup()
        pcs = pcs.merge(
            self.pc_llsoa_lookup[["postcode", "lsoa11cd"]],
            how="left",
            left_on="postcode_to_match",
            right_on="postcode",
        )
        pcs.drop(columns=["postcode_to_match"], inplace=True)
        return pcs

    def _load_lad_lookup(self):
        """Load a lookup of UK Local Authority District -> Geospatial Centroid."""
        lad_lookup_cache_contents = self.cache_manager.retrieve("lad_centroids")
        if lad_lookup_cache_contents is not None:
            logging.debug(
                "Loading Local Authority District lookup from cache ('%s')",
                "lad_centroids",
            )
            return lad_lookup_cache_contents
        logging.info(
            "Extracting the Local Authority District Centroids data (this only needs to "
            "be done once)"
        )
        lad_lookup = {}
        with open(self.lad_lookup_file) as fid:
            for line in fid:
                _, name, longitude, latitude = line.strip().split("|")
                match_str = name.strip().replace(" ", "").replace(",", "").lower()
                lad_lookup[match_str] = (float(latitude), float(longitude))
        self.cache_manager.write("lad_centroids", lad_lookup)
        logging.info(
            "Local Authority District lookup extracted and pickled to '%s'",
            "lad_centroids",
        )
        return lad_lookup

    def _load_postcode_llsoa_lookup(self):
        """Load a lookup of postcode <-> LLSOA."""
        postcode_llsoa_lookup_cache_contents = self.cache_manager.retrieve(
            "pc_llsoa_lookup"
        )
        if postcode_llsoa_lookup_cache_contents is not None:
            logging.debug(
                "Loading postcode<->LLSOA lookup from cache ('%s')", "pc_llsoa_lookup"
            )
            return postcode_llsoa_lookup_cache_contents
        pc_llsoa_lookup = pd.read_csv(self.pc_llsoa_zipfile, dtype=str)
        pc_llsoa_lookup["postcode"] = (
            pc_llsoa_lookup.pcds.str.strip().str.upper().str.replace(" ", "")
        )
        self.cache_manager.write("pc_llsoa_lookup", pc_llsoa_lookup)
        return pc_llsoa_lookup

    def _lad_centroid(self, local_authority):
        """Lookup the GC for a given Local Authority."""
        try:
            match_str = (
                local_authority.strip().replace(" ", "").replace(",", "").lower()
            )
            return self.lad_lookup[match_str]
        except KeyError:
            return None, None

    def _constituency_centroid(self, constituency):
        """Lookup the GC for a given constituency."""
        try:
            match_str = constituency.strip().replace(" ", "").replace(",", "").lower()
            return self.constituency_lookup[match_str]
        except KeyError:
            return None, None

    def _llsoa_centroid(self, llsoa):
        """Lookup the PWC for a given LLSOA code."""
        try:
            return (
                self.llsoa_lookup.loc[llsoa]["latitude"],
                self.llsoa_lookup.loc[llsoa]["longitude"],
            )
        except KeyError:
            return None, None
