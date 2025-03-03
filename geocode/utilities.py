"""
Utilities for the Geocode library.

- Ethan Jones <ejones18@sheffield.ac.uk>
- Jamie Taylor <jamie.taylor@sheffield.ac.uk>
- First Authored: 2022-10-19
"""

import sys
import logging
import requests
import json
from typing import Optional, Iterable, Tuple, Union, List, Dict

import pyproj
try:
    from shapely.geometry import shape, Point
    from shapely.ops import unary_union
    SHAPELY_AVAILABLE = True
except ImportError:
    logging.warning("Failed to import Shapely library - you will not be able to reverse-geocode! "
                    "See notes in the README about installing Shapely on Windows machines.")
    SHAPELY_AVAILABLE = False

class GenericException(Exception):
    """A generic exception for anticipated errors."""
    def __init__(self, msg, err=None):
        self.msg = msg
        if err is not None:
            self.msg += f"\n{repr(err)}"
        logging.exception(self.msg)

    def __str__(self):
        return self.msg

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
        raise utilities.GenericException("Function is deprecated")
        results = []
        if postcodes is None and addresses is None:
            raise utilities.GenericException("You must pass either postcodes or addresses, or both.")
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
                cpo_manager.cpo_geocode(inputs.loc[use_cpo, ["input_postcode"]])[results_cols]
            logging.debug("Successfully geocoded %s postcodes using Code Point Open",
                          (results.latitude.notnull()).sum())
        use_gmaps = results.latitude.isnull()
        if use_gmaps.any():
            logging.debug("Geocoding %s postcodes/addresses using Google Maps", use_gmaps.sum())
            gmaps_geocode = lambda i: gmaps_manager.gmaps_geocode_one(i.input_postcode, i.input_address)
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
    raise utilities.GenericException("Function is deprecated")
    if postcode is None and address is None:
        raise utilities.GenericException("You must specify either a postcode or an address, or both.")
    if address is None:
        lat, lon, status = cpo_manager.cpo_geocode_one(postcode)
        if status > 0:
            return lat, lon, status
    return gmaps_manager.gmaps_geocode_one(postcode, address).to_records(index=False)

def reverse_geocode(coords: List[Tuple[float, float]],
                    regions: Dict,
                    show_progress : bool = None,
                    prefix : str = None) -> List:
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
    if not SHAPELY_AVAILABLE:
        raise utilities.GenericException("Geocode was unable to import the Shapely library, follow "
                                         "the installation instructions at "
                                         "https://github.com/SheffieldSolar/Geocode")
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
                success = True
                break
        if not success:
            results.append(None)
    return results

def _fetch_from_ons_api(url, proxies=None, ssl_verify=True):
    """Download data from the ONS ARCGIS API which uses pagination."""
    exceeded_transfer_limit = True
    offset = 0
    record_count = 2000
    pages = []
    while exceeded_transfer_limit:
        url_ = f"{url}&resultOffset={offset}&resultRecordCount={record_count}"
        success, api_response = fetch_from_api(url_, proxies=proxies, ssl_verify=ssl_verify)
        if success:
            page = json.loads(api_response.text)
            exceeded_transfer_limit = "properties" in page and \
                                      "exceededTransferLimit" in page["properties"] and \
                                      page["properties"]["exceededTransferLimit"]
            pages.append(page)
            offset += record_count
        else:
            raise GenericException("Encountered an error while extracting LLSOA data from ONS API.")
    return pages

def fetch_from_api(url, proxies=None, ssl_verify=True):
    """Generic function to GET data from web API with retries."""
    retries = 0
    while retries < 3:
        try:
            response = requests.get(url, proxies=proxies, verify=ssl_verify)
            response.raise_for_status()
            if response.status_code != 200:
                retries += 1
                continue
            return 1, response
        except:
            retries += 1
    return 0, None

def latlon2bng(lons: List[float], lats: List[float]) -> Tuple[List[float], List[float]]:
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

def bng2latlon(eastings: Iterable[Union[float, int]],
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

