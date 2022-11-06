"""
A generalised cache manager module for the Geocode library.

- Ethan Jones <ejones18@sheffield.ac.uk>
- Jamie Taylor <jamie.taylor@sheffield.ac.uk>
- First Authored: 2022-10-19
"""

import os
import pickle
import sys
import glob
import logging

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

import utilities as utils
from version import __version__

class CacheManager:
    def __init__(self, dir):
        self.cache_dir = os.path.join(SCRIPT_DIR, "cache") if dir is None else dir
        if not os.path.isdir(self.cache_dir):
            raise utils.GenericException(f"The cache_dir '{self.cache_dir}' does not exist.")
        self.version_string = __version__.replace(".", "-")

    def get_dir(self):
        return self.cache_dir

    def get_filename(self, label):
        file_name = f"{label}{self.version_string}.p"
        return os.path.join(self.cache_dir, file_name)

    def check_exists(self, filename):
        if os.path.isfile(filename):
            return True
        else:
            return False

    def retrieve(self, label):
        cache_file = self.get_filename(label)
        if self.check_exists(cache_file):
            with open(cache_file, "rb") as pickle_fid:
                return pickle.load(pickle_fid)
        return None

    def write(self, label, data):
        cache_file = self.get_filename(label)
        with open(cache_file, "wb") as pickle_fid:
            pickle.dump(data, pickle_fid)

    def clear(self,
              delete_gmaps_cache: bool = False,
              old_versions_only: bool = False) -> None:
        """
        Clear all cache files from the cache directory including from old versions.

        Parameters
        ----------
        delete_gmaps_cache : boolean
            Optional boolean deciding whether or not to clear the Google Maps data in cache.
            Defaults to False.
        old_versions_only : boolean
            Optional boolean deciding whether or not to clear all cache files or just those
            corresponding to previous versions of the Geocode library. Defaults to False.
        """
        logging.debug("Deleting cache files (delete_gmaps_cache=%s, old_versions_only=%s)",
                      delete_gmaps_cache, old_versions_only)
        cache_files = glob.glob(os.path.join(self.cache_dir, "*.p"))
        for cache_file in cache_files:
            if not delete_gmaps_cache and "gmaps" in cache_file:
                continue
            if old_versions_only:
                if self.version_string in cache_file:
                    continue
            try:
                os.remove(cache_file)
            except FileNotFoundError:
                pass
            except:
                raise utils.GenericException("Error deleting cache file : ", cache_file)
            logging.debug("Deleted '%s'", cache_file)

