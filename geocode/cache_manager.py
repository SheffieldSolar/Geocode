"""
A generalised cache manager module for the Geocode library.

- Ethan Jones <ejones18@sheffield.ac.uk>
- Jamie Taylor <jamie.taylor@sheffield.ac.uk>
- First Authored: 2022-10-19
"""

import os
import pickle
import glob
import logging
from pathlib import Path
import errno
from typing import Any, Optional

SCRIPT_DIR = Path(os.path.dirname(os.path.realpath(__file__)))

from . version import __version__

class CacheManager:
    """Cache Python variables to files using Pickle."""
    def __init__(self, cache_dir: Optional[Path] = None):
        """
        Cache Python variables to files using Pickle.

        Parameters
        ----------
        cache_dir : str
            Path to a directory to use for writing cache files. If not set, or set to None, default
            location will be `os.path.join(os.path.dirname(os.path.realpath(__file__)), "cache")`.
        """
        self.cache_dir = SCRIPT_DIR.joinpath("cache") if cache_dir is None else cache_dir
        if not self.cache_dir.is_dir():
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), self.cache_dir)
        self.version_string = __version__.replace(".", "-")

    def _get_filename(self, label):
        """Generate a filename using `label` and the current version string."""
        file_name = f"{label}_{self.version_string}.p"
        return self.cache_dir.joinpath(file_name)

    def retrieve(self, label: str) -> Any:
        """
        Retrieve a Python variable from a cache file using Pickle.

        Parameters
        ----------
        label : str
            Provide a unique label to use when identifying the data.

        Returns
        -------
        Any or None
            The Python variable that was stored in the cache file with `label`. Returns None if the
            cache was not found.
        """
        cache_file = self._get_filename(label)
        if cache_file.is_file():
            with open(cache_file, "rb") as pickle_fid:
                return pickle.load(pickle_fid)
        return None

    def write(self, label: str, data: Any):
        """
        Write a Python variable to a cache file using Pickle.

        Parameters
        ----------
        label : str
            Provide a unique label to use when identifying the data.
        data : Any
            Data to cache - must be a data type that can be pickled, see
            https://docs.python.org/3/library/pickle.html#what-can-be-pickled-and-unpickled
        """
        cache_file = self._get_filename(label)
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
        cache_files = self.cache_dir.glob("*.p")
        for cache_file in cache_files:
            if not delete_gmaps_cache and "gmaps" in cache_file.name:
                continue
            if old_versions_only:
                if self.version_string in cache_file.name:
                    continue
            try:
                cache_file.unlink()
            except FileNotFoundError:
                pass
            except:
                raise Exception("Error deleting cache file: ", cache_file)
            logging.debug("Deleted '%s'", cache_file)

