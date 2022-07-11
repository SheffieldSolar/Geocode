#!/usr/bin/python3
"""
Unit tests for the Geocode library.

.. code:: console

    $ python -m unittest Tests.test_geocode

Jamie Taylor 2020-05-22
"""

import unittest
from numpy.testing import assert_almost_equal
import pandas as pd
from pandas.testing import assert_frame_equal

from geocode import Geocoder

class geocodeTestCase(unittest.TestCase):
    """Tests for `geocode.py`."""
    def setUp(self):
        with Geocoder(progress_bar=False) as geo:
            geo.clear_cache(delete_gmaps_cache=True)

    def test_geocode_llsoa(self):
        """
        Test the `geocode_llsoa()` function with several test cases.
        """
        llsoas = ["E01012082", "E01011214", "E01002050", "W01000323", "S00101253", "S01008087"]
        centroids = [
            (54.547776537068664, -1.195629080286167),
            (53.666095344794648, -1.703771184460476),
            (51.578729873335718, -0.068445270723745),
            (53.207256254835059, -3.13247635788833),
            (55.94492620443608, -4.333451009831742),
            (55.91836588770352, -4.21934323024909)
        ]
        with Geocoder(progress_bar=False) as geo:
            assert_almost_equal(geo.geocode_llsoa(llsoas), centroids)

    def test_reverse_geocode_llsoa(self):
        """
        Test the `reverse_geocode_llsoa()` function with several test cases.
        """
        llsoas = ["E01012082", "E01011214", "E01002050", "W01000323", "S00101253"]
        datazones = ["S01008087"]
        latlons = [
            (54.547776537068664, -1.195629080286167),
            (53.666095344794648, -1.703771184460476),
            (51.578729873335718, -0.068445270723745),
            (53.207256254835059, -3.13247635788833),
            (55.94492620443608, -4.333451009831742)
        ]
        datazone_latlons = [(55.91836588770352, -4.21934323024909)]
        with Geocoder(progress_bar=False) as geo:
            self.assertEqual(geo.reverse_geocode_llsoa(latlons), llsoas)
            self.assertEqual(geo.reverse_geocode_llsoa(datazone_latlons, datazones=True), datazones)

    def test_geocode_constituency(self):
        """
        Test the `geocode_constituency()` function with several test cases.
        """
        constituencies = ["Banbury", "Cardiff Central",
                          "Inverclyde"]
        latlons = [
            (52.000099, -1.4026),
            (51.505779, -3.16395),
            (55.900299, -4.75387),
        ]
        with Geocoder(progress_bar=False) as geo:
            assert_almost_equal(geo.geocode_constituency(constituencies), latlons)

    def test_geocode_local_authority(self):
        """
        Test the `geocode_local_authority()` function with several test cases.
        """
        lads = ["Blackpool", "Armagh City, Banbridge and Craigavon", "Bristol, City of"]
        latlons = [
            (53.82164, -3.0219901),
            (54.3867, -6.4345498),
            (51.471149, -2.57742),
        ]
        with Geocoder(progress_bar=False) as geo:
            assert_almost_equal(geo.geocode_local_authority(lads), latlons)

    def test_postcode2llsoa(self):
        """
        Test the `postcode2llsoa()` function with several test cases.
        """
        postcodes = pd.DataFrame({"input_postcode": ["IP1 5HX", "NE25 8EA", "SK14 2SF", "UB4 9UA"]})
        expected = pd.DataFrame({
            "input_postcode": ["IP1 5HX", "NE25 8EA", "SK14 2SF", "UB4 9UA"],
            "postcode": ["IP15HX", "NE258EA", "SK142SF", "UB49UA"],
            "lsoa11cd": ["E01030028", "E01008520", "E01006028", "E01002545"],
        })
        with Geocoder(progress_bar=False) as geo:
            assert_frame_equal(geo.postcode2llsoa(postcodes), expected)

    # def test_reverse_geocode_gsp(self):
        # """
        # Test the `reverse_geocode_gsp` function with several test cases.
        # """
        
        # with Geocoder(progress_bar=False) as geo:
            # assertAlmostEqual(geo.geocode_constituency(constituencies), latlons)

if __name__ == "__main__":
    unittest.main()
