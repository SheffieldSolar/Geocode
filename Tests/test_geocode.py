#!/usr/bin/python3
"""
Unit tests for the Geocode library.

.. code:: console

    $ python -m unittest Tests.test_geocode

Jamie Taylor 2020-05-22
"""

import unittest
import numpy as np

from geocode import Geocoder

class geocodeTestCase(unittest.TestCase):
    """Tests for `geocode.py`."""
    def setUp(self):
        with Geocoder(quiet=True) as geo:
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
            (55.91836588770352, -4.21934323024909),
        ]
        with Geocoder(quiet=True) as geo:
            # test_data = geo.geocode_llsoa(llsoas)
            # for i, c in enumerate(centroids):
                # self.assertAlmostEqual(test_data[i][0], c[0])
                # self.assertAlmostEqual(test_data[i][1], c[1])
            #self.assertAlmostEqual(geo.geocode_llsoa(llsoas), centroids)
            np.testing.assert_almost_equal(geo.geocode_llsoa(llsoas), centroids)

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
        with Geocoder(quiet=True) as geo:
            self.assertEqual(geo.reverse_geocode_llsoa(latlons), llsoas)
            self.assertEqual(geo.reverse_geocode_llsoa(datazone_latlons, datazones=True), datazones)

    def test_geocode_constituency(self):
        """
        Test the `geocode_constituency()` function with several test cases.
        """
        constituencies = ["Banbury", "Cardiff Central", "Inverclyde"]
        latlons = [
            (51.9910766, -1.286259136),
            (51.5047741, -3.163350737),
            (55.90118449, -4.743636917),
        ]
        with Geocoder(quiet=True) as geo:
            #self.assertAlmostEqual(geo.geocode_constituency(constituencies), latlons)
            np.testing.assert_almost_equal(geo.geocode_constituency(constituencies), latlons)

    # def test_reverse_geocode_gsp(self):
        # """
        # Test the `reverse_geocode_gsp` function with several test cases.
        # """
        
        # with Geocoder(quiet=True) as geo:
            # assertAlmostEqual(geo.geocode_constituency(constituencies), latlons)

if __name__ == "__main__":
    unittest.main()
