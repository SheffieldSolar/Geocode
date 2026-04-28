#!/usr/bin/python3
"""
Unit tests for the Geocode library.

.. code:: console

    $ python -m unittest Tests.test_geocode

Jamie Taylor & Ethan Jones 2020-05-22
"""

import sys
import os
import unittest
from unittest.mock import MagicMock
from shapely.geometry import Polygon
from pathlib import Path
import geopandas as gpd

from numpy.testing import assert_almost_equal, assert_equal

# sys.path.append("../geocode/")
from geocode import Geocoder


class geocodeTestCase(unittest.TestCase):
    """Tests for `geocode.py`."""

    def test_clear_cache(self):
        """Test the `cache_manager.clear()` method."""
        with Geocoder() as geo:
            geo.cache_manager.clear(delete_gmaps_cache=False)
        cache_dir = geo.cache_manager.cache_dir
        assert cache_dir.is_dir()
        assert len([c for c in cache_dir.glob("*.p") if "gmaps" not in c.name]) == 0

    @unittest.skip("Skipped to avoid external API calls")
    def test_force_setup(self):
        """Test the `force_setup()` method."""
        with Geocoder() as geo:
            geo.force_setup()
        cache_dir = geo.cache_manager.cache_dir
        assert cache_dir.is_dir()
        assert len([c for c in cache_dir.glob("*.p") if "gmaps" not in c.name]) == 17

    def test_force_setup_without_external_api_calls(self):
        """Test the `force_setup()` method with mocked components."""
        with Geocoder() as geo:

            def mock_neso_force_setup():

                gsp_boundaries = gpd.GeoDataFrame(
                    {
                        "GSPs": ["BRED_1", "DEWP"],
                        "GSPGroup": ["_G", "_N"],
                        "geometry": [
                            Polygon(
                                [
                                    (-2.1, 53.3),
                                    (-2.0, 53.3),
                                    (-2.0, 53.4),
                                    (-2.1, 53.4),
                                    (-2.1, 53.3),
                                ]
                            ),
                            Polygon(
                                [
                                    (-3.2, 55.9),
                                    (-3.1, 55.9),
                                    (-3.1, 56.0),
                                    (-3.2, 56.0),
                                    (-3.2, 55.9),
                                ]
                            ),
                        ],
                    },
                    crs="EPSG:4326",
                )
                for version in ["20220314", "20250109", "20251204"]:
                    geo.cache_manager.write(f"gsp_boundaries_{version}", gsp_boundaries)
                geo.cache_manager.write("dno_boundaries", gpd.GeoDataFrame({}))

            def mock_ons_nrs_force_setup():
                llsoa_boundaries = gpd.GeoDataFrame(
                    {
                        "region_id": [
                            "E01012082",
                            "E01011214",
                            "E01002050",
                            "W01000323",
                            "S01008087",
                        ],
                        "geometry": [
                            Polygon(
                                [
                                    (-1.2, 54.5),
                                    (-1.19, 54.5),
                                    (-1.19, 54.55),
                                    (-1.2, 54.55),
                                    (-1.2, 54.5),
                                ]
                            ),
                            Polygon(
                                [
                                    (-1.71, 53.66),
                                    (-1.69, 53.66),
                                    (-1.69, 53.67),
                                    (-1.71, 53.67),
                                    (-1.71, 53.66),
                                ]
                            ),
                            Polygon(
                                [
                                    (-0.07, 51.57),
                                    (-0.06, 51.57),
                                    (-0.06, 51.58),
                                    (-0.07, 51.58),
                                    (-0.07, 51.57),
                                ]
                            ),
                            Polygon(
                                [
                                    (-3.14, 53.20),
                                    (-3.12, 53.20),
                                    (-3.12, 53.21),
                                    (-3.14, 53.21),
                                    (-3.14, 53.20),
                                ]
                            ),
                            Polygon(
                                [
                                    (-4.23, 55.91),
                                    (-4.21, 55.91),
                                    (-4.21, 55.93),
                                    (-4.23, 55.93),
                                    (-4.23, 55.91),
                                ]
                            ),
                        ],
                    },
                    crs="EPSG:4326",
                )
                for version in ["2011", "2021"]:
                    geo.cache_manager.write(
                        f"llsoa_boundaries_{version}", llsoa_boundaries
                    )
                    geo.ons_nrs._load_datazone_lookup(version)
                geo.ons_nrs._load_constituency_lookup()
                geo.ons_nrs._load_lad_lookup()
                geo.ons_nrs._load_llsoa_lookup()

            geo.neso.force_setup = MagicMock(side_effect=mock_neso_force_setup)
            geo.ons_nrs.force_setup = MagicMock(side_effect=mock_ons_nrs_force_setup)

            geo.force_setup()

            geo.neso.force_setup.assert_called_once()
            geo.ons_nrs.force_setup.assert_called_once()

    def test_geocode_llsoa(self):
        """
        Test the `geocode_llsoa()` function with several test cases.
        """
        llsoas = [
            "E01012082",
            "E01011214",
            "E01002050",
            "W01000323",
            "S00101253",
            "S01008087",
            "S01020873",
        ]
        centroids = [
            (54.5477949315505, -1.19562636315068),
            (53.6669451917253, -1.70300404181518),
            (51.5787798943552, -0.06847625193368),
            (53.2072680650806, -3.13215047150594),
            (55.9449262044360, -4.33345100983174),
            (55.9183658877035, -4.21934323024909),
            (55.9341580155129, -3.46004249282003),
        ]
        with Geocoder() as geo:
            assert_almost_equal(geo.geocode_llsoa(llsoas), centroids)

    def test_reverse_geocode_llsoa(self):
        """
        Test the `reverse_geocode_llsoa()` function with several test cases.
        """
        llsoas = ["E01012082", "E01011214", "E01002050", "W01000323"]
        datazones = ["S01008087"]
        latlons = [
            (54.547776537068664, -1.195629080286167),
            (53.666095344794648, -1.703771184460476),
            (51.578729873335718, -0.068445270723745),
            (53.207256254835059, -3.13247635788833),
        ]
        datazone_latlons = [(55.91836588770352, -4.21934323024909)]
        with Geocoder() as geo:
            self.assertEqual(geo.reverse_geocode_llsoa(latlons), llsoas)
            self.assertEqual(
                geo.reverse_geocode_llsoa(datazone_latlons, dz=True), datazones
            )

    def test_reverse_geocode_nuts(self):
        """
        Test the `reverse_geocode_nuts()` function with several test cases.
        """
        nuts0 = ["UK", "FR", "DE"]
        nuts1 = ["UKK", "FRG", "DE7"]
        nuts2 = ["UKK1", "FRG0", "DE72"]
        nuts3 = ["UKK15", "FRG04", "DE724"]
        latlons = [(51.3259, -1.9613), (47.9995, 0.2335), (50.8356, 8.7343)]
        with Geocoder() as geo:
            self.assertEqual(geo.reverse_geocode_nuts(latlons, level=3), nuts3)
            self.assertEqual(geo.reverse_geocode_nuts(latlons, level=2), nuts2)
            self.assertEqual(geo.reverse_geocode_nuts(latlons, level=1), nuts1)
            self.assertEqual(geo.reverse_geocode_nuts(latlons, level=0), nuts0)

    def test_reverse_geocode_gsp(self):
        """
        Test the `reverse_geocode_gsp` function with several test cases.
        """
        gsp_regions = [("BRED_1", "_G"), ("DEWP", "_N")]
        latlons = [(53.33985, -2.051880), (55.950095, -3.178485)]
        with Geocoder() as geo:
            assert_equal(
                geo.reverse_geocode_gsp(latlons, version="20220314"), gsp_regions
            )
            assert_equal(
                geo.reverse_geocode_gsp(latlons, version="20250109"), gsp_regions
            )

    def test_geocode_constituency(self):
        """
        Test the `geocode_constituency()` function with several test cases.
        """
        constituencies = ["Poplar and Limehouse", "Blyth Valley"]
        latlons = [(51.507938, -0.015729999), (55.092758, -1.56095)]
        with Geocoder() as geo:
            assert_almost_equal(geo.geocode_constituency(constituencies), latlons)

    def test_geocode_local_authority(self):
        """
        Test the `geocode_local_authority()` function with several test cases.
        """
        lads = ["Medway", "Barrow-in-Furness", "Derry City and Strabane"]
        latlons = [(51.44772, 0.56317401), (54.15731, -3.1998999), (54.80904, -7.42064)]
        with Geocoder() as geo:
            assert_almost_equal(geo.geocode_local_authority(lads), latlons)

    def test_geocode_postcode(self):
        """
        Test the `geocode_postcode()` function with several test cases.
        """
        postcodes = ["RG1 3PE", "S10 2FR", "FY2 0RD"]
        latlons = [
            (51.45511, -0.94070, 1),
            (53.37708, -1.48700, 1),
            (53.83989, -3.04328, 1),
        ]
        with Geocoder() as geo:
            assert_almost_equal(
                geo.geocode_postcode(postcodes).tolist(), latlons, decimal=4
            )


if __name__ == "__main__":
    unittest.main()
