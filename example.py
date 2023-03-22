import os
import logging

from geocode import Geocoder

def main():
    with Geocoder() as geocoder:
        # Geocode some postcodes / addresses...
        print("GEOCODE POSTCODES / ADDRESSES:")
        postcodes = ["S3 7RH", "S3 7", "S3", None, None, "S3 7RH"]
        addresses = [None, None, None, "Hicks Building, Sheffield", "Hicks", "Hicks Building"]
        results = geocoder.geocode(postcodes, "postcode", address=addresses)
        for postcode, address, (lat, lon, status) in zip(postcodes, addresses, results):
            print(f"    Postcode + Address: `{postcode}` + `{address}`  ->  {lat:.3f}, {lon:.3f} "
                  f"({geocoder.status_codes[status]})")
        # Geocode some LLSOAs...
        print("GEOCODE LLSOAs:")
        llsoas = ["E01033264", "E01033262"]
        results = geocoder.geocode_llsoa(llsoas)
        for llsoa, (lat, lon) in zip(llsoas, results):
            print(f"    LLSOA: `{llsoa}`  ->  {lat:.3f}, {lon:.3f}")
        # Geocode some Constituencies...
        print("GEOCODE CONSTITUENCIES:")
        constituencies = ["Sheffield Central", "Sheffield Hallam"]
        results = geocoder.geocode_constituency(constituencies)
        for constituency, (lat, lon) in zip(constituencies, results):
            print(f"    Constituency: `{constituency}`  ->  {lat:.3f}, {lon:.3f}")
        # Reverse-geocode some lat/lons to LLSOAs...
        print("REVERSE-GEOCODE TO LLSOA:")
        latlons = [(53.384, -1.467), (53.388, -1.470)]
        results = geocoder.reverse_geocode_llsoa(latlons)
        for llsoa, (lat, lon) in zip(results, latlons):
            print(f"    LATLON: {lat:.3f}, {lon:.3f}  ->  {llsoa}")
        # Reverse-geocode some lat/lons to GSP...
        print("REVERSE-GEOCODE TO GSP:")
        latlons = [(53.384, -1.467), (53.388, -1.470)]
        results = geocoder.reverse_geocode_gsp(latlons)
        for (lat, lon), region_id in zip(latlons, results):
            print(f"    LATLON: {lat:.3f}, {lon:.3f}  ->  {region_id}")
        # Reverse-geocode some lat/lons to 2021 NUTS2...
        print("REVERSE-GEOCODE TO NUTS2:")
        latlons = [(51.3259, -1.9613), (47.9995, 0.2335), (50.8356, 8.7343)]
        results = geocoder.reverse_geocode_nuts(latlons, year=2021, level=2)
        for (lat, lon), nuts2 in zip(latlons, results):
            print(f"    LATLON: {lat:.3f}, {lon:.3f}  ->  {nuts2}")

if __name__ == "__main__":
    log_fmt = "%(asctime)s [%(levelname)s] [%(filename)s:%(funcName)s] - %(message)s"
    fmt = os.environ.get("GEOCODE_LOGGING_FMT", log_fmt)
    datefmt = os.environ.get("GEOCODE_LOGGING_DATEFMT", "%Y-%m-%dT%H:%M:%SZ")
    logging.basicConfig(format=fmt, datefmt=datefmt, level=os.environ.get("LOGLEVEL", "WARNING"))
    main()