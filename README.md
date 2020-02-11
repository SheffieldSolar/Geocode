# Geocode

Geocode postcodes or addresses using the Code Point Open database and GMaps API.

## What is this repository for? ##

* Use Code Point Open and/or Google Maps API to geocode postcode and/or address into lat/lng co-ordinates.
* Use ONS LLSOA Population Weighted Centroids to geocode Lower Layer Super Output Areas.
* Use GIS data from data.gov.uk to geocode GB constituencies based on geospatial centroid.
* Prioritises Code Point Open for postcode lookup to save expensive GMaps API bills.
* Caches GMaps API queries locally so that repeated queries can be fulfilled without a new API request.
* Version 0.5.0
* Developed and tested with Python 3.6, should work for 3.6+.

## How do I get set up? ##

Make sure you have Git installed - [Download Git](https://git-scm.com/downloads)

Run `pip3 install git+https://github.com/SheffieldSolar/Geocode/`

Check that the installation was successful by running the following command from terminal / command-line:

```>> geocode -h```

This will print the helper for the limited command line interface which provides tools to help get set up and to clear the cache when needed:

```
usage: geocode [-h] [--clear-cache] [--debug]
               [--load-cpo-zip </path/to/zip-file>]
               [--load-gmaps-key </path/to/zip-file>]

This is a command line interface (CLI) for the Geocode module.

optional arguments:
  -h, --help            show this help message and exit
  --clear-cache         Specify to delete the cache files.
  --debug               Geocode some sample postcodes/addresses/LLSOAs.
  --load-cpo-zip </path/to/zip-file>
                        Load the Code Point Open data from a local zip file.
  --load-gmaps-key </path/to/zip-file>
                        Load a Google Maps API key.

Jamie Taylor, 2019-10-08
```

### Code Point Open setup ###

As of version 0.4.15, the Code Point Open data is packaged with this repository, however the following instructions can be used to update the local Code Point Open dataset without re-installing the Geocode library.

Contains OS data © Crown copyright and database right 2018

Contains Royal Mail data © Royal Mail copyright and Database right 2018

Contains National Statistics data © Crown copyright and database right 2018

#### Updating Code Point Open ####

Download the (free) Code Point Open data [here](https://www.ordnancesurvey.co.uk/business-government/products/code-point-open).
Run the following command:

```
>> geocode --load-cpo-zip "<path-to-zip-file>"
```

For example, if you saved the code point open data to 'C:\Users\jamie\Downloads\codepo_gb.zip', you would run:
    
```
>> geocode --load-cpo-zip "C:\Users\jamie\Downloads\codepo_gb.zip"
```

N.B. If you encounter problems using the `--load-cpo-zip` utility, you might like to manually copy/paste the zip file `codepo_gb.zip` into the `geocode/code_point_open` subdirectory of the geocode module. (Do not extract the contents of the zipfile)

The CPO data will be extracted and stored locally in `geocode/code_point_open/code_point_open_<version>.p`.

### GMaps setup ###

If you want to make use of the Google Maps API, you can load an API key using the command:

```
>> geocode --load-gmaps-key "<your-api-key>"
```

N.B. The API key will be stored in plaintext in the `geocode/google_maps/key.txt` file within your local installation. This is not secure and only appropriate when using the Geocode library for development purposes.

See [here](https://developers.google.com/maps/documentation/javascript/get-api-key) for help getting an API key.

Any queries you make to the GMaps API will be cached to `geocode/google_maps/gmaps_cache_<version>.p` so that repeated queries are faster and cheaper.

### ONS setup ###

The Geocode library makes use of data from the Office for National Statistics in order to geocode Lower Layer Super Output Areas (LLSOAs). The first time you make use of the `Geocoder.geocode_llsoa()` method, the LLSOA (December 2011) Population Weighted Centroids data will be downloaded from the ONS API and cached locally in `geocode/ons/llsoa_centroids_<version>.p`. More information [here](https://geoportal.statistics.gov.uk/datasets/lower-layer-super-output-areas-december-2011-population-weighted-centroids).

### data.gov.uk setup ###

The Geocode library makes use of the Westminster Parliamentary Constituencies (December 2015) Generalised Clipped Boundaries in Great Britain&dagger; from data.gov.uk in order to geocode constituencies according to their (unweighted) geospatial centroid. More information [here](https://data.gov.uk/dataset/24c282a1-1330-427e-b154-36ff3bfa5dac/westminster-parliamentary-constituencies-december-2015-generalised-clipped-boundaries-in-great-britain). The geospatial centroids have been calculated using ESRI ArcMap's 'Feature To Point' toolbox and the derived lookup data is packaged within this respository as a pipe-separated variable file: `geocode/gov/constituency_centroids.psv`. The first time you make use of the `Geocoder.geocode_constituency()` method, this file will be loaded and cached back to `geocode/gov/constituency_centroids_<version>.p` for faster loading next time.

&dagger;Contains public sector information licensed under the Open Government Licence v3.0.

## Getting started ##

Within your Python code, I recommend using the context manager so that GMaps cache will be automatically flushed on exit. See `example.py`:

```Python
from geocode import Geocoder

def main():
    with Geocoder() as geocoder:
        # Geocode some postcodes / addresses...
        postcodes = ["S3 7RH", "S3 7", "S3", None, None, "S3 7RH"]
        addresses = [None, None, None, "Hicks Building, Sheffield", "Hicks", "Hicks Building"]
        results = geocoder.geocode(postcodes, addresses)
        for postcode, address, (lat, lon, status) in zip(postcodes, addresses, results):
            print(f"Postcode: `{postcode}`    Address: `{address}`")
            if status == 0:
                print("    Failed to geocode!")
            else:
                print(f"    {lat:.3f}, {lon:.3f}    ->  {geocoder.status_codes[status]}")
        # Geocode some LLSOAs...
        llsoas = ["E01033264", "E01033262"]
        results = geocoder.geocode_llsoa(llsoas)
        for llsoa, (lat, lon) in zip(llsoas, results):
            print(f"LLSOA: `{llsoa}`")
            print(f"    {lat:.3f}, {lon:.3f}")
        # Geocode some Constituencies...
        constituencies = ["Sheffield Central", "Sheffield Hallam"]
        results = geocoder.geocode_constituency(constituencies)
        for constituency, (lat, lon) in zip(constituencies, results):
            print(f"Constituency: `{constituency}`")
            print(f"    {lat:.3f}, {lon:.3f}")

if __name__ == "__main__":
    main()
```

```
>> python geocode_example.py
Postcode: `S3 7RH`    Address: `None`
    53.381, -1.486    ->  Full match with Code Point Open
Postcode: `S3 7`    Address: `None`
    53.383, -1.481    ->  Partial match with Code Point Open
Postcode: `S3`    Address: `None`
    53.387, -1.474    ->  Partial match with Code Point Open
Postcode: `None`    Address: `Hicks Building, Sheffield`
    53.381, -1.487    ->  Full match with GMaps
Postcode: `None`    Address: `Hicks`
    Failed to geocode!
Postcode: `S3 7RH`    Address: `Hicks Building`
    53.381, -1.487    ->  Full match with GMaps
LLSOA: `E01033264`
    53.384, -1.467
LLSOA: `E01033262`
    53.388, -1.470
Constituency: `Sheffield Central`
    53.376, -1.474
Constituency: `Sheffield Hallam`
    53.382, -1.590
```

It is also possible to avoid using the context manager, but if doing so you should manually call the `Geocoder.flush_gmaps_cache()` method when finished to take advantage of cached GMaps API queries.

In the above example, `postcodes` and `addresses` are lists of strings, but it should be fine to use any iterator such as Numpy arrays or Pandas DataFrame columns, although the `geocode()` method will still return a list of tuples.

## Documentation ##

* Coming soon

## How do I update? ##

Run `pip3 install --upgrade git+https://github.com/SheffieldSolar/Geocode/`.
You may also wish to clear all locally cached data by running the following command from terminal/command-prompt:

```
geocode --clear-cache
```
