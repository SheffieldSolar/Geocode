# Geocode

Geocode various geographical entities including postcodes and LLSOAs. Reverse-geocode to LLSOA or GSP/GNode.

*Latest Version: 0.8.9*

## What is this repository for?

* Use Code Point Open and/or Google Maps API to geocode postcode and/or address into lat/lon co-ordinates.
* Use ONS & NRS LLSOA Population Weighted Centroids to geocode Lower Layer Super Output Areas.
* Use GIS data from data.gov.uk to geocode GB constituencies based on geospatial centroid.
* Use GIS boundaries data from ONS and NRS to reverse-geocode lat/lon to LLSOA.
* Use GIS data from National Grid ESO's data portal to reverse-geocode to GSP / GNode. 

## Benefits
* Prioritises Code Point Open for postcode lookup to save expensive GMaps API bills.
* Caches GMaps API queries locally so that repeated queries can be fulfilled without a new API request.
* Fetches data automatically at runtime from public APIs where possible.

## How do I get set up?

Developed and tested with Python 3.7, should work for 3.6+.

Make sure you have Git installed - [Download Git](https://git-scm.com/downloads)

Run `pip install git+https://github.com/SheffieldSolar/Geocode/`

Check that the installation was successful by running the following command from terminal / command-line:

```>> geocode -h```

This will print the helper for the limited command line interface which provides tools to help get set up and to clear the cache when needed:

```
usage: geocode.py [-h] [--clear-cache] [--debug] [--setup]
                  [--load-cpo-zip </path/to/zip-file>]
                  [--load-gmaps-key <gmaps-api-key>]

This is a command line interface (CLI) for the Geocode module version 0.7.2.

optional arguments:
  -h, --help            show this help message and exit
  --clear-cache         Specify to delete the cache files.
  --debug               Geocode some sample postcodes/addresses/LLSOAs.
  --setup               Force download all datasets to local cache (useful if
                        running inside a Docker container i.e. run this as
                        part of image build).
  --load-cpo-zip </path/to/zip-file>
                        Load the Code Point Open data from a local zip file.
  --load-gmaps-key <gmaps-api-key>
                        Load a Google Maps API key.

Jamie Taylor, 2019-10-08
```

No additional set up is needed at this stage - the required datasets will be downloaded (or extracted from the packaged data) the first time you use the associated method. If you want to force the Geocode library to download/extract all data, you can run the following command:

```>> geocode --setup```

This is especially useful if you are installing / running the library inside a container - using the above command you can download the data once during the image build rather than have to re-download every time the container is destroyed.

**Important**

Note that this library makes use of the [Shapely library](https://pypi.org/project/Shapely/) from PyPi, which often does not install correctly on Windows machines due to some missing dependencies. If using Windows and you see an error like `OSError: [WinError 126] The specified module could not be found`, you should install Shapely from one of the unofficial binaries [here](https://www.lfd.uci.edu/~gohlke/pythonlibs/#shapely) e.g.

```>> pip install https://download.lfd.uci.edu/pythonlibs/s2jqpv5t/Shapely-1.7.0-cp37-cp37m-win_amd64.whl```

All data required by this library is either packaged with the code or is downloaded at runtime from public APIs. Some data is subect to licenses and/or you may wish to manually update certain datasets (e.g. OS Code Point Open) - see [appendix](#Appendix).

## Usage

### Within a Python script

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
        # Reverse-geocode some lat/lons to LLSOAs...
        latlons = [(53.384, -1.467), (53.388, -1.470)]
        results = geocoder.reverse_geocode_llsoa(latlons)
        for llsoa, (lat, lon) in zip(results, latlons):
            print(f"LATLON: {lat:.3f}, {lon:.3f}:")
            print(f"    `{llsoa}`")
        # Reverse-geocode some lat/lons to GSP/GNode...
        latlons = [(53.384, -1.467), (53.388, -1.470)]
        results, results_more = geocoder.reverse_geocode_gsp(latlons)
        for (lat, lon), region_id, extra in zip(latlons, results, results_more):
            print(f"LATLON: {lat:.3f}, {lon:.3f}:")
            print(f"    {region_id}")
            for e in extra:
                print(f"        {e}")
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
    Failed to geocode!
Postcode: `None`    Address: `Hicks`
    Failed to geocode!
Postcode: `S3 7RH`    Address: `Hicks Building`
    Failed to geocode!
LLSOA: `E01033264`
    53.384, -1.467
LLSOA: `E01033262`
    53.388, -1.470
LATLON: 53.384, -1.467:
    `E01033264`
LATLON: 53.388, -1.470:
    `E01033262`
LATLON: 53.384, -1.467:
    179
        {'ng_id': 310, 'ggd_id': 310, 'gnode_id': 310, 'gnode_name': 'PIT1', 'gnode_lat': 53.400459999999995, 'gnode_lon': -1.44215, 'gsp_id': 293, 'gsp_name': 'PITS_3', 'gsp_lat': 53.400459999999995, 'gsp_lon': -1.44215, 'dc_id': <NA>, 'dc_name': <NA>, 'dc_lat': <NA>, 'dc_lon': <NA>, 'region_id': 179, 'region_name': 'Pitsmoor', 'has_pv': 1, 'pes_id': 23, 'pes_name': '_M'}
        {'ng_id': 311, 'ggd_id': 311, 'gnode_id': 311, 'gnode_name': 'PIT2', 'gnode_lat': 53.400459999999995, 'gnode_lon': -1.44215, 'gsp_id': 293, 'gsp_name': 'PITS_3', 'gsp_lat': 53.400459999999995, 'gsp_lon': -1.44215, 'dc_id': <NA>, 'dc_name': <NA>, 'dc_lat': <NA>, 'dc_lon': <NA>, 'region_id': 179, 'region_name': 'Pitsmoor', 'has_pv': 1, 'pes_id': 23, 'pes_name': '_M'}
LATLON: 53.388, -1.470:
    179
        {'ng_id': 310, 'ggd_id': 310, 'gnode_id': 310, 'gnode_name': 'PIT1', 'gnode_lat': 53.400459999999995, 'gnode_lon': -1.44215, 'gsp_id': 293, 'gsp_name': 'PITS_3', 'gsp_lat': 53.400459999999995, 'gsp_lon': -1.44215, 'dc_id': <NA>, 'dc_name': <NA>, 'dc_lat': <NA>, 'dc_lon': <NA>, 'region_id': 179, 'region_name': 'Pitsmoor', 'has_pv': 1, 'pes_id': 23, 'pes_name': '_M'}
        {'ng_id': 311, 'ggd_id': 311, 'gnode_id': 311, 'gnode_name': 'PIT2', 'gnode_lat': 53.400459999999995, 'gnode_lon': -1.44215, 'gsp_id': 293, 'gsp_name': 'PITS_3', 'gsp_lat': 53.400459999999995, 'gsp_lon': -1.44215, 'dc_id': <NA>, 'dc_name': <NA>, 'dc_lat': <NA>, 'dc_lon': <NA>, 'region_id': 179, 'region_name': 'Pitsmoor', 'has_pv': 1, 'pes_id': 23, 'pes_name': '_M'}
Constituency: `Sheffield Central`
    53.376, -1.474
Constituency: `Sheffield Hallam`
    53.382, -1.590
```

It is also possible to avoid using the context manager, but if doing so you should manually call the `Geocoder.flush_gmaps_cache()` method when finished to take advantage of cached GMaps API queries.

In the above example, `postcodes` and `addresses` are lists of strings, but it should be fine to use any iterator such as Numpy arrays or Pandas DataFrame columns, although the `geocode()` method will still return a list of tuples.

When reverse-geocoding to GSP, the `reverse_geocode_gsp()` method returns both a list of Region IDs and a corresponding list of GSP / GNodes etc. Since the relationship between Region:GSP:GNode is theoretically MANY:MANY:MANY, the second object returned is a list of lists of dicts. This is rather clunky and will likely be refined in a future release. An alternative use case could disregard this second return object and instead make use of the `Geocoder.gsp_lookup` instance attribute - this is a Pandas DataFrame giving the full lookup between Regions / GSPs / GNodes / DNO License Areas (i.e. [this](https://data.nationalgrideso.com/system/gis-boundaries-for-gb-grid-supply-points/r/gsp_-_gnode_-_direct_connect_-_region_lookup) dataset on the ESO Data Portal). In testing, the `reverse_geocode_gsp()` method was able to allocate ~1 million random lat/lons to the correct GSP in average wall-clock time of around 300 seconds.

### Command Line Utilities

#### latlons2llsoa

This utility can be used to load a CSV file containing latitudes and longitudes and to reverse-geocode them to LLSOAs (optionally switching to Data Zones in Scotland):

```
>> latlons2llsoa -h
usage: latlons2llsoa [-h] -f </path/to/file> -o </path/to/file> [--datazones]

This is a command line interface (CLI) for the latlons2llsoa.py module

optional arguments:
  -h, --help            show this help message and exit
  -f </path/to/file>, --input-file </path/to/file>
                        Specify a CSV file containing a list of latitudes and
                        longitudes to be reverse-geocoded. The file must
                        contain the columns 'latitude' and 'longitude' (it can
                        contain others, all of which will be kept).
  -o </path/to/file>, --output-file </path/to/file>
                        Specify an output file.
  --datazones           Specify to use Data Zones in Scotland.

Jamie Taylor, 2020-04-16
```

#### latlons2gsp

This utility can be used to load a CSV file containing latitudes and longitudes and to reverse-geocode them to GSPs/GNodes.

```
>> latlons2gsp -h
usage: latlons2gsp.py [-h] -f </path/to/file> -o </path/to/file>

This is a command line interface (CLI) for the latlons2gsp.py module

optional arguments:
  -h, --help            show this help message and exit
  -f </path/to/file>, --input-file </path/to/file>
                        Specify a CSV file containing a list of latitudes and
                        longitudes to be reverse-geocoded. The file must
                        contain the columns 'latitude' and 'longitude' (it can
                        contain others, all of which will be kept).
  -o </path/to/file>, --output-file </path/to/file>
                        Specify an output file.

Jamie Taylor, 2020-05-29
```

#### postcodes2latlon

This utility can be used to load a CSV file containing postcodes and to geocode them to lat/lons.

```
>> postcodes2latlon -h
usage: postcodes2latlon.py [-h] -f </path/to/file> -o </path/to/file>

This is a command line interface (CLI) for the postcodes2latlon.py module

optional arguments:
  -h, --help            show this help message and exit
  -f </path/to/file>, --input-file </path/to/file>
                        Specify a CSV file containing a list of postcodes to
                        be geocoded. The file must contain the column
                        'postcode' (it can contain others, all of which will
                        be kept).
  -o </path/to/file>, --output-file </path/to/file>
                        Specify an output file.

Jamie Taylor, 2020-06-12
```

#### bng2latlon

This utility can be used to load a CSV file containing eastings and northings and to convert them to lat/lons.

```
>> bng2latlon -h
usage: bng2latlon.py [-h] -f </path/to/file> -o </path/to/file>

This is a command line interface (CLI) for the bng2latlon.py module

optional arguments:
  -h, --help            show this help message and exit
  -f </path/to/file>, --input-file </path/to/file>
                        Specify a CSV file containing a list of eastings and
                        northings to be converted. The file must contain the
                        columns 'eastings' and 'northings' (it can contain
                        others, all of which will be kept).
  -o </path/to/file>, --output-file </path/to/file>
                        Specify an output file.

Jamie Taylor, 2020-06-12
```

## Documentation

* Coming soon

## How do I update?

Run `pip install --upgrade git+https://github.com/SheffieldSolar/Geocode/`.

You may also wish to clear all locally cached data by running the following command from terminal/command-prompt:

```
geocode --clear-cache
```

## Developers

### Running Tests

```
>> cd <root-dir>
>> python -m unittest Tests.test_geocode
```

## Appendix

### Code Point Open

As of version 0.4.15, the Code Point Open data is packaged with this repository, however the following instructions can be used to update the local Code Point Open dataset without re-installing the Geocode library.

Contains OS data © Crown copyright and database right 2018

Contains Royal Mail data © Royal Mail copyright and Database right 2018

Contains National Statistics data © Crown copyright and database right 2018

#### Updating Code Point Open

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

### GMaps

If you want to make use of the Google Maps API, you can load an API key using the command:

```
>> geocode --load-gmaps-key "<your-api-key>"
```

N.B. The API key will be stored in plaintext in the `geocode/google_maps/key.txt` file within your local installation. This is not secure and only appropriate when using the Geocode library for development purposes.

See [here](https://developers.google.com/maps/documentation/javascript/get-api-key) for help getting an API key.

Any queries you make to the GMaps API will be cached to `geocode/google_maps/gmaps_cache_<version>.p` so that repeated queries are faster and cheaper.

### ONS

The Geocode library makes use of data from the Office for National Statistics in order to geocode Lower Layer Super Output Areas (LLSOAs) in England and Wales. The first time you make use of the `Geocoder.geocode_llsoa()` method, the LLSOA (December 2011) Population Weighted Centroids data will be downloaded from the ONS API and cached locally in `geocode/ons_nrs/llsoa_centroids_<version>.p`. The first time you make use of the `Geocoder.reverse_geocode_llsoa()` method, the LLSOA (December 2011) Boundaries EW data will be downloaded from the ONS API and cached locally in `geocode/ons_nrs/llsoa_boundaries_<version>.p`. More information [here](https://geoportal.statistics.gov.uk/datasets/lower-layer-super-output-areas-december-2011-population-weighted-centroids) and [here](https://geoportal.statistics.gov.uk/datasets/lower-layer-super-output-areas-december-2011-boundaries-ew-bsc).

### NRS

The Geocode library makes use of data from National Records of Scotland in order to geocode Lower Layer Super Output Areas (LLSOAs) in Scotland. The raw population-weighted centroids and boundary data is available from the NRS website, but for convenience and performance reasons the data has been re-formatted and re-projected and is packaged with this library. More information [here](https://www.nrscotland.gov.uk/statistics-and-data/geography/our-products/census-datasets/2011-census/2011-census-supporting-information).

Contains NRS data © Crown copyright and database right 2020

Contains Ordnance Survey data © Crown copyright and database right 2020

### data.gov.uk

The Geocode library makes use of the Westminster Parliamentary Constituencies (December 2015) Generalised Clipped Boundaries in Great Britain&dagger; from data.gov.uk in order to geocode constituencies according to their (unweighted) geospatial centroid. More information [here](https://data.gov.uk/dataset/24c282a1-1330-427e-b154-36ff3bfa5dac/westminster-parliamentary-constituencies-december-2015-generalised-clipped-boundaries-in-great-britain). The geospatial centroids have been calculated using ESRI ArcMap's 'Feature To Point' toolbox and the derived lookup data is packaged within this respository as a pipe-separated variable file: `geocode/gov/constituency_centroids.psv`. The first time you make use of the `Geocoder.geocode_constituency()` method, this file will be loaded and cached back to `geocode/gov/constituency_centroids_<version>.p` for faster loading next time.

&dagger;Contains public sector information licensed under the Open Government Licence v3.0.

### NGESO Data Portal

The Geocode library makes use of GSP/GNode GIS boundaries developed by Sheffield Solar. In May 2020, these region definitions were uploaded to National Grid ESO's Data Portal - see [here](https://data.nationalgrideso.com/system/gis-boundaries-for-gb-grid-supply-points). The first time you make use of the `Geocoder.reverse_geocode_gsp()` method, the GIS data is downloaded from the Data Portal API at runtime.

Supported by National Grid ESO Open Data

Subject to [NGESO Open Licence](https://data.nationalgrideso.com/licence)

To update your locally cached boundary definitions, clear your local cache:

```
geocode --clear-cache
```
