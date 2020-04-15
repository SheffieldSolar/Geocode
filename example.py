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
        # Geocode some Constituencies...
        constituencies = ["Sheffield Central", "Sheffield Hallam"]
        results = geocoder.geocode_constituency(constituencies)
        for constituency, (lat, lon) in zip(constituencies, results):
            print(f"Constituency: `{constituency}`")
            print(f"    {lat:.3f}, {lon:.3f}")

if __name__ == "__main__":
    main()