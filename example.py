from geocode import Geocoder

def main():
    with Geocoder() as geocoder:
        postcodes = ["S3 7RH", "S3 7", "S3", None, None, "S3 7RH"]
        addresses = [None, None, None, "Hicks Building, Sheffield", "Hicks", "Hicks Building"]
        results = geocoder.geocode(postcodes, addresses)
        for postcode, address, (lat, lon, status) in zip(postcodes, addresses, results):
            print(f"Postcode: `{postcode}`    Address: `{address}`")
            if status == 0:
                print("    Failed to geocode!")
            else:
                print(f"    {lat:.4f}, {lon:.4f}    ->  {geocoder.status_codes[status]}")

def main_pandas():
    import pandas as pd
    with Geocoder() as geocoder:
        data = {"postcodes": ["S3 7RH", "S3 7", "S3", None, None, "S3 7RH"],
                "addresses": [None, None, None, "Hicks Building, Sheffield", "Hicks",
                              "Hicks Building"]}
        df = pd.DataFrame(data)
        results = geocoder.geocode(df.postcodes, df.addresses)
        for postcode, address, (lat, lon, status) in zip(df.postcodes, df.addresses, results):
            print(f"Postcode: `{postcode}`    Address: `{address}`")
            if status == 0:
                print("    Failed to geocode!")
            else:
                print(f"    {lat:.4f}, {lon:.4f}    ->  {geocoder.status_codes[status]}")

if __name__ == "__main__":
    main()
    main_pandas()