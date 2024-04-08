# https://geocoding.geo.census.gov/geocoder/Geocoding_Services_API.html

import pandas as pd
import requests

electric_stations_dataset = "electric_stations (Apr 7 2024).csv"
date_search = "2023-01"  # format: yyyy-mm-dd (leave empty to scan all)

# for geocoding census API
benchmark = "Public_AR_Census2020"  # eg: "Public_AR_Census2020", "2020", etc.
vintage = "Census2010_Census2020"

# read charging/refueling stations dataset
df = pd.read_csv(electric_stations_dataset)

# create a mask to filter by date
mask = df["Open Date"].str.contains(str(date_search), case=False, na=False)
masked_df = df[mask]
masked_df["Qualify for Tax Benefits"] = "None"

# read cross-check datasets and convert their FIPS codes to strings for later searching
lowincome_df = pd.read_csv("Urban-Low-income-Communities-Dataset.csv")
NMTC_df = pd.read_excel(
    "NMTC_2016-2020_ACS_LIC_Sept1_2023.xlsb", engine="pyxlsb", header=0
)
lowincome_df["2010 Census Tract Number FIPS code. GEOID"] = lowincome_df[
    "2010 Census Tract Number FIPS code. GEOID"
].astype(str)
NMTC_df["2020 Census Tract Number FIPS code. GEOID"] = NMTC_df[
    "2020 Census Tract Number FIPS code. GEOID"
].astype(str)


# search API
api_url = r"https://geocoding.geo.census.gov/geocoder/geographies/address?"

# basic function to prepare spaces (' ') for url
space_encoding = lambda x: x.replace(" ", "+")

print(str(masked_df.shape[0]) + " data points")

# iter through each filtered charging/refueling station
for index, row in masked_df.iterrows():
    args = [
        "street=" + space_encoding(str(row["Street Address"])),
        "city=" + space_encoding(str(row["City"])),
        "state=" + row["State"],
        "zip=" + str(row["ZIP"]),
        "benchmark=" + benchmark,
        "vintage=" + vintage,
        "format=json",
    ]

    # eg geocoder/geographies/onelineaddress?address=4600+silver+hill+rd%2C+20233&benchmark=2020&vintage=2010&format=json
    # eg geocoder/geographies/onelineaddress?address=4600+silver+hill+rd%2C+20233&benchmark=Public_AR_Census2020&vintage=Census2010_Census2020&format=json
    request_api = api_url + "&".join(args)
    print('\n'+request_api)

    # retrieve data from the api
    response = requests.get(request_api)
    dataset = response.json()["result"]["addressMatches"]
    for data in dataset:
        if "geographies" in data:
            geographies = data["geographies"]
            if "Census Blocks" in geographies:
                census_blocks = geographies["Census Blocks"]
                # Assuming you want to extract the FIPS code of the first block
                if census_blocks:
                    first_block = census_blocks[0]
                    fips_code = first_block.get("GEOID", None)
                    if fips_code:
                        fips_code_trimmed = str(fips_code)[:-4]
                        print("FIPS Code:", fips_code_trimmed)

                        lowincome_row = lowincome_df[
                            lowincome_df[
                                "2010 Census Tract Number FIPS code. GEOID"
                            ].str.contains(fips_code_trimmed)
                        ]
                        NTMC_row = NMTC_df[
                            NMTC_df[
                                "2020 Census Tract Number FIPS code. GEOID"
                            ].str.contains(fips_code_trimmed)
                        ]

                        case1 = lowincome_row[
                            "Urban Low Income Community (yes, no)"
                        ].values

                        case2 = NTMC_row[
                            "Does Census Tract Qualify For NMTC Low-Income Community (LIC) on Poverty or Income Criteria?"
                        ].values

                        
                        masked_df["Qualify for Tax Benefits"][index] = True if case1 == ['yes'] or case2 == ['YES'] else False

                    else:
                        print("FIPS code not found in the response")

csv_data = masked_df.to_csv("output.csv", index=True)
