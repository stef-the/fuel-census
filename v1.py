# https://geocoding.geo.census.gov/geocoder/Geocoding_Services_API.html

import pandas as pd
import requests
import json
import time
import datetime
from cli_color_py import red, yellow, green, bold

electric_stations_dataset = "electric_stations (Apr 7 2024).csv"
date_search = "2023-02"  # format: yyyy-mm-dd (leave empty to scan all)

skipped_sets = 0

# for geocoding census API
benchmark = "Public_AR_Census2020"  # eg: "Public_AR_Census2020", "2020", etc.
vintage = "Census2010_Census2020"

# read charging/refueling stations dataset
df = pd.read_csv(electric_stations_dataset, low_memory=False)

# create a mask to filter by date
mask = df["Open Date"].str.contains(str(date_search), case=False, na=False)
masked_df = df[mask]
masked_df = masked_df.copy()
masked_df.loc[:, "Qualify for Tax Benefits"] = "None"

# read cross-check datasets and convert their FIPS codes to strings for later searching
lowincome_df = pd.read_csv("Urban-Low-income-Communities-Dataset.csv", low_memory=False)
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

total = masked_df.shape[0]
done, total_time = 0, 0
print(str(total) + " data points")
print("Running for " + date_search)

# iter through each filtered charging/refueling station
for index, row in masked_df.iterrows():
    start_time = time.time()
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
    done += 1

    # retrieve data from the api
    response = requests.get(request_api)
    try:
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

                            masked_df.loc[:, ("Qualify for Tax Benefits", index)] = (
                                True
                                if (len(case1) > 0 and case1[0] == "yes")
                                or (len(case2) > 0 and case2[0] == "YES")
                                else False
                            )

                        else:
                            print("FIPS code not found in the response")
                            skipped_sets += 1
                    else:
                        skipped_sets += 1
                else:
                    skipped_sets += 1
            else:
                skipped_sets += 1
    except json.decoder.JSONDecodeError as e:
        print("Error decoding JSON: " + str(e))
        skipped_sets += 1

    time_delta = time.time() - start_time
    total_time += time_delta
    percentage_complete = int((done / total) * 100)

    print(
        (red(str(percentage_complete) + "%") if percentage_complete < 33 else yellow(str(percentage_complete) + "%") if percentage_complete < 67 else green(str(percentage_complete) + "%"))
        + " - "
        + str(done)
        + "/"
        + str(total)
        + " - Estimate time remaining (h/m/s): "
        + bold(str(datetime.timedelta(seconds=total_time / done * (total - done))))
        + "\n"
    )

csv_data = masked_df.to_csv("output.csv", index=True)
print("Skipped sets: " + str(skipped_sets))
