import pandas as pd
import requests
import json
import time
import datetime
from cli_color_py import red, yellow, green, blue, bold
import concurrent.futures
from tqdm import tqdm
import multiprocessing

electric_stations_dataset = "electric_stations (Apr 7 2024).csv"
date_search = "2023-01-01"  # format: yyyy-mm-dd (leave empty to scan all)
skipped_sets = multiprocessing.Value('i', 0)

# for geocoding census API
benchmark = "Public_AR_Census2020"  # eg: "Public_AR_Census2020", "2020", etc.
vintage = "Census2010_Census2020"

# read charging/refueling stations dataset
df = pd.read_csv(electric_stations_dataset, low_memory=False)

# create a mask to filter by date
mask = df["Open Date"].str.contains(str(date_search), case=False, na=False)
masked_df = df[mask].copy()
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

# Define a function to process a single row of data
def process_row(row):
    global skipped_sets
    args = [
        "street=" + space_encoding(str(row["Street Address"])),
        "city=" + space_encoding(str(row["City"])),
        "state=" + row["State"],
        "zip=" + str(row["ZIP"]),
        "benchmark=" + benchmark,
        "vintage=" + vintage,
        "format=json",
    ]

    request_api = api_url + "&".join(args)

    response = requests.get(request_api)
    try:
        dataset = response.json()["result"]["addressMatches"]
        for data in dataset:
            if "geographies" in data:
                geographies = data["geographies"]
                if "Census Blocks" in geographies:
                    census_blocks = geographies["Census Blocks"]
                    if census_blocks:
                        first_block = census_blocks[0]
                        fips_code = first_block.get("GEOID", None)
                        if fips_code:
                            fips_code_trimmed = str(fips_code)[:-4]

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
                            
                            return (
                                True
                                if (len(case1) > 0 and case1[0] == "yes")
                                or (len(case2) > 0 and case2[0] == "YES")
                                else False
                            )

                        else:
                            print(red("FIPS code not found in the response"))
                            with skipped_sets.get_lock():
                                skipped_sets.value += 1
                    else:
                        with skipped_sets.get_lock():
                            skipped_sets.value += 1
                else:
                    with skipped_sets.get_lock():
                        skipped_sets.value += 1
            else:
                with skipped_sets.get_lock():
                    skipped_sets.value += 1
    except json.decoder.JSONDecodeError as e:
        #print(red("Error decoding JSON: ") + str(e))
        with skipped_sets.get_lock():
            skipped_sets.value += 1

# Define a function to process a batch of data points
def process_batch(batch):
    global done
    global total_time
    for index, row in batch.iterrows():
        start_time = time.time()
        qualify_tax_benefit = process_row(row)
        with masked_df.get_lock():
            masked_df.at[index, "Qualify for Tax Benefits"] = qualify_tax_benefit

        time_delta = time.time() - start_time
        total_time += time_delta
        with done.get_lock():
            done.value += 1

        percentage_complete = int((done.value / total) * 100)

# Split the data into batches
batch_size = 100  # Adjust as needed
data_batches = [masked_df[i:i+batch_size] for i in range(0, len(masked_df), batch_size)]

# Process each batch in parallel using multiprocessing and show progress bar
with tqdm(total=len(data_batches)) as pbar:
    def update_progress(*_):
        pbar.update()

    with concurrent.futures.ProcessPoolExecutor() as executor:
        for batch in data_batches:
            executor.submit(process_batch, batch).add_done_callback(update_progress)

# Combine the results as needed

csv_data = masked_df.to_csv("output.csv", index=True)
print("Skipped sets: " + str(skipped_sets.value))
