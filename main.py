# Author: James Westwood
# Copyright: Office for National Statistics
# coding: utf-8

# imports
import os
import re
import json
import datetime

import requests
from gssutils import *
from urllib.parse import urljoin

from modules import (csvs_to_pandas,
                     get_mapping_dicts,
                     get_mapping_and_scraper,
                     override_writer,
                     write_csv)

"""
sdg-csv-data-filler is the first module in a data pipeline to take
data from the SDG data repo and make it exportable as CSVW.
"""

# setting paths to directories and files
remote_data_url = "https://github.com/ONSdigital/sdg-data/tree/develop/data"
cwd = os.getcwd()
data_path = os.path.join(cwd, 'data')
out_path = os.path.join(cwd, 'out')
overrides_yam = (os.path.join(cwd, "overrides_dict.yaml"))

POC3_urls = ['https://raw.githubusercontent.com/ONSdigital/sdg-data/develop/data/indicator_11-7-1.csv',
                'https://raw.githubusercontent.com/ONSdigital/sdg-data/develop/data/indicator_16-9-1.csv']

# +
cubes = Cubes("base_info.json")

def entry_point(data_url):

    # define pattern for name matching outside the for-loop.
    # pattern used for writing out later
    pattern = r"(indicator_\d{1,2}-\d{1,2}-\d+\.csv)$"

    # create an empty results dict
    results = {}

    for url in POC3_urls:
        # get the overrides dict for this dataset
        overrides_dict = get_mapping_dicts(overrides_yam, url)
        
        # Create df
        df = csvs_to_pandas(url)
        
        # get dataset name
        file_name = f"{re.search(pattern, url).group(0)}"
        if df is None or df.empty:  # sometimes no df will be returned
            results[file_name] = False
            continue  # empty dfs are skipped
        
        # Apply transformations to the df
        df = override_writer(df, overrides_dict)
        
        # Create a basic "Scraper" class to handle metadata
        # NOTE - also writes info.json to ./
        mapping, scraper = get_mapping_and_scraper(url, overrides_dict)
        
        # Create a new cube with mapping
        cubes.add_cube(scraper, df, scraper.title, info_json_dict=mapping)
        
    cubes.output_all()

    return results


# -

if __name__ == "__main__":
    results = entry_point(data_url=remote_data_url)



