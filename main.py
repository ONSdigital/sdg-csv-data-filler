# Author: James Westwood
# Copyright: Office for National Statistics
# coding: utf-8

# imports
import os
import re
import json

import requests
from gssutils import *
from urllib.parse import urljoin

from modules import (csvs_to_pandas,
                     get_mapping_dicts,
                     get_scraper,
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


def entry_point(data_url):

    # define pattern for name matching outside the for-loop.
    # pattern used for writing out later
    pattern = r"(indicator_\d{1,2}-\d{1,2}-\d+\.csv)$"

    # create an empty results dict
    results = {}

    for _url in POC3_urls:
        # get the overrides dict for this dataset
        overrides_dict = get_mapping_dicts(overrides_yam, _url)
        # Create df
        df = csvs_to_pandas(_url)
        # get dataset name
        file_name = f"{re.search(pattern, _url).group(0)}"
        if df is None or df.empty:  # sometimes no df will be returned
            results[file_name] = False
            continue  # empty dfs are skipped
        
        # Apply transformations to the df
        df = override_writer(df, overrides_dict)
        
        # Create a basic "Scraper" class to handle metadata
        scraper = get_scraper(_url, overrides_dict)
        
        # Use the scraper and the dataframe to create csvw
        # TODO - this could/should probably be a module
        scraper.set_dataset_id("specify-path-here-please")
        scraper.set_base_uri('http://gss-data.org.uk')
        
        out = Path('out')
        out.mkdir(exist_ok=True)

        # Write the csvw
        csvw_transform = CSVWMapping()
        csvw_transform.set_csv(out / file_name)
        csvw_transform.set_mapping(json.load(open('info.json')))
        csvw_transform.set_dataset_uri(urljoin(scraper._base_uri, f'data/{scraper._dataset_id}'))
        csvw_transform.write(out / f'{file_name}-metadata.json')
        
        # NOTE:
        # Title/description is currently being capture by the RDF focussed .trig files in the IDP project.
        # As that's not appropriate here, we're going to read the csvw back in and insert them directly into the json.
        with open(out / f'{file_name}-metadata.json', "r") as f:
            csvw_as_dict = json.load(f)

        indicator = _url.split("_")[-1].split(".")[0]   # get indicator from url
        meta_url = "https://sdgdata.gov.uk/sdg-data/en/meta/{}.json".format(indicator)
        r = requests.get(meta_url)
        if r.status_code != 200:
            raise Exception('Cannot find metadata at url "{}" for source "{}".'.format(meta_url, _url))
        metadata = r.json() # metadata json response as python dict
   
        csvw_as_dict["title"] = metadata["indicator_name"]
        csvw_as_dict["description"] = metadata["other_info"]
        csvw_as_dict["published"] = metadata["source_release_date_1"]
        
        with open(out / f'{file_name}-metadata.json', "w") as f:
            json.dump(csvw_as_dict, f, indent=4)
        
        # Writing the df to csv locally
        was_written = write_csv(df, out_path, file_name)
        results[file_name] = was_written

    return results


if __name__ == "__main__":
    results = entry_point(data_url=remote_data_url)

