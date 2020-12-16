# Author: James Westwood
# Copyright: Office for National Statistics
# coding: utf-8

# imports
import os
import re


from modules import (csvs_to_pandas,
                     get_mapping_dicts, 
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
        # Writing the df to csv locally
        was_written = write_csv(df, out_path, file_name)
        results[file_name] = was_written

    return results


if __name__ == "__main__":
    results = entry_point(data_url=remote_data_url)
