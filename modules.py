# Author: James Westwood
# Copyright: Office for National Statistics
# coding: utf-8

# Core imports
import os
import random
import re
import datetime
import json
from typing import Optional, Dict

# Third party imports
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from yaml import safe_load
from gssutils import Scraper, utils
from gssutils.metadata.dcat import Dataset


def get_mapping_dicts(path_to_yaml, url):
    """
    Loads dictionaries for the gap filling and mapping of column names
    from locally stored .yaml files. Returns only the dictionary for
    the specified dataset as the url is the unique identifier for the dataset.
    This unique identifier is used as a key on the returned dictionary.
    If the unique identifier (key) does not exist, `none` is returned.

        Parameters:
            path_to_yaml (string): Path to the yaml file storing the values
                to override values with
            url (string): The source url for the dataset which is a
                unique identifier
        Returns:
            dict: dict_from_yam
    """
    with open(path_to_yaml) as file:
        dict_from_yam = safe_load(file)
        if url not in dict_from_yam.keys():
            print(f"{url} not in dictionary")
        dict_from_yam = dict_from_yam.get(url)
    return dict_from_yam


def find_csv_urls(url):
    """
    Provided with a data folder URL, this function finds the URLS
    of the CSV files within the folder. A generator is yielded with
    the links of all files in the folder.
        Parameters:
            url (string): the URL of the repo/folder which contains
                the CSV files to be captured
        Yields:
            string: generator, the next URL for the CSV file in the
            remote data folder"""
    page = requests.get(url)
    soup = bs(page.text, 'html.parser')
    csv_link_pattern = r"\/ONSdigital\/sdg-data\/blob\/develop\/data\/indicator_\d-\d{1,2}-\d{1,2}\.csv"
    to_repl_pattern = r"\/sdg-data\/blob\/develop"
    replacement_pattern = "/sdg-data/develop"
    attrs = {'href': re.compile(csv_link_pattern)}
    for link in soup.findAll('a', attrs=attrs):
        link = link.get('href')
        link = re.sub(to_repl_pattern, replacement_pattern, link)
        yield ("https://raw.githubusercontent.com"+link)


def csvs_to_pandas(url_to_csv):
    """Provided with a URL of a file, the function will check if the CSV
    is populated and if not empty return a Pandas dataframe of the CSV
        Parameters:
            url (string): the URL of a CSV file to be captured
        Returns:
            pd.DataFrame: a Pandas dataframe of the CSV
    """
    str_response = str(bs(requests.get(url_to_csv).text, 
                    features="html.parser"))
    if "no data for this indicator yet" in str_response:
        return None
    else:
        return pd.read_csv(url_to_csv)

# TODO make sure this is used
def prevent_bad_replacement(overrides_dict, df):
    """Checks that there is not any values equal to the "placeholder-values"
        Placeholder values are used as keys in the overrides dict.
        If they did exist in the dataframe this could cause a bad replacement.
        It is very unlikely that this is needed, but just here as an extra safety step"""
    place_holder_values = ['FILL_NA', 'OldValue1', 'OldValue2', 'OldValue3', 'to']
    results_dict = {}
    for search_value in place_holder_values:
        if search_value in df.values:
            results_dict[search_value] = True
            raise Exception(f"""
            Value {search_value} has been
            found in the dataframe""")
    return None


def csvsample_to_pandas(path_to_file, pct=1.0):
    """A function to create a sample extract of a csv as a dataframe
        Parameters:
            path_to_file (string): full path to csv file
            p (float): decimal amount of lines to extract

        Returns:
            pd.Dataframe
            """
    p = pct/100
    # keep the header, then take only 1% of lines
    # if random from [0,1] interval is greater than 0.01 the row will be skipped
    df = pd.read_csv(
             path_to_file,
             header=0,
             skiprows=lambda i: i>0 and random.random() > p)
    return df

def fill_gaps(df, gap_filler_dict):
    """
    Given a Pandas dataframe and a dictionary containing the column names
    the correct 'fillers' for each column, this function will fill
    each column with the correct values when empty cells are found.
        Parameters:
            pd_df (pd.Dataframe): the variable to which the dataframe
                containing the csv data is assigned
            gap_filler_dict (dict): a dictionary with column name and value
                to fill gaps as key value pairs, e.g.
                {"Age":"All","Sex":"T"}
        Returns:
            pd.Dataframe: A dataframe with all cells full"""
    df = df.fillna(gap_filler_dict, axis=1)
    return df

def standardise_cell_values(pd_df, dict_of_nonstandard_standard):
    """
    Maps non-standard values e.g. "Males" to standard values like "M".
    Mapping is carried out on a column-specific basis.
    """
    df = (pd_df.replace
          (to_replace=dict_of_nonstandard_standard,
          value=None))
    return df

def write_csv(df, out_path, filename):
    """
    Converts a Pandas dataframe to CSV and writes it out to a local folder.
        Parameters:
            pd_df (pd.Dataframe): The pandas data frame of the data
            path (string): the path of the local "out" folder
        Returns:
            Boolean: True if is written, False if not written"""
    status = True

    # If the out_path isn't there, make it
    if not os.path.exists(out_path):
        os.makedirs(out_path, exist_ok=True)

    try:
        full_path = os.path.join(out_path, filename)
        df.to_csv(full_path, index=False)
    except Exception:
        extended_e = Exception(f"""
        Error encountered when attempting csv write to {full_path}: """)
        print(extended_e)
        status = False

    return status

def override_writer(df, overrides_dict):
    """Takes the data frame and makes column-specific replacements or
        overrides. If fix_headers is True (False is default), it will
        change the headers to name in the overides dict.
        If standardise_cells is True (default), it will search for the value
        to be replaced and if found value will be replaced.
        If fill_gaps is True (default) it will fill any gaps with the replacement
        or "gap filler" value.

        Parameters:
            df (pd.Dataframe): dataframe to be processed
            overrides_dict (dict): The overrides dictionary specific to the
                dataset being processed

        Returns:
            pd.Dataframe: complete with requested value overrides
    """
    fix_headers = overrides_dict['fix_headers']
    standardise_cells = overrides_dict['standardise_cells']
    fill_gaps = overrides_dict['fill_gaps']
    if fix_headers:
        #not used at the moment
        pass
    if fill_gaps:
        for column in df.columns:
            if column in ['value','Value']:
                continue #skipping because Value is never a key in the dict
            #replacing string 'nan' with numpy.nan
            df[column].replace('nan', np.nan, inplace=True)
            df[column].fillna(
                value=overrides_dict[column]['FILL_NA'],
                inplace=True)
    if standardise_cells:
        for column in df.columns:
            if column in ['value','Value']:
                continue #skipping because Value is never a key in the dict
            orig_dtype = str(df[column].dtype)
            df[column] = df[column].astype(str)
            df[column] = df[column].replace(to_replace=overrides_dict[column])
            #df[column] = df[column].astype(orig_dtype)
            df[column] = df[column].map(lambda x: utils.pathify(x) if isinstance(x, str) else x)
   
    return df


# +
def get_mapping_and_scraper(url, overrides_dict):
    """
    Creates a scraper class with a default info.json using the url of the csv in question
        Where a mapping describing the concepts within the columns has been provided, 
        the function will make use of it
    """
    # create dict from the base_info.json (which is a template for the metadat json)
    with open("base_info.json", "r") as f:
        info_dict = json.load(f)
        
    # Get the metadata from the SDG metadata endpoint
    indicator = url.split("_")[-1].split(".")[0]   # get indicator name from url
    # use indicator name to define the SDG metadata url
    meta_url = "https://sdgdata.gov.uk/sdg-data/en/meta/{}.json".format(indicator)
    # fetch SDG metadata
    r = requests.get(meta_url)
    if r.status_code != 200:
        raise Exception('Cannot find metadata at url "{}" for source "{}".'.format(meta_url, _url))
    # Put metadata json response into Python dict
    metadata = r.json() 
        
    # Populate the info dictionary
    info_dict["dataURL"] = url

    # If there is mapping defined in overrides_dict.yaml it will 
    if "mapping" in overrides_dict.keys():
        info_dict["transform"]["columns"] = overrides_dict["mapping"]
        mapping = overrides_dict["mapping"]
    else:
        mapping = {}

    # Getting the time now as a string, according to the Y-M-D format 
    # TODO: Why is "T00:00" needed?
    dt_now = datetime.datetime.now().strftime("%Y-%m-%d") + "T00:00"
    info_dict["published"] = dt_now

    # Writing out the info dict to json
    with open("info.json", "w") as f:
        json.dump(info_dict, f)
        
    # use the now dataset-specific info json to create a scraper
    scraper = Scraper(seed="info.json")
    dataset: Dataset = scraper.dataset

    # setting dataset parameter "title" from values pulled from sdg metadata
    dataset.title = metadata.get("indicator_available", metadata.get("indicator_name"))

    description_fields_to_concat = ["computation_definitions", "other_info", "computation_calculations"]
    description_lines = [metadata.get(f) for f in description_fields_to_concat]
    # setting dataset parameter "description" from values pulled from sdg metadata
    dataset.description = "\n".join([l for l in description_lines if l is not None])

    # setting dataset parameter "landingPage" from values pulled from sdg metadata
    dataset.landingPage = metadata.get("source_url_1")

    # TODO: Is get_maybe_date func meant to be defined inside this get_mapping_and_scraper function?
    def get_maybe_date(metadata: Dict, prop_name: str, or_val: Optional[str] = None) -> Optional[str]:
        """
        Parses DD/mm/YYYY date and returns an ISO 8601 datetime string.
        """
        maybe_value = metadata.get(prop_name)
        if not maybe_value:
            return or_val
        return datetime.datetime.strptime(maybe_value, "%d/%m/%Y").isoformat()

    # setting dataset parameter "issued" from values pulled from sdg metadata
    # assigns the dt_now string if no relase data in metadata
    dataset.issued = get_maybe_date(metadata, "source_release_date_1", dt_now)
    pattern = re.compile("^.+_(\\d+)$")
    source_release_numbers = [int(re.search(pattern, k).group(1)) for k in metadata.keys() if k.startswith("source_release_date_")]
    dt_modified = get_maybe_date(metadata, f"source_release_date_{max(source_release_numbers)}") if len(source_release_numbers) > 0 else dt_now
    # dataset.modified = dt_modified # Don't even bother trying to set this as it'll ignore whatever youset.

    # TODO: Are 
    # Hardcoding 'license' 'create' 'publisher' properties of dataset. 
    dataset.license = "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/"
    dataset.creator = "https://www.ons.gov.uk"
    dataset.publisher = "https://www.ons.gov.uk"
    
    # setting dataset parameter "keyword" from values pulled from sdg metadata
    # replacing ";" with "," in the list of keywords
    dataset.keyword = ",".join(metadata.get("data_keywords").split(";"))
    
    return mapping, scraper
    
