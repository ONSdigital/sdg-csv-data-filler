import pandas as pd

def get_mapping_dicts(path_to_yaml):
    """
    Loads dictionaries for the gap filling and mapping of column names 
    from locally stored .yaml files
    
        Parameters:
            gap_filler_yaml (string): Path to the yaml file storing the values
                to fill gaps with in each column
            header_mapping_yaml (string): Path to the yaml file storing the 
                names to change headers to for each column
        Returns:
            dict: dict_from_yam
            
    """
    with open(path_to_yaml) as file:
        dict_from_yam = safe_load(file)
    
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
            remote data folder """
    page = requests.get(url)
    soup = bs(page.text, 'html.parser')
    csv_link_pattern = r"\/ONSdigital\/sdg-data\/blob\/develop\/data\/indicator_\d-\d{1,2}-\d{1,2}\.csv"
    to_repl_pattern = r"\/sdg-data\/blob\/develop"
    replacement_pattern = "/sdg-data/develop"
    list_of_links = []
    attrs={'href': re.compile(csv_link_pattern)}
    for link in soup.findAll('a', attrs=attrs):
        link = link.get('href')
        link = re.sub(to_repl_pattern, replacement_pattern, link)
        yield ("https://raw.githubusercontent.com"+link)


def csvs_to_pandas(url):
    """Provided with a URL of a file, the fucntion will check if the CSV
    is populated and if not empty return a Pandas dataframe of the CSV
        Parameters:
            url (string): the URL of a CSV file to be captured
        Returns:
            pd.DataFrame: a Pandas dataframe of the CSV
    """
    if "no data for this indicator yet" in str(bs(requests.get(url).text)):
        return None
    else:
        return pd.read_csv(url)

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
            Boolean: True is written, False if not written """ 
    status = True

    # If the out_path isn't there, make it
    if not os.path.exists(out_path):
        os.makedirs(out_path, exist_ok=True)

    try:
        full_path = os.path.join(out_path, filename)
        df.to_csv(csv_dir, index=False)
    except Exception as e:
        extended_e = Exception("Error encountered when attempting csv write to csv_dir: ".format(csv_dir)) #from e
        print(extended_e)
        return False

    return status

def delete_random_values(df, holes=20):
    """
    Smashes holes in your dataframe to the approximate number that you
    request (randint might choose the same cell twice)
    """
    for i in range(holes):
        row = random.randint(1, df.shape[0]-1)
        col = random.randint(0, df.shape[1]-1)
        df.iloc[row, col] = float('nan')
    return df