import pandas as pd
from gssutils.utils import pathify
import numpy as np
import re
import os

codelists_urls = [
    {
        "output_name": "sex.csv",
        "url": 'https://sdgdata.gov.uk/sdg-data/values--disaggregation--sex.csv'
    }, 
    {
        'output_name': 'country.csv',
        'url': 'https://sdgdata.gov.uk/sdg-data/values--disaggregation--country.csv'
    }
    ]



for codelist in codelists_urls:
    csv_df = pd.read_csv(codelist['url'])
    
    values = ["All"]
    values += [v for v in list(csv_df.Value) if not "*" in v]
    output_df = pd.DataFrame({'Label': values,
                                'Notation': values,
                                'Parent Notation': [None if x == "All" else "all" for x in values],
                                'Sort Priority': range(0,len(values))}) 

    output_df.Notation = output_df.Notation.apply(pathify)

    outpath = os.path.join("codelists", codelist['output_name'])
    output_df.to_csv(outpath, index=False)