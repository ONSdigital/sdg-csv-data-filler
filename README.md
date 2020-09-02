# sdg-csv-data-filler

The SDG CSV data filler is the first script in a pipeline to convert SDG data in CSV format, to CSVW format, which is a [W3C Standard](https://www.w3.org/standards/). 

The script will become part of pipeline which may be integrated into the build scripts for the [UK SDG site](https://sdgdata.gov.uk/). 

Later it may be integrated into the build scripts for the [Open SDG platform](https://open-sdg.org/), meaning that countries and cities which use the platform may choose to have the CSVW export function on their site. 

## Functions of the script

The script functions as follows:
1. It scrapes the UK SDG [data repository of the SDG site](https://github.com/ONSdigital/sdg-data) for links to CSV files 
2. Downloads the CSV data from the URL.
3. It checks settings in the [overrides yaml file](https://github.com/jwestw/sdg-csv-data-filler/blob/master/overrides_dict.yaml) makes 3 different data transformations unique to any dataset and to each column as follows:
  - If parameter 'fill_gaps' is True for the data set it will fill any gaps, `nan`, `NaN` or `Null` values with the gap filler value for that column  
  - If parameter 'fix_headers' is True it will standardise the headers by replacement. This is currently not used, but may need to be in the future. It is currently set to False
  - if parameter 'standardise_cells'is True it will replace any non-standard values specified, and replace them with a standard value, e.g. it may replace 'male', 'Males' and 'M' with the standard value 'Male'.
4. It outputs the transformed data in CSV format to a folder called "out"

### To do:
- Code for a fix_headers function.
- Code unit tests for each function in modules.py
- Use a Python-github library to get data from github instead of scraping
  - suggested to try [PyGithub](https://pygithub.readthedocs.io/en/latest/introduction.html)

## Related projects

* [CSVlint](https://github.com/GSS-Cogs/csvlint.rb)
* [Open SDG](https://open-sdg.org/)

## Licensing

SDG CSV data filler is under an MIT licence.
