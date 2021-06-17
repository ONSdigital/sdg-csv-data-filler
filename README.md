# sdg-csv-data-filler

The SDG CSV data filler is the first script in a pipeline to convert SDG data in CSV format, to CSVW format, which is a [W3C Standard](https://www.w3.org/standards/). 

The script will become part of pipeline which may be integrated into the build scripts for the [UK SDG site](https://sdgdata.gov.uk/). 

Later it may be integrated into the build scripts for the [Open SDG platform](https://open-sdg.org/), meaning that countries and cities which use the platform may choose to have the CSVW export function on their site. 

# What has been done so far

This work was carried out by James Westwood with a lot of help from the Integrated Data Platform Dissemination (IDP-D) team (formerly Connected Open Government Statistics (COGS)). The primary function of the script is to process the SDG data for publication as CSV on the web (CSVW) formatted data on the IDP platform. 

SDG data is published CSV files in the Tidy Data arrangement which is a good starting point for this script to work from, but not all the terms in the data are compatible with CSVW and no gaps would be tolerated so they needed to be filled – hence the name of the script that James created was the “sdg-csv-data-filler”. 

CSVW is a World Wide Web Consortium (W3C) data format which has a CSV published along side its metadata in json format.  The IPD-D team have a data pipeline to transform CSV data into CSVW, RDF, Data Cubed and the new international standard for the exchange of statistical data SDMX. James created a script to apply changes to the SDG CSV data to make it suitable for the IPD pipeline. 

On 11th Jan 2021 James worked with Rob Barry and Michael Adams from the IPD -D team to further develop the script he had built. The script so far can be seen on the Proof of Concept Branch 3 or POC3 branch of the repository.

The script was then cloned and modified to be compatible with the Jenkins workflow that the IDP-D team has set up. Two datasets were published on the IDP-D platform (know as PMD, Publish My Data) as a proof of concept from a data producer's site to finish on the PMD platform. 


## Schematic diagrams of the the script

Overview of csvdata-filler and CSVW system

![csvdata-filler and CSVW system](https://github.com/jwestw/sdg-csv-data-filler/blob/master/img_for_readme/CSVW%20process%20overview%203.jpg?raw=true)

Overview of csv-data-filler functional processes

![csv-data-filler functional processes](https://github.com/jwestw/sdg-csv-data-filler/blob/master/img_for_readme/CSVW%20process%20overview%202.jpg?raw=true)

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
