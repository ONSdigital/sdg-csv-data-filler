# sdg-csv-data-filler

The SDG CSV data filler is the first script in a pipeline to convert SDG data in CSV format, to CSVW format, which is a W3C Standard. 

The script will become part of pipeline which may be integrated into the build scripts for the [UK SDG site](https://sdgdata.gov.uk/). 

Later it may be integrated into the build scripts for the Open SDG platform, meaning that countries and cities which use the platform may choose to have the CSVW export function on their site. 

## Functions of the script

The (planned) functions are this script are as follows:
1. Download the CSV data from the [data repository of the SDG site](https://github.com/ONSdigital/sdg-data).
2. In any of the disagregation columns, fill any blanks with "T"
3. In any value column, fill any blanks with NaN, None, or Null (tbc)
4. Output the edited data in CSV format to a folder called "out"

## Licensing

SDG CSV data filler is under an MIT licence.
