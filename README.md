# sdg-csv-data-filler

The SDG CSV data filler is the first script in a pipeline to convert SDG data in CSV format, to CSVW format, which is a [W3C Standard](https://www.w3.org/standards/). 

The script will become part of pipeline which may be integrated into the build scripts for the [UK SDG site](https://sdgdata.gov.uk/). 

Later it may be integrated into the build scripts for the [Open SDG platform](https://open-sdg.org/), meaning that countries and cities which use the platform may choose to have the CSVW export function on their site. 

## Functions of the script

The (planned) functions are this script are as follows:
1. Download the CSV data from the [data repository of the SDG site](https://github.com/ONSdigital/sdg-data).
2. In any of the disagregation columns, fill any blanks with "T"
3. In any value column, fill any blanks with NaN, None, or Null (tbc)
4. Output the edited data in CSV format to a folder called "out"

## Latest: Testing a proof of concept

### Testing all the functions

* find_csv_urls and csvs_to_pandas are tested in https://colab.research.google.com/drive/1McY5SRuVgnjPsZb1g-VFNHGo7RdDzVAK?usp=sharing as I wasn't able to run the url requests from an ONS machine.
* proof_of_concept is used to bring all functions together for intial test on small dataset
* csvsample_to_pandas used in place of csvs_to_pandas. A local path is used instead of a URL
* csvsample_to_pandas accepts a percent (pct) argument to randomly sample a percentage of records from a csv to create a df
* get_mapping_dicts tested to create dictionaries from yaml files
* The structure for the yaml files is now laid out to work with this code
* `df.fillna` works with the filler dictionary passed as the `value` parameter
* `standardise_cell_values` seems to work (more rigorous testing needed). It takes any existing value and substitutes it. e.g. subsitituting 'males' with 'Male'. 

### To do:
- Use a Python-github library to get data from github instead of scraping
  - suggested to try [PyGithub](https://pygithub.readthedocs.io/en/latest/introduction.html)

## Related projects

* [CSVlint](https://github.com/GSS-Cogs/csvlint.rb)
* [Open SDG](https://open-sdg.org/)

## Licensing

SDG CSV data filler is under an MIT licence.
