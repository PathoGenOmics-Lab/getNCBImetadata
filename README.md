# getNCBImetadata

This Python script fetches metadata for a list of SRA IDs using the `pysradb` command line tool. It is designed to handle errors and retry failed requests, making it robust against temporary API issues.

## Requirements

- Python 3
- `pysradb` command line tool
- pandas library
- concurrent.futures library

## How to Run

1. Make sure you have Python 3 and the required libraries installed.
2. Clone this repository or download the script.
3. Run the script from the command line, providing the path to the input file containing SRA IDs and the desired output file:

```
python3 get_sra.py -i input_file.tsv -o output_file.tsv
```
## Input Format

The script expects a TSV file with one SRA ID per line, like this:

```

SRR5486088
SRR5486090
SRR5486091
...
```
## Output Format

The script will output a TSV file containing the fetched metadata. Each row corresponds to one SRA ID.

## Error Handling

If fetching the metadata for an ID fails, the script will log the error and retry it in the next iteration. The script continues to retry failed IDs until it has fetched metadata for all IDs or it has retried an ID too many times.

## Logging

The script logs basic information about its progress, including which SRA ID it's currently processing and any errors it encounters. The log messages are printed to the console.
