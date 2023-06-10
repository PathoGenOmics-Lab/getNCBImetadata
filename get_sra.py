#! /usr/bin/python3
# -*- coding: utf-8 -*-

import concurrent.futures
import subprocess
import pandas as pd
import time
import logging
import argparse

__author__ = 'Paula Ruiz-Rodriguez'
__credits__ = ['Paula Ruiz-Rodriguez', 'PathoGenOmics']
__license__ = ['GPL 3']
__version__ = '0.1.0'
__maintainer__ = 'Paula Ruiz-Rodriguez: @paururo'
__email__ = 'paula.ruiz-rodriguez@uv.es'
__status__ = 'developing'
__lab__ = 'PathoGenOmics (I2SysBio)'

# Function to get metadata of the SRA id
def get_data(srp_id):
    '''
    This function runs a command to fetch metadata for a given SRA ID.
    It then parses the command output and returns it as a pandas DataFrame.
    If the command fails, it logs the error and returns None.
    '''
    logging.info(f"Processing SRA ID: {srp_id}")  # Logs the ID that's being processed
    command = f"pysradb metadata {srp_id} --detailed"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        logging.error(f"Error executing the command for SRA ID {srp_id}: {result.stderr}")
        return None, srp_id  # If there's an error, it returns None and the problematic ID

    # If there's no error, it parses the command output and returns it as a DataFrame
    output = result.stdout
    lines = output.split("\n")
    header = lines[0].split('\t')
    data = [line.split('\t') for line in lines[1:] if line]

    df = pd.DataFrame(data, columns=header)
    return df, srp_id

# Function to parse command line arguments
def arg_parser():
    parser = argparse.ArgumentParser(description='Obtain the metadata of the SRA id')
    parser.add_argument('-i', '--input', type=str, required=True, help='Input file name')
    parser.add_argument('-o', '--output', type=str, required=True, help='Output file name')
    args = parser.parse_args()
    return args

# Main function
def main():
    args = arg_parser()  # Parse command line arguments
    list_sras = pd.read_csv(args.input, sep='\t', header=None)  # Read the input file
    srp_ids = list_sras[0].tolist()  # Get a list of SRA IDs

    final_df_list = []  # List to store all successful DataFrames

    while srp_ids:  # As long as there are SRA IDs to process
        error_ids = []  # List to store the IDs that produce errors in this iteration
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            results = executor.map(get_data, srp_ids)  # Fetch metadata for all IDs
        for df, srp_id in results:
            if df is None:  # If an error occurred
                error_ids.append(srp_id)  # Store the problematic ID
            else:  # If no error occurred
                final_df_list.append(df)  # Store the resulting DataFrame
        srp_ids = error_ids  # For the next iteration, only process the IDs that produced errors
        time.sleep(1)  # Wait a bit to not overwhelm the API

    # If we got some results
    if final_df_list:
        final_df = pd.concat(final_df_list, axis=0, ignore_index=True, join='outer')
        logging.info(f"\n{final_df}")  # Log the final DataFrame
        final_df.to_csv(args.output, sep='\t', index=False)  # Save it to a file
    else:  # If we didn't get any results
        logging.error("Unable to obtain data for any of the provided SRA IDs.")  # Log an error message

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()  # Run the main function
