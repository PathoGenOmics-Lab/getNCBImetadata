#! /usr/bin/python3
# -*- coding: utf-8 -*-

import concurrent.futures
import subprocess
import pandas as pd
import time
import logging
import argparse

def get_data(srp_id):
    '''
    Obtain the metadata of the SRA id
    '''
    logging.info(f"Processing SRA ID: {srp_id}")
    command = f"pysradb metadata {srp_id} --detailed"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        logging.error(f"Error executing the command for SRA ID {srp_id}: {result.stderr}")
        return None

    output = result.stdout
    lines = output.split("\n")
    header = lines[0].split('\t')
    data = [line.split('\t') for line in lines[1:] if line]

    df = pd.DataFrame(data, columns=header)
    return df

def arg_parser():
    parser = argparse.ArgumentParser(description='Obtain the metadata of the SRA id')
    parser.add_argument('-i', '--input', type=str, required=True, help='Input file name')
    parser.add_argument('-o', '--output', type=str, required=True, help='Output file name')
    args = parser.parse_args()
    return args

def main():
    args = arg_parser()
    list_sras = pd.read_csv(args.input, sep='\t', header=None)
    srp_ids = list_sras[0].tolist()
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        df_list = list(executor.map(get_data, srp_ids))
        time.sleep(1)
    # Filtramos la lista para remover los None (casos en los que ocurrieron errores)
    df_list = [df for df in df_list if df is not None]

    if df_list:  # Si la lista no está vacía
        final_df = pd.concat(df_list, axis=0, ignore_index=True, join='outer')
        logging.info(f"Final dataframe:\n {final_df}")
        final_df.to_csv(args.output, sep='\t', index=False)
    else:
        logging.warning("Could not retrieve data for any of the provided SRA IDs.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
