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
        return None, srp_id  # Modificado aquí para devolver srp_id también en caso de error

    output = result.stdout
    lines = output.split("\n")
    header = lines[0].split('\t')
    data = [line.split('\t') for line in lines[1:] if line]

    df = pd.DataFrame(data, columns=header)
    return df, srp_id

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

    final_df_list = []  # Lista para almacenar todos los DataFrames exitosos

    while srp_ids:
        error_ids = []  # Lista para almacenar los IDs que producen errores en esta iteración
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            results = executor.map(get_data, srp_ids)
        for df, srp_id in results:
            if df is None:
                error_ids.append(srp_id)
            else:
                final_df_list.append(df)
        srp_ids = error_ids
        time.sleep(1)

    if final_df_list:  # Si la lista final no está vacía
        final_df = pd.concat(final_df_list, axis=0, ignore_index=True, join='outer')
        logging.info(f"\n{final_df}")
        final_df.to_csv(args.output, sep='\t', index=False)
    else:
        logging.error("Unable to obtain data for any of the provided SRA IDs.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
