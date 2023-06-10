#! /usr/bin/python3
# -*- coding: utf-8 -*-

import concurrent.futures
import subprocess
import pandas as pd
import time

def get_data(srp_id):
    '''
    Obtain the metadata of the SRA id
    '''
    command = f"pysradb metadata {srp_id} --detailed"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"Error al ejecutar el comando: {result.stderr}")
        return None

    output = result.stdout
    lines = output.split("\n")
    header = lines[0].split('\t')
    data = [line.split('\t') for line in lines[1:] if line]

    df = pd.DataFrame(data, columns=header)
    return df