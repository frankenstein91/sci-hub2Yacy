#!/bin/env python3
# -*- coding: utf-8 -*-

from dask import dataframe as ddf
import argparse
import random

def main():
    argsparser = argparse.ArgumentParser(description='Send Sci-Hub URLs to Yacy for indexing')
    argsparser.add_argument('-i','--input', help='Input file', required=True, type=str)
    argsparser.add_argument('--shs', help='Sci-Hub servers', required=True, nargs='+', type=str)

    args = argsparser.parse_args()
    dois = ddf.read_csv(args.input, sep='\t', header=None, names=['doi'], dtype={'doi': 'str'})
    # add URL column to the dataframe
    dois['url'] = dois.apply(lambda row: 'http://'+random.choice(args.shs)'/'+row['doi'], axis=1, meta=('url', 'str'))

    print(dois.head())
    


if __name__ == '__main__':
    main()