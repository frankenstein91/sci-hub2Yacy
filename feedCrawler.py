#!/bin/env python3
# -*- coding: utf-8 -*-

# only needed for set_option
import pandas as pd

from dask import dataframe as ddf
from dask.distributed import Client
import argparse
import random
import os
import requests

def main():
    #region argparse
    parser = argparse.ArgumentParser(description='Send Sci-Hub URLs to Yacy for indexing')
    parser.add_argument('-i','--input', help='Input file', required=True, type=str)
    parser.add_argument('--shs', help='Sci-Hub servers', metavar="https://host:port", required=True, nargs='+', type=str)
    parser.add_argument('--ys', help='YaCy servers', metavar="http://host:port", required=True, nargs='+', type=str)
    # add arg group for Yacy user credentials
    yacyUser = parser.add_argument_group('YaCy user credentials')
    yacyUser.add_argument('--yuser', help='YaCy username', type=str)
    yacyUser.add_argument('--ypass', help='YaCy password', type=str)
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input):
        print("ERROR: File not found " + args.input)
        exit(os.EX_NOINPUT)
    #endregion
    
    client = Client()
    dois = ddf.read_csv(args.input, sep='\t', header=None, names=['doi'], dtype={'doi': 'str'})
    # add URL column to the dataframe
    dois['url'] = dois.apply(lambda row: random.choice(args.shs)+'/'+row['doi'], axis=1, meta=('url', 'str'))

    # create the YaCy API Call URLs
    dois['yacy'] = dois.apply(lambda row: f"{random.choice(args.ys)}/Crawler_p.html?crawlingDomMaxPages=10000&range=wide&crawlingMode=url&crawlingURL={row['url']}&crawlingstart=NewCrawlSciHub&xsstopw=on&indexMedia=on&indexText=on&crawlingDepth=1&directDocByURL=on", axis=1, meta=('yacy', 'str'))

    dois = client.compute(dois)
    dois = dois.result()
    for yacy in dois['yacy']:
        if args.yuser and args.ypass:
            requests.get(yacy, auth=(args.yuser, args.ypass))
        else:
            requests.get(yacy)


if __name__ == '__main__':
    main()