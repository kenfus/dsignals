from genericpath import exists
import pandas as pd
from tqdm import tqdm
import numerapi
from datetime import datetime
import os
from eodhis_downloader.eodhd_map.build_eodhd_map import create_tickername_to_bloomberg_mapping
from eodhis_downloader.quote_downloader.download_quotes import download_tickers_and_map_tickername_to_bloomberg, read_quotes

def load_concat_all_tickers(data_folder):
    # Today:
    today = datetime.now().strftime('%Y-%m-%d')

    # Get historic Data:
    napi = numerapi.SignalsAPI()
    # Create data folder
    os.makedirs(data_folder, exist_ok=True)
    PATH_HISTORIC_DATA = f'{data_folder}/historic_data.csv'
    napi.download_validation_data(dest_filename=PATH_HISTORIC_DATA)

    # Read ticker universe:
    ticker_universe = pd.read_csv(PATH_HISTORIC_DATA).bloomberg_ticker.unique()
    tickers_df = []

    for ticker in tqdm(ticker_universe):
        df_ticker = read_quotes(ticker)
        if df_ticker is not None:
            df_ticker['bloomberg_ticker'] = ticker
            tickers_df.append(df_ticker.reset_index())
    df = pd.concat(tickers_df)
    df.to_parquet(f'{data_folder}/{today}.parquet')

def create_apply_mapping_download_eod_data(data_folder, debug=False):
    # Create mapping file:
    create_tickername_to_bloomberg_mapping()
    # Download all ticker and rename them. If you pass debug = True, it will download a fraction of all tickers.
    download_tickers_and_map_tickername_to_bloomberg(debug=debug)
    # Concat all the files into one dataframe
    load_concat_all_tickers(data_folder=data_folder)

if __name__ == "__main__":
    create_apply_mapping_download_eod_data(data_folder='data/eodhist')
