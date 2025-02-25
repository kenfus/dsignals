from genericpath import exists
import pandas as pd
from tqdm import tqdm
import numerapi
from datetime import datetime
import os
from eodhis_downloader.eodhd_map.build_eodhd_map import create_tickername_to_bloomberg_mapping
from eodhis_downloader.quote_downloader.download_quotes import DATA_FOLDER, download_tickers_and_map_tickername_to_bloomberg, read_quotes

DATA_FOLDER = 'data/eodhist'
def load_concat_all_tickers(file_name_path, only_current_universe=False):
    # Get historic Data:
    napi = numerapi.SignalsAPI()
    # Create data folder
    os.makedirs(DATA_FOLDER, exist_ok=True)
    if only_current_universe:
        ticker_universe = pd.Series(napi.ticker_universe(), name='bloomberg_ticker')
    else:
        # Sometimes, on windows, the file is not correctly overwritten and the pipeline crashes.
        if os.path.exists(PATH_HISTORIC_DATA):
            os.remove(PATH_HISTORIC_DATA)
        PATH_HISTORIC_DATA = f'{DATA_FOLDER}/historic_data.csv'
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
    df.to_parquet(file_name_path)

def create_apply_mapping_download_eod_data(file_name_path, only_current_universe):
    # Create mapping file:
    create_tickername_to_bloomberg_mapping()
    # Download all ticker and rename them. If you pass debug = True, it will download a fraction of all tickers.
    download_tickers_and_map_tickername_to_bloomberg()
    # Concat all the files into one dataframe
    load_concat_all_tickers(file_name_path=file_name_path, only_current_universe=only_current_universe)

if __name__ == "__main__":
    create_apply_mapping_download_eod_data(file_name_path='data/eodhist/2022-04-17.parquet')
