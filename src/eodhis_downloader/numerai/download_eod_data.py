import pandas as pd
from tqdm import tqdm
import numerapi
from datetime import datetime

from eodhis_downloader.eodhd_map.build_eodhd_map import create_tickername_to_bloomberg_mapping
from eodhis_downloader.quote_downloader.download_quotes import download_tickers_and_map_tickername_to_bloomberg, read_quotes

## PARAMETERS
DATA_FOLDER = 'data/eodhist'
PATH_HISTORIC_DATA = f'{DATA_FOLDER}/historic_data.csv'

def load_concat_all_tickers(data_folder):
    # Today:
    today = datetime.now().strftime('%Y-%m-%d')

    # Get historic Data:
    napi = numerapi.SignalsAPI()
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
    df.to_parquet(f'{DATA_FOLDER}/{today}.parquet')

if __name__ == "__main__":
    # Create mapping file:
    create_tickername_to_bloomberg_mapping()
    # Download all ticker and rename them. If you pass debug = True, it will download a fraction of all tickers.
    download_tickers_and_map_tickername_to_bloomberg(debug=False)
    # Concat all the files into one dataframe
    load_concat_all_tickers(data_folder='data/eodhist')
