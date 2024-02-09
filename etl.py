import os
import pandas as pd
from datetime import datetime

from src.transform import __transform_ips_rank, __transform_host_rank
from src.load import __split_df_and_save

DATETIME_NOW = datetime.utcnow()

def extract(file = 'queries') -> pd.DataFrame:
    """
    Read a CSV file into a DataFrame.

    Args:
        file (str, optional): The file path or buffer to read from. Defaults to 'queries'.

    Returns:
        pd.DataFrame: The DataFrame containing the data from the CSV file.
    """
    return pd.read_csv(file, sep=r'\s|#', engine='python', header=None)

def transform(df: pd.DataFrame) -> list[pd.DataFrame, pd.DataFrame]:
    """
    This function takes a pandas DataFrame as input and returns a list of two pandas DataFrames.
    It calculates the total number of rows in the input DataFrame and then calls two helper
    functions to transform the input DataFrame.
    
    Args:
        df (pd.DataFrame): The input pandas DataFrame.
    
    Returns:
        list[pd.DataFrame, pd.DataFrame]: A list containing two transformed DataFrames.
    """
    total_rows = df[0].size
    print(f"Total records: {total_rows}")
    return __transform_ips_rank(df, total_rows), __transform_host_rank(df, total_rows)

def load(df_ips_rank: pd.DataFrame, df_host_rank: pd.DataFrame) -> None:
    """
    Load function to create a folder with the current timestamp, and then
    split and print the client IPs and host ranks.
    
    Args:
        df_ips_rank (pd.DataFrame): The dataframe of IP ranks.
        df_host_rank (pd.DataFrame): The dataframe of host ranks.
    
    Returns:
        None
    """
    name_folder = str(DATETIME_NOW.timestamp())
    os.makedirs(name_folder)

    print('Client IPs Rank')
    __split_df_and_save(df_ips_rank, df_ips_rank['Percentage'].size, 'Client_IPs_Rank', name_folder)
    print('Host Rank')
    __split_df_and_save(df_host_rank, df_host_rank['Percentage'].size, 'Host_Rank', name_folder)

if __name__ == "__main__":
    df = extract()
    df_ips_rank, df_host_rank = transform(df)
    load(df_ips_rank, df_host_rank)