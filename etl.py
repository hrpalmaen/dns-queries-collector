import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

from src.transform import __transform_ips_rank, __transform_host_rank, __clean_record_to_lumu_format
from src.load import __parser_and_send_to_lumu, __print_report

load_dotenv()

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
    df_lumu_formated = __clean_record_to_lumu_format(df)
    return __transform_ips_rank(df, total_rows), __transform_host_rank(df, total_rows), df_lumu_formated

def load(df_ips_rank: pd.DataFrame, df_host_rank: pd.DataFrame, df: pd.DataFrame) -> None:
    """
    Load function to process dataframes and send reports to Lumu.
    
    Args:
        df_ips_rank (pd.DataFrame): The dataframe of IP ranks.
        df_host_rank (pd.DataFrame): The dataframe of host ranks.
        df: pd.DataFrame (pd.DataFrame): The dataframe that will be sen.
    
    Returns:
        None
    """
    __parser_and_send_to_lumu(df, df.index.size)
    
    __print_report(df_ips_rank, 'Client IPs Rank')
    __print_report(df_host_rank, 'Host Rank')


if __name__ == "__main__":
    df = extract()
    df_ips_rank, df_host_rank, df = transform(df)
    load(df_ips_rank, df_host_rank, df)