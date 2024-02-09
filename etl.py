import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

from src.transform import __clean_record_to_lumu_format, __rank_with_percentage
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

def transform(df: pd.DataFrame) -> list[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Function to transform a DataFrame by ranking and cleaning records, and returning three DataFrames.
    
    Args:
        df (pd.DataFrame): A pandas DataFrame
    
    Returns:
        df_ips_rank (pd.DataFrame): A pandas DataFrame containing the ranked and percentage-transformed IP records
        df_host_rank (pd.DataFrame): A pandas DataFrame containing the ranked and percentage-transformed host records
        df_lumu_formated (pd.DataFrame): A pandas DataFrame with the records cleaned and formatted to Lumu's specifications
    """    
    total_rows = df[0].size
    df_ips_rank = __rank_with_percentage(df.groupby(6).size(), total_rows)
    df_host_rank = __rank_with_percentage(df.groupby(10).size(), total_rows)
    df_lumu_formated = __clean_record_to_lumu_format(df)

    return df_ips_rank, df_host_rank, df_lumu_formated

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
    total_rows = df.index.size
    __parser_and_send_to_lumu(df, total_rows)
    
    print(f"Total records: {total_rows}")
    __print_report(df_ips_rank, 'Client IPs Rank')
    __print_report(df_host_rank, 'Host Rank')


if __name__ == "__main__":
    df = extract()
    df_ips_rank, df_host_rank, df = transform(df)
    load(df_ips_rank, df_host_rank, df)