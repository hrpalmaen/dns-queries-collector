import os
import requests
import pandas as pd

MAX_SIZE = 500
URL_LUMU = "https://api.lumu.io/collectors/{0}/dns/queries?key={1}"

def __print_report(df: pd.DataFrame, name_report: str) -> None:
    """
    A function that prints the given report name and
    the first 10 rows of the DataFrame without the header.
    
    Args:
        df (pd.DataFrame): The input DataFrame to be printed.
        name_report (str): The name of the report.
        
    Returns:
        None
    """
    print(name_report)
    print("-----------------------------------------------------")
    print(df.head(10).to_string(header=False))
    print("-----------------------------------------------------")


def __parser_and_send_to_lumu(
        df: pd.DataFrame, total_rows: int, max_size: int = MAX_SIZE) -> None:
    """
    Parses the input pandas DataFrame into blocks of maximum size 'max_size'
    and sends each block to Lumu using the __send_to_lumu function.

    Args:
        df (pd.DataFrame): The input pandas DataFrame.
        total_rows (int): The total number of rows in the DataFrame.
        max_size (int, optional): The maximum size of each block. Defaults to MAX_SIZE.
    Returns:
        None
    """    
    for i in range(0, total_rows, max_size):
        block = df.iloc[i:i + max_size]
        parser = block.to_json(orient='records')
        __send_to_lumu(parser)

def __send_to_lumu(parser: list[dict]) -> None:
    """
    Send the data to LUMU.
    
    Args:
        parser (list[dict]): The data to be sent to LUMU.
        
    Returns:
        None
    """
    try:
        requests.post(
            URL_LUMU.format(os.getenv('COLLECTOR_ID'), os.getenv('LUMU_CLIENT_KEY')),
            data=parser,
            headers={'Content-Type': 'application/json'}
        )
    except requests.exceptions.RequestException as e:
        print('error request lumus',e)

