import pandas as pd
from pathlib import Path

MAX_SIZE = 500

def __split_df_and_save(
        df: pd.DataFrame, total_rows: int, name_load: str,
        name_folder: str, max_size: int = MAX_SIZE) -> None:
    """
    Split the input DataFrame into smaller blocks and save them as separate CSV files.
    
    Args:
        df (pd.DataFrame): The input DataFrame to be split.
        total_rows (int): The total number of rows in the DataFrame.
        name_load (str): The name of the data load.
        name_folder (str): The name of the folder where the CSV files will be saved.
        max_size (int, optional): The maximum size of each block. Defaults to MAX_SIZE.
        
    Returns:
        None
    """
    print("-----------------------------------------------------")
    print(df.head(5).to_string(header=False))
    print("-----------------------------------------------------")

    for i in range(0, total_rows, max_size):
        block = df.iloc[i:i + max_size]
        path = Path(name_folder) / f'records_{i+1}-{i+max_size}_{name_load}'
        block.to_csv(path, header=False)
