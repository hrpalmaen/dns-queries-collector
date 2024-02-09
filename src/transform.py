
import pandas as pd

def __rank_with_percentage(grouped_df: pd.DataFrame, total_rows: int) -> pd.DataFrame:
    """
    Calculate the percentage of each group in the given DataFrame and return a new DataFrame with the count and percentage for each group. 
    Args:
        grouped_df (pd.DataFrame): The DataFrame containing the grouped data
        total_rows (int): The total number of rows in the original DataFrame
    Returns:
        pd.DataFrame: A new DataFrame with the count and percentage for each group, sorted by percentage in descending order
    """
    group_percentages = grouped_df / total_rows * 100
    new_df = pd.DataFrame({'Count': grouped_df, 'Percentage': group_percentages})
    new_df['Percentage'] = new_df['Percentage'].map('{:,.2f}%'.format)
    
    return new_df.sort_values(by=['Percentage'], ascending=False)

def __clean_record_to_lumu_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    A function to reorganize the records in the given DataFrame to a specific format and return the modified DataFrame.
    
    Args:
        df (pd.DataFrame): The input DataFrame to be reorganized.
    
    Returns:
        pd.DataFrame: The reorganized DataFrame with selected and renamed columns.
    """
    # fromater to datetime
    df[0] = df[0] + " " + df[1]
    df[0] = pd.to_datetime(df[0]).dt.tz_localize('UTC')
    # df[0] = df[0].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    # remove @ from client name
    df[5] = df[5].str.replace("@", "")
    # select and rename columns
    new_df = df[[0,5,6,10,12]]
    new_df = new_df.rename(columns={0: 'timestamp', 5: 'client_name', 6: 'client_ip', 10: 'name', 12: 'type'})
    return new_df