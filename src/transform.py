
import pandas as pd

def __transform_ips_rank (df: pd.DataFrame, total_rows: int) -> pd.DataFrame:
    """
    Function to transform IP addresses rank based on count and percentage.
    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be processed.
        total_rows (int): The total number of rows in the DataFrame.
    Returns:
        pd.DataFrame: The DataFrame with IP addresses sorted by percentage in descending order.
    """
    groups_ips = df.groupby(6).size()
    group_percentages = groups_ips / total_rows * 100
    ips_df = pd.DataFrame({'Count': groups_ips, 'Percentage': group_percentages})
    ips_df['Percentage'] = ips_df['Percentage'].map('{:,.2f}%'.format)
    
    return ips_df.sort_values(by=['Percentage'], ascending=False)

def __transform_host_rank (df: pd.DataFrame, total_rows: int) -> pd.DataFrame:
    """
    Transforms the given DataFrame by grouping it by the 10th column and calculating the percentage of each group.
    
    Args:
        df (pd.DataFrame): The DataFrame to be transformed.
        total_rows (int): The total number of rows in the DataFrame.
        
    Returns:
        pd.DataFrame: The transformed DataFrame with columns 'Count' and 'Percentage', sorted in descending order by 'Percentage'.
    """
    
    groups_host = df.groupby(10).size()
    group_percentages = groups_host / total_rows * 100
    host_df = pd.DataFrame({'Count': groups_host, 'Percentage': group_percentages})
    host_df['Percentage'] = host_df['Percentage'].map('{:,.2f}%'.format)

    
    return host_df.sort_values(by=['Percentage'], ascending=False)

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