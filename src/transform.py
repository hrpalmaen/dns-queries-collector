
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
    
    Parameters:
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
