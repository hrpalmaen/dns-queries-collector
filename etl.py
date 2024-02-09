import pandas as pd

MAX_SIZE = 500

def extract(file = 'queries') -> pd.DataFrame:
    return pd.read_csv(file, sep=r'\s|#', engine='python', header=None)

def __split_df(df: pd.DataFrame, total_rows: int, name: str, max_size: int = MAX_SIZE) -> pd.DataFrame:
    for i in range(0, total_rows, max_size):
        block = df.iloc[i:i + max_size]
        print("-----------------------------------------------------")
        print(block)
        print("-----------------------------------------------------")
        block.to_csv(f'records_{i+1}-{i+max_size}_{name}', header=False)


def __transform_ips_rank (df: pd.DataFrame, total_rows: int) -> None:
    groups_ips = df.groupby(6).size()
    group_percentages = groups_ips / total_rows * 100
    ips_df = pd.DataFrame({'Count': groups_ips, 'Percentage': group_percentages})
    ips_df['Percentage'] = ips_df['Percentage'].map('{:,.2f}%'.format)
    print('Client IPs Rank')
    __split_df(ips_df.sort_values(by=['Percentage'], ascending=False), ips_df['Percentage'].size, 'Client_IPs_Rank')

def __transform_host_rank (df: pd.DataFrame, total_rows: int) -> None:
    groups_host = df.groupby(10).size()
    group_percentages = groups_host / total_rows * 100
    host_df = pd.DataFrame({'Count': groups_host, 'Percentage': group_percentages})
    host_df['Percentage'] = host_df['Percentage'].map('{:,.2f}%'.format)

    print('Host Rank')
    __split_df(host_df.sort_values(by=['Percentage'], ascending=False), host_df['Percentage'].size, 'Host_Rank')

def transform(df: pd.DataFrame) -> pd.DataFrame:
    total_rows = df[0].size
    print(f"Total records: {total_rows}")
    __transform_ips_rank(df, total_rows)
    __transform_host_rank(df, total_rows)

if __name__ == "__main__":
    df = extract()
    df = transform(df)