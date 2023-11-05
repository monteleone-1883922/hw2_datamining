
import pandas as pd


def load_data():
    return pd.read_csv("data/amazon_products_gpu.tsv",sep="\t")

def top10(df,column, numeric = True, higher = True):
    if numeric:
        df[column] = df[column].str.replace(".","").str.replace(",",".").astype(float)
    return df.sort_values(by=column, ascending=not higher).head(10)



def test():
    pass





if __name__ == "__main__":
    test()