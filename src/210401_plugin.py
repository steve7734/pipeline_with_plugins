import pandas as pd

# All plugins have a method called Process that takes a dataframe, modifies it and returns it.
def Process(df: pd.DataFrame):
    print('inside plugin: multiply SECONDS by 2')
    df.SECONDS *=2
    return df
