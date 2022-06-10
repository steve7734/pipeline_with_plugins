import argparse
import importlib
import pandas as pd

def invokePlugin(plugin, df: pd.DataFrame):
    try:
        print(f'invoking plugin for system {args.systemName}')
        df = plugin.Process(df)
        return df
    except BaseException as err:
         print(f'An error occurred during the plugin for system {args.systemName} : {err}')

if __name__ == '__main__':

    print('------------------------------------------------ retrieve the file from the previous step ---------------------------------------------------')

    # retrieve the system name from command line args
    parser = argparse.ArgumentParser()
    parser.add_argument('--systemName', type=str, required=True)
    args = parser.parse_args()
    print(f'Processing daily file: {args.systemName}')

    # read in the parquet file
    fileName = f'Q_{args.systemName}_remote_22_05_09.parquet'
    df = pd.read_parquet(fileName)
    
    # print out the column names
    # for col in df.columns:
    #     print(col)

    # get the sum of the seconds column
    totalSeconds = df['SECONDS'].sum()
    print(f'totalSeconds before plugin {totalSeconds}')

    print('------------------------------------------------ process any plugin for this system ---------------------------------------------------')

    # import the plugin for this system if there is one
    try:   
        plugin = importlib.import_module(f'{args.systemName}_plugin', '.')
        df = invokePlugin(plugin, df)
    except BaseException as err:
        print(f'no plugin found for system {args.systemName}')

    totalSeconds = df['SECONDS'].sum()
    print(f'totalSeconds after plugin {totalSeconds}')

    print('------------------------------------------------ do the usual processing for the step ---------------------------------------------------')

    print('doing the usual processing for this step....')
   
       
    



