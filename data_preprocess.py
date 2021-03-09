import pandas as pd
import os

# References:
#   https://stackoverflow.com/questions/25224545/filtering-multiple-items-in-a-multi-index-python-panda-dataframe


def preprocess_data():
    '''
    Read all the individual files and combine into a single summarized file
    :return: N/A
    '''

    # File locations
    file_path_all = os.path.join("..\..\COVID-19-master\csse_covid_19_data\csse_covid_19_daily_reports", "01-01-2021.csv")
    file_path_us  = os.path.join("..\..\COVID-19-master\csse_covid_19_data\csse_covid_19_daily_reports_us", "01-01-2021.csv")


    # Read All
    df_all = pd.read_csv(file_path_all,
                         # index_col= [1,0],
                         usecols = [2,3,4,7,8,9,10,12,13], # 'All' Template
                         na_values=["nan"])

    # Remove US and keep Rest, Set Index
    df_rest = df_all[df_all.Country_Region.ne("US")] \
              .set_index(["Country_Region","Province_State"])


    # Read US
    df_us = pd.read_csv(file_path_us,
                         index_col=[1,0], # Set Index
                         usecols=[0,1,2,5,6,7,8,10,13],    # 'US' Template
                         na_values=["nan"])


    # Combine Rest & US
    df = df_rest\
        .append(df_us, ignore_index=False)


    # Filter by Countries
    df = df[df.index.isin(["US","China","India"],level="Country_Region")]


    # Sort
    df = df.sort_values(by=["Last_Update"],ascending=False)\
        .sort_index(ascending=True)
    # print()


def download_file():
    '''
    Download the summary file as .csv
    :return: N/A
    '''
    pass


def main():
    '''
    The main method!
    :return:N/A
    '''
    preprocess_data()


if __name__ == "__main__":
    main()