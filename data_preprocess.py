import pandas as pd
import os
import datetime as dt

# References:
#   https://stackoverflow.com/questions/25224545/filtering-multiple-items-in-a-multi-index-python-panda-dataframe


def preprocess_data():
    '''
    Read all the individual files and combine into a single summarized file
    :return: N/A
    '''
    date_format_in = "%m-%d-%Y"

    df_rest = pd.DataFrame()
    df_us   = pd.DataFrame()


    path_all = os.fspath("..\..\COVID-19-master\csse_covid_19_data\csse_covid_19_daily_reports")
    path_us  = os.fspath("..\..\COVID-19-master\csse_covid_19_data\csse_covid_19_daily_reports_us")

    files_all = os.listdir(path=path_all)
    files_us = os.listdir(path=path_us)


    # Daily Reports
    for file in files_all:
        if file.endswith("21.csv"):

            file_path_all = os.path.join(path_all,file)
            date = dt.datetime.strptime(file.strip(".csv"),date_format_in).date()


            # Read All
            df_all = pd.read_csv(file_path_all,
                                 # index_col= [1,0],
                                 usecols=[2, 3, 4, 7, 8, 9, 10, 12, 13],  # 'All' Template
                                 na_values=["nan"])
            df_all['Date'] = date # Set Date


            # Remove US and keep Rest, Set Indices
            df_rest_tmp = df_all[df_all.Country_Region.ne("US")] \
                        .set_index(["Country_Region","Province_State","Date"])


            # Combine files
            df_rest = df_rest\
                     .append(df_rest_tmp, ignore_index=False)


    # Daily Reports - US
    for file in files_us:
        if file.endswith(".csv"):

            file_path_us = os.path.join(path_us,file)
            date = dt.datetime.strptime(file.strip(".csv"),date_format_in).date()


            # Read US
            df_us_tmp = pd.read_csv(file_path_us,
                                usecols=[0, 1, 2, 5, 6, 7, 8, 10, 13],  # 'US' Template
                                na_values=["nan"])
            df_us_tmp['Date'] = date # Set Date


            # Set Indices
            df_us_tmp = df_us_tmp.set_index(["Country_Region","Province_State","Date"])


            # Combine files
            df_us = df_us\
                    .append(df_us_tmp, ignore_index=False)


    # Combine Rest & US
    df = df_rest\
        .append(df_us, ignore_index=False)


    # Filter by Countries
    df = df[df.index.isin(["US","China","India"],level="Country_Region")]


    # Sort
    df = df.sort_index(ascending=True)
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