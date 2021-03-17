from globals import *
from setup import *
import pandas as pd
import matplotlib.pyplot as mpl
import os
import datetime as dt
import logging as lg
# lg.basicConfig(level=lg.DEBUG)

# Data source: https://github.com/CSSEGISandData/COVID-19
# References:
#   https://stackoverflow.com/questions/25224545/filtering-multiple-items-in-a-multi-index-python-panda-dataframe
#   https://www.digitalocean.com/community/tutorials/how-to-use-logging-in-python-3
#   https://stackoverflow.com/questions/23198053/how-do-you-shift-pandas-dataframe-with-a-multiindex
#   https://pandas.pydata.org/pandas-docs/stable/user_guide/advanced.html#sorting-a-multiindex

def preprocess_data(countries):
    '''
    Read all the individual files and combine into a single summarized file
    :return:
    df = pandas.DataFrame
    '''

    c_date = "Date"
    c_date_format_in = "%m-%d-%Y"
    c_country_region = "Country_Region"
    c_province_state = "Province_State"


    df_rest = pd.DataFrame()
    df_us   = pd.DataFrame()

    c_pattern = ".csv"

    files_all = os.listdir(path=path_all)
    files_us  = os.listdir(path=path_us)


    df_in = pd.read_csv(os.path.join(path_dat, "IN-covid.csv"), na_values=["nan"])


    # Daily Reports
    for file in files_all:
        if file.endswith(c_pattern):

            file_path_all = os.path.join(path_all,file)
            date = dt.datetime.strptime(file.strip(".csv"), c_date_format_in).date()
            lg.debug("Date: {}".format(date))

            # Read All
            df_all = pd.read_csv(file_path_all, na_values=["nan"])

            # Slice cols
            if df_all.shape[1] == 14: # For 14 Column csv files

                df_all = df_all.iloc[:,[2, 3, 4, 7, 8, 9]]

            elif df_all.shape[1] in [8,6]: # For 8 or 6 Column csv files

                df_all = df_all.iloc[:,0:6]
                df_all = df_all\
                        .rename(columns={"Province/State": c_province_state, "Country/Region": c_country_region})


            df_all[c_date] = date # Set Date

            df_all.loc[df_all.Country_Region.eq(c_in) & df_all.Province_State.isnull(), c_province_state] = c_all # Set state values

            df_all.loc[df_all.Country_Region.isin([c_mch,c_hk,c_mc]), c_country_region] = c_ch # Set country values


            # Remove US and keep Rest, Set Indices
            df_rest_tmp = df_all[df_all.Country_Region.ne(c_us)] \
                        .set_index([c_country_region,c_province_state,c_date])


            # Combine files
            df_rest = df_rest\
                     .append(df_rest_tmp, ignore_index=False)

    # Slice final
    df_rest = df_rest.iloc[:,0:4]


    # Daily Reports - US
    for file in files_us:
        if file.endswith(c_pattern):

            file_path_us = os.path.join(path_us,file)
            date = dt.datetime.strptime(file.strip(".csv"),c_date_format_in).date()
            lg.debug("Date : {}".format(date))

            # Read US
            df_us_tmp = pd.read_csv(file_path_us,
                                usecols=[0, 1, 2, 5, 6, 7],  # 'US' Template
                                na_values=["nan"])
            df_us_tmp[c_date] = date # Set Date


            # Set Indices
            df_us_tmp = df_us_tmp.set_index([c_country_region,c_province_state,c_date])


            # Combine files
            df_us = df_us\
                    .append(df_us_tmp, ignore_index=False)


    # Combine Rest & US
    df = df_rest\
        .append(df_us, ignore_index=False)


    # Filter by Countries
    df = df[df.index.isin(countries,level=c_country_region)]


    # Sort, drop col, and fill nan
    df = df.sort_index(ascending=True)
    df = df.iloc[:,1:4].fillna(0)


    return df



def generate_summary(raw):
    '''
    COVID Case summary
    :return:
    daily_cummulative = pandas.DataFrame
    '''

    df_nums = raw #.iloc[:,1:4]

    # Previous days count / Shifted Province level
    df_prev_day = df_nums.groupby(level=1)\
                         .shift(1)

    daily_cummulative = df_nums - df_prev_day


    return daily_cummulative



def show_charts(cumulative, filter):
    '''
    Plot selected regions
    :return: N/A
    '''

    # Filter by cross section
    mpl.plot(cumulative.xs(filter))

    mpl.savefig('Covid_cases.png')
    # mpl.show()
    mpl.close()



def download_file(data, path=None):
    '''
    Download the summary file as .csv
    :return: N/A
    '''

    data.to_csv('..\..\summary.csv', index=True)



def main():
    '''
    The main method!
    :return:N/A
    '''

    countries = ["US","China","India"]

    df_raw = preprocess_data(countries)
    df_summ = generate_summary(df_raw)
    # show_charts(df_summ, ("US","California") )
    # show_charts(df_summ, ("US","New York") )
    download_file(df_summ)



if __name__ == "__main__":
    main()

