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
    c_pattern = ".csv"

    # US Add data (US) – 1/22/2020 to 4/11/2020
    date_st = dt.datetime.strptime("01-22-2020", c_date_format_in).date()
    date_en = dt.datetime.strptime("04-11-2020", c_date_format_in).date()

    # date_309 = dt.datetime.strptime("03-09-2020", c_date_format_in).date()
    date_321 = dt.datetime.strptime("03-21-2020", c_date_format_in).date()


    df_rest     = pd.DataFrame()
    df_us_early = pd.DataFrame()
    df_us       = pd.DataFrame()


    files_all = os.listdir(path=path_all)
    files_us  = os.listdir(path=path_us)


    # Daily Reports
    for file in files_all:
        if file.endswith(c_pattern):

            file_path_all = os.path.join(path_all,file)
            date = dt.datetime.strptime(file.strip(".csv"), c_date_format_in).date()
            lg.debug("Date: {}".format(date))

            # Read All
            df_all = pd.read_csv(file_path_all)
            df_all.drop_duplicates(inplace=True)


            # Slice cols
            if df_all.shape[1] in [12,14]: # For 12 or 14 Column csv files

                df_all = df_all.iloc[:,[2, 3, 7, 8, 9]]

            elif df_all.shape[1] in [8,6]: # For 8 or 6 Column csv files

                df_all = df_all.iloc[:,[0, 1, 3, 4, 5]]
                df_all = df_all\
                        .rename(columns={"Province/State": c_province_state, "Country/Region": c_country_region})


            df_all[c_date] = date # Set Date

            df_all.loc[df_all.Country_Region.eq(c_in) & df_all.Province_State.isnull(), c_province_state] = c_all # Set state values

            df_all.loc[df_all.Country_Region.isin([c_mch,c_hk,c_mc]), c_country_region] = c_ch # Set country values


            # Remove US and keep Rest, Set Indices
            df_rest_tmp = df_all[df_all.Country_Region.ne(c_us)] \
                            .groupby(by=[c_country_region, c_province_state, c_date]).sum()



            # Combine files non US
            df_rest = df_rest\
                     .append(df_rest_tmp, ignore_index=False)



            # Capture US (early) data only
            if date_st <= date <= date_en:

                df_us_early_tmp = df_all[df_all.Country_Region.eq(c_us)].copy() # US only


                # Files from "01-22-2020"  to "03-21-2020"
                if date <= date_321:
                    df_us_early_tmp.loc[df_us_early_tmp.index, c_province_state] = c_all


                df_us_early_tmp = df_us_early_tmp\
                                    .groupby(by=[c_country_region, c_province_state, c_date]).sum() # Aggregate to State


                # Combine files US (early)
                df_us_early = df_us_early\
                                .append(df_us_early_tmp, ignore_index=False)



    # Slice final
    df_rest = df_rest.iloc[:,0:3]


    # Daily Reports - US
    for file in files_us:
        if file.endswith(c_pattern):

            file_path_us = os.path.join(path_us,file)
            date = dt.datetime.strptime(file.strip(".csv"),c_date_format_in).date()
            lg.debug("Date : {}".format(date))

            # Read US
            df_us_tmp = pd.read_csv(file_path_us,
                                usecols=[0, 1, 5, 6, 7])  # 'US' Template
            df_us_tmp.drop_duplicates(inplace=True)
            df_us_tmp[c_date] = date # Set Date


            # Set Indices
            df_us_tmp = df_us_tmp.\
                        groupby(by=[c_country_region, c_province_state, c_date]).sum()


            # Combine files
            df_us = df_us\
                    .append(df_us_tmp, ignore_index=False)


    # Combine US & US Early
    df_us = df_us\
          .append(df_us_early, ignore_index=False)


    # Combine Rest & US
    df = df_rest\
        .append(df_us, ignore_index=False)


    # Filter by Countries
    df = df[df.index.isin(countries,level=c_country_region)]


    # Sort, drop col, and fill nan
    df = df.sort_index(ascending=True)
    df = df.fillna(0)


    return df



def generate_daily(raw):
    '''
    COVID Case summary
    :return:
    daily_cummulative = pandas.DataFrame
    '''

    df_nums = raw #.iloc[:,1:4]


    # Previous days count / Shifted Province level
    df_prev_day = df_nums.groupby(level=1)\
                         .shift(periods=1)


    # Remove nan after shift down
    df_na = df_prev_day.isna()
    df_prev_na = df_prev_day[df_na.all(axis='columns')]


    df_nums_na = df_prev_na.join(df_nums, lsuffix='NA')
    df_nums_na = df_nums_na.iloc[:,3:7] # Remove nan cols
    df_nums_na.iloc[:,:] = 0 # Set value to zero

    df_prev_day.update(df_nums_na)


    # Cummulative counts
    daily_cummulative = df_nums - df_prev_day

    daily_cummulative = daily_cummulative.join(raw,rsuffix='_Cummulative')

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
    df_out = generate_daily(df_raw)
    # show_charts(df_out, ("US","California") )
    # show_charts(df_out, ("US","New York") )
    download_file(df_out)


if __name__ == "__main__":
    main()

