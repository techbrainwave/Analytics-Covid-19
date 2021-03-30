from globals import *
from setup import *
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sb
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
    c_pattern = ".csv"

    # US Add data (US) – 1/22/2020 to 4/11/2020
    date_st = dt.datetime.strptime("01-22-2020", c_date_format_in).date()
    date_en = dt.datetime.strptime("04-11-2020", c_date_format_in).date()
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
    daily_data = pandas.DataFrame
    '''

    df_nums = raw #.iloc[:,1:4]


    # Previous days count / Shifted Province level
    df_prev_day = df_nums.groupby(level=[0,1])\
                         .shift(periods=1)



    # Remove nan after shift down
    df_na = df_prev_day.isna()
    df_prev_na = df_prev_day[df_na.all(axis='columns')]


    df_nums_na = df_prev_na.join(df_nums, lsuffix='NA')
    df_nums_na = df_nums_na.iloc[:,3:7] # Remove nan cols
    df_nums_na.iloc[:,:] = 0 # Set value to zero

    df_prev_day.update(df_nums_na)


    # Cummulative counts
    daily_data = df_nums - df_prev_day

    daily_data = daily_data.join(raw,rsuffix='_Cummulative')

    return daily_data



def show_charts(df_data):
    '''
    Plot data
    :return: N/A
    '''

    # Remove columns
    df_daily = (df_data.iloc[:,0:3]).copy()


    # Clean data / remove bad data entries
    df_daily = df_daily[~df_daily.index.isin(c_bad,level=c_province_state)]


    # Summarize data at State level
    df_state_tot = df_daily. \
                    groupby(level=1).sum()

    # Remove entry All from states
    df_state_tot = df_state_tot[~df_state_tot.index.isin([c_all],level=c_province_state)]



    # Read stats.csv
    file_path_stats = os.path.join(path_stats, c_sfile)
    df_stats = pd.read_csv(file_path_stats)

    df_stats = df_stats.iloc[:,1:6] # Remove country col
    df_stats = df_stats[df_stats.State.ne(c_all)] # Remove entry All

    df_stats = df_stats.set_index(c_state) # Index by state


    # Merge covid & stats data
    df_state_data = df_state_tot.join(df_stats)



    # Summarize to daily at Country level
    df_cntot_daily = df_daily. \
                    groupby(level=[0,2]).sum()


    # Split data into US,IN,CH
    df_ctry_us = df_cntot_daily[df_cntot_daily.index.isin([c_us],level=c_country_region)]
    df_ctry_in = df_cntot_daily[df_cntot_daily.index.isin([c_in],level=c_country_region)]
    df_ctry_ch = df_cntot_daily[df_cntot_daily.index.isin([c_ch],level=c_country_region)]

    df_ctry_us=(df_ctry_us.reset_index(level=0)).iloc[:,1:4]
    df_ctry_in=(df_ctry_in.reset_index(level=0)).iloc[:,1:4]
    df_ctry_ch=(df_ctry_ch.reset_index(level=0)).iloc[:,1:4]


    # Remove corrupt entries
    rem_us = dt.datetime.strptime("02-23-2021", c_date_format_in).date()
    rem_in = dt.datetime.strptime("06-10-2020", c_date_format_in).date()
    df_ctry_us.drop(index=rem_us,inplace=True)
    df_ctry_in.drop(index=rem_in,inplace=True)



    # Histogram
    plt.figure()
    df_state_data.diff().hist(color="k", alpha=0.5, bins=50)
    plt.savefig(os.path.join(path_charts,"Data_distribution.png"))
    plt.close()


    # Correlation
    correlation = df_state_data.corr()
    correlation.to_csv(os.path.join(path_charts,'Correlation_matrix.csv'))

    # Heatmaps
    sb.heatmap(correlation,linewidths=0.4)
    plt.savefig(os.path.join(path_charts,'Correlation_matrix.png'), bbox_inches='tight')
    plt.close()


    # Country data
    plt.plot(df_ctry_us)
    plt.savefig(os.path.join(path_charts,'Covid_cases_US.png'))
    plt.close()
    plt.plot(df_ctry_in)
    plt.savefig(os.path.join(path_charts,'Covid_cases_IN.png'))
    plt.close()
    plt.plot(df_ctry_ch)
    plt.savefig(os.path.join(path_charts,'Covid_cases_CH.png'))
    plt.close()



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

    show_charts(df_out)
    download_file(df_out)


if __name__ == "__main__":
    main()

