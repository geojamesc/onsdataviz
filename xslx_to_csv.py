import os
import pandas as pd
import datetime


def create_start_date_from_data_col(in_date_str):
    """
    s_date = create_start_date_from_data_col('19 April to 2 May 2021')
    --> 19-04-2021

    convert the val

    :param wave_date_str: a str like 19 April to 2 May 2021
    :return: the start date in form dd-mm-YYYY

    copes with the different ways things are written in the date column

    e.g.
    19 April to 2 May 2021 --> 19-04-2021
    7 to 20 September 2020 --> 07-09-2020
    28 December 2020 to 10 January 2021 --> 28-12-2020
    """
    [d_start, d_end] = in_date_str.split(' to ')
    d_parts_s = d_start.split(' ')
    d_parts_e = d_end.split(' ')

    d_start_d, d_start_m, d_start_y, d_end_d, d_end_m, e_end_y = None, None, None, None, None, None

    if len(d_parts_s) == 1:
        d_start_d = d_parts_s[0]
        d_start_m = None
        d_start_y = None
    elif len(d_parts_s) == 2:
        d_start_d = d_parts_s[0]
        d_start_m = d_parts_s[1]
        d_start_y = None
    elif len(d_parts_s) == 3:
        d_start_d = d_parts_s[0]
        d_start_m = d_parts_s[1]
        d_start_y = d_parts_s[2]

    if len(d_parts_e) == 1:
        d_end_d = d_parts_e[0]
        d_end_m = None
        d_end_y = None
    elif len(d_parts_e) == 2:
        d_end_d = d_parts_e[0]
        d_end_m = d_parts_e[1]
        d_end_y = None
    elif len(d_parts_e) == 3:
        d_end_d = d_parts_e[0]
        d_end_m = d_parts_e[1]
        d_end_y = d_parts_e[2]

    if d_start_m is None:
        if d_end_m is not None:
            d_start_m = d_end_m

    if d_start_y is None:
        if d_end_y is not None:
            d_start_y = d_end_y

    start_date = datetime.date(
        int(d_start_y),
        datetime.datetime.strptime(d_start_m, "%B").month,
        int(d_start_d)
    )

    # end_date = datetime.date(
    #     int(d_end_y),
    #     datetime.datetime.strptime(d_end_m, "%B").month,
    #     int(d_end_d)
    # )

    return datetime.datetime.strftime(start_date, "%d-%m-%Y")


def format_cell_pcnt(in_cell_val):
    """
    takes an input value e.g. 0.098888 and converts to a % with 1 digit to right of decimal point
    :param in_cell_val:
    :return:
    """
    return "{:.1f}".format((in_cell_val * 100))


def format_industry_band(in_cell_val):
    out_cell_val = in_cell_val.strip()
    if 'employees' in in_cell_val:
        out_cell_val = ''.join(['Band: ', out_cell_val])
    else:
        out_cell_val = ''.join(['Industry: ', out_cell_val])

    return out_cell_val


def convert_data(xlsx_fn, limit_output_columns=False):
    """

    :param xlsx_fn:  path to .xlsx file
    :param limit_output_columns: when True only output to csv a subset of the columns, those that are focus for viz,
     otherwise, default is to output all columns
    :return:
    """

    all_dates = []

    # which columns we want to apply the format_cell() function to as we read the data in from the xlsx into the df
    columns_to_reformat_by_sheet = {
        'TradingStatus_TS': {3: 'format_industry_band', 4: 'pcnt', 5: 'pcnt', 6: 'pcnt', 7: 'pcnt', 8: 'pcnt', 10: 'pcnt', 11: 'pcnt', 12: 'pcnt'},
        'FinancialPerformance_TS': {3: 'format_industry_band', 4: 'pcnt', 5: 'pcnt', 6: 'pcnt', 7: 'pcnt', 8: 'pcnt', 9: 'pcnt', 10: 'pcnt', 11: 'pcnt', 13: 'pcnt', 14: 'pcnt', 15: 'pcnt'},
        'WorkforceStatus_TS': {2: 'format_industry_band', 3: 'pcnt', 4: 'pcnt', 5: 'pcnt', 6: 'pcnt', 7: 'pcnt', 8: 'pcnt', 11: 'pcnt'},
        'CashFlow_TS': {3: 'format_industry_band', 4: 'pcnt', 5: 'pcnt', 6: 'pcnt', 7: 'pcnt', 8: 'pcnt', 9: 'pcnt', 11: 'pcnt', 12: 'pcnt'}
    }

    # which columns we want to dump out to the csv
    # these are the columns that Francis described as being those to focus on
    # plus a start_date column we will derive
    # order here is the order we will dump the columns out to in the csv
    columns_to_output_by_sheet = {
        'TradingStatus_TS': [
            'Wave',
            'Date',
            'wave_start_date',
            'Industry/ Band',
            'Has permanently ceased trading ',
            'current and started trading',
            'paused trading'
        ],
        'FinancialPerformance_TS': [
            'Wave',
            'Date',
            'wave_start_date',
            'Industry/ Band',
            'Turnover has not been affected',
            'Lower turnover',
            'Higher turnover'
        ],
        'WorkforceStatus_TS': [
            'Wave',
            'Date',
            'wave_start_date',
            'Industry/ Band',
            'On furlough leave ',
            'Working at their normal place of work ',
            'Working remotely instead of at their normal place of work '
        ],
        'CashFlow_TS': [
            'Wave',
            'Date',
            'wave_start_date',
            'Industry/ Band',
            '3 months or less'
        ]
    }

    if os.path.exists(xlsx_fn):
        for sheet in columns_to_reformat_by_sheet:

            # build up a dict mapping column index to conversion function
            # that can be supplied as the convertors param in the pandas
            # read_excel call to run conversion on the cell values
            the_convertors = {}
            columns_to_be_formatted = columns_to_reformat_by_sheet[sheet]
            for col in columns_to_be_formatted:
                # i.e. the_convertors[4] = format_cell_pcnt
                if columns_to_be_formatted[col] == 'pcnt':
                    the_convertors[col] = format_cell_pcnt
                elif columns_to_be_formatted[col] == 'format_industry_band':
                    the_convertors[col] = format_industry_band

            # use the first number column as the pandas index
            pd_index_col = 0
            if sheet == 'WorkforceStatus_TS':
                # except in this case where there is no such column
                pd_index_col = None

            # read the worksheet into a pandas dataframe
            df = pd.read_excel(
                xlsx_fn,
                sheet_name=sheet,
                skiprows=9,  # skip the first 9 rows as these contain textual notes
                header=0,  # index of row (0-based) containing the header (after we have skipped!)
                index_col=pd_index_col,  # which col to use as the pandas index
                na_values='*',  # NULL values
                converters=the_convertors  # convert specified cols as per our defined above the_convertors dict
            )

            # add to the df a new column of start_date of wave using the create_start_date_from_data_col() func
            # applied to the Date column
            df['wave_start_date'] = df['Date'].apply(create_start_date_from_data_col)

            if limit_output_columns:
                columns_to_output = columns_to_output_by_sheet[sheet]
            else:
                columns_to_output = None

            # write the dataframe out as a CSV file
            # use columns to select a subset of the columns to write out
            out_fn = os.path.join('/home/james/Desktop', ''.join([sheet.replace('_TS', '').lower(), '.csv']))
            with open(out_fn, 'w', newline='') as outpf:
                # reindex is used so that we can change the order by which columns are written out as
                # columns indicates which columns are to output and their order
                # index=False means don`t include the df index column in the output
                df.reindex(columns=columns_to_output).to_csv(outpf, columns=columns_to_output, index=False)


if __name__ == "__main__":
    convert_data(
        xlsx_fn='/home/james/Desktop/Work/FGreen/Supplied_Data_210521/basic data.xlsx',
        limit_output_columns=True
    )
