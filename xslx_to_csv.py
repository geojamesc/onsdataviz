import csv
import os
import pandas as pd
import datetime


def create_start_date_from_data_col(in_date_str):
    """
    s_date = create_start_date_from_data_col('19 April to 2 May 2021')
    --> 19-04-2021

    convert the val

    :param in_date_str: a str like 19 April to 2 May 2021
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


def convert_data(xlsx_fn, out_path, limit_output_columns=False):
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

    # cleanup the headers written to output csvs
    out_headers = {
        'TradingStatus_TS': ['wave', 'date', 'wave_start_date', 'industry_band', 'ts_ceased_trading', 'ts_current_and_started_trading', 'ts_paused_trading'],
        'FinancialPerformance_TS': ['wave', 'date', 'wave_start_date', 'industry_band', 'fp_turnover_not_affected', 'fp_lower_turnover', 'fp_higher_turnover'],
        'WorkforceStatus_TS': ['wave', 'date', 'wave_start_date', 'industry_band', 'ws_on_furlough', 'ws_working_normal_place_of_work', 'ws_wfh'],
        'CashFlow_TS': ['wave', 'date', 'wave_start_date', 'industry_band', 'cf_lt_3mths']
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
            out_fn = os.path.join(out_path, ''.join([sheet.replace('_TS', '').lower(), '.csv']))

            out_header = True
            if sheet in out_headers:
                out_header = out_headers[sheet]

            with open(out_fn, 'w', newline='') as outpf:
                # reindex is used so that we can change the order by which columns are written out as
                # columns indicates which columns are to output and their order
                # index=False means don`t include the df index column in the output
                df.reindex(columns=columns_to_output).to_csv(outpf, columns=columns_to_output, index=False, header=out_header)


def write_merged_csv(out_path):
    """
    take the 4 csv`s produced by convert_data and merges into a single csv containing
    wave, industry_band and the columns from the 4 csv`s

    NOTE: (start) date of waves in FinancialPerformance is inconsistent with other data
    according to FG`s notes this is because the question refers to previous week
    assumption is that the data can from different metrics can still be grouped by wave
    even though in the case of FinancialPerformance this is 1 week difft
    """
    indv_csvs = {
        'cf': os.path.join(out_path, 'cashflow.csv'),
        'fp': os.path.join(out_path, 'financialperformance.csv'),
        'ts': os.path.join(out_path, 'tradingstatus.csv'),
        'ws': os.path.join(out_path, 'workforcestatus.csv')
    }

    merged_records = {}

    for metric in indv_csvs:
        pth_to_csv = indv_csvs[metric]
        with open(pth_to_csv, 'r') as inpf:
            my_reader = csv.DictReader(inpf)

            for r in my_reader:
                wave = r['wave']
                industry_band = r['industry_band']
                k = '__'.join([wave, industry_band])
                if metric == 'cf':
                    cf_lt_3mths = r['cf_lt_3mths']
                    data_for_metric = {
                        'cf_lt_3mths':  cf_lt_3mths
                    }

                elif metric == 'fp':
                    fp_turnover_not_affected = r['fp_turnover_not_affected']
                    fp_lower_turnover = r['fp_lower_turnover']
                    fp_higher_turnover = r['fp_higher_turnover']
                    data_for_metric = {
                        'fp_turnover_not_affected': fp_turnover_not_affected,
                        'fp_lower_turnover': fp_lower_turnover,
                        'fp_higher_turnover': fp_higher_turnover
                    }
                elif metric == 'ts':
                    ts_ceased_trading = r['ts_ceased_trading']
                    ts_current_and_started_trading = r['ts_current_and_started_trading']
                    ts_paused_trading = r['ts_paused_trading']
                    data_for_metric = {
                        'ts_ceased_trading': ts_ceased_trading,
                        'ts_current_and_started_trading': ts_current_and_started_trading,
                        'ts_paused_trading': ts_paused_trading
                    }
                elif metric == 'ws':
                    ws_on_furlough = r['ws_on_furlough']
                    ws_working_normal_place_of_work = r['ws_working_normal_place_of_work']
                    ws_wfh = r['ws_wfh']
                    data_for_metric = {
                        'ws_on_furlough': ws_on_furlough,
                        'ws_working_normal_place_of_work': ws_working_normal_place_of_work,
                        'ws_wfh': ws_wfh
                    }

                if k in merged_records:
                    merged_records[k][metric] = data_for_metric
                else:
                    merged_record = {'cf': None, 'fp': None, 'ts': None, 'ws': None}
                    merged_record[metric] = data_for_metric
                    merged_records[k] = merged_record

    out_records = []

    for k in merged_records:
        [wave, industry_band] = k.split('__')
        cf_lt_3mths = merged_records[k]['cf']['cf_lt_3mths']
        fp_higher_turnover = merged_records[k]['fp']['fp_higher_turnover']
        fp_lower_turnover = merged_records[k]['fp']['fp_lower_turnover']
        fp_turnover_not_affected = merged_records[k]['fp']['fp_turnover_not_affected']
        ts_ceased_trading = merged_records[k]['ts']['ts_ceased_trading']
        ts_current_and_started_trading = merged_records[k]['ts']['ts_current_and_started_trading']
        ts_paused_trading = merged_records[k]['ts']['ts_paused_trading']
        ws_on_furlough = merged_records[k]['ws']['ws_on_furlough']
        ws_wfh = merged_records[k]['ws']['ws_wfh']
        ws_working_normal_place_of_work = merged_records[k]['ws']['ws_working_normal_place_of_work']

        out_record = [
            wave,
            industry_band,
            ts_current_and_started_trading,
            ts_paused_trading,
            ts_ceased_trading,
            cf_lt_3mths,
            fp_lower_turnover,
            fp_turnover_not_affected,
            fp_higher_turnover,
            ws_working_normal_place_of_work,
            ws_wfh,
            ws_on_furlough
        ]

        out_records.append(out_record)

        with open(os.path.join(out_path, 'merged_records_w_all_metrics.csv'), 'w') as outpf:
            my_writer = csv.writer(outpf, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            header = ['wave', 'industry_band', 'ts_current_and_started_trading', 'ts_paused_trading', 'ts_ceased_trading',
                      'cf_lt_3mths', 'fp_lower_turnover', 'fp_turnover_not_affected', 'fp_higher_turnover',
                      'ws_working_normal_place_of_work', 'ws_wfh', 'ws_on_furlough']
            my_writer.writerow(header)
            my_writer.writerows(out_records)


def filter_empty_industry_bands(out_path):
    """
    filter off all records where industry_band is one of the following

    Counts of waves per industry_band where all metrics are null/zero
    Industry: Education : 19
    Industry: Health and social work : 17
    Industry: Mining and quarrying : 15
    Industry: Other service activities : 23
    Industry: Real estate activities : 23
    Industry: Water supply, sewerage, waste : 23

    i.e. there are 23 waves in total, for Industry: Education, in 19 of the waves all of the 10 metrics are
    null or equal to zero so we can`t do anything across the entire dataset in the first instance

    :param out_path:
    :return:
    """
    if os.path.exists(os.path.join(out_path, 'merged_records_w_all_metrics.csv')):
        count_of_empty_records_per_industry_band = {}

        with open(os.path.join(out_path, 'merged_records_w_all_metrics.csv'), 'r') as inpf:
            my_reader = csv.DictReader(inpf)
            for r in my_reader:
                industry_band = r['industry_band']
                null_cell_count = 0
                zero_cell_count = 0
                for k in r:
                    if k not in ('wave', 'industry_band'):
                        # TODO actually we only care about null values
                        if r[k] == '':
                            null_cell_count += 1
                        if r[k] == '0.0':
                            zero_cell_count += 1

                # TODO but actually is it not only nulls that are problematic?
                # there are 10 metrics, so have all 10 are null or zero we can`t
                # do anything with the data
                if (null_cell_count + zero_cell_count) == 10:
                    if industry_band not in count_of_empty_records_per_industry_band:
                        count_of_empty_records_per_industry_band[industry_band] = 1
                    else:
                        count_of_empty_records_per_industry_band[industry_band] += 1

        if len(count_of_empty_records_per_industry_band.keys()) > 1:
            print('Counts of waves per industry_band where all metrics are null/zero')
            print('Re-writing merged_records_w_all_metrics.csv as merged_records_w_all_metrics_filtered.csv without these')
            for k in sorted(count_of_empty_records_per_industry_band.keys()):
                print('\t{} : {}'.format(k, count_of_empty_records_per_industry_band[k]))
            print('\n')

            # then we read in merged_records_w_all_metrics.csv and create a new output csv
            # merged_records_w_all_metrics_filtered.csv in which we have excluded industry_bands
            # where in a significant number of waves the 10 columns were all null or zero
            with open(os.path.join(out_path, 'merged_records_w_all_metrics.csv'), 'r') as inpf:
                my_reader = csv.reader(inpf)
                with open(os.path.join(out_path, 'merged_records_w_all_metrics_filtered.csv'), 'w') as outpf:
                    my_writer = csv.writer(outpf, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
                    for r in my_reader:
                        industry_band = r[1]
                        if industry_band not in count_of_empty_records_per_industry_band:
                            my_writer.writerow(r)


def validate_filtered_metrics(out_path):
    """
    for each of the 10 metrics obtain count of the number of records
    where the metric value is null or equal to zero

    ts_current_and_started_trading 0
    ts_paused_trading 87
    ts_ceased_trading 200
    cf_lt_3mths 0
    fp_lower_turnover 15
    fp_turnover_not_affected 18
    fp_higher_turnover 26
    ws_working_normal_place_of_work 0
    ws_wfh 0
    ws_on_furlough 0

    i.e. for ws_wfh metric all records have a value other than 0
    whereas for ts_paused_trading 87 records have a value which is 0 or null

    :param out_path:
    :return:
    """
    metric_values_count_null_or_zero = {
        'ts_current_and_started_trading': 0,
        'ts_paused_trading': 0,
        'ts_ceased_trading': 0,
        'cf_lt_3mths': 0,
        'fp_lower_turnover': 0,
        'fp_turnover_not_affected': 0,
        'fp_higher_turnover': 0,
        'ws_working_normal_place_of_work': 0,
        'ws_wfh': 0,
        'ws_on_furlough': 0
    }

    with open(os.path.join(out_path, 'merged_records_w_all_metrics_filtered.csv'), 'r') as inpf:
        my_reader = csv.DictReader(inpf)
        metrics = metric_values_count_null_or_zero.keys()
        for r in my_reader:
            for metric in metrics:
                metric_val = r[metric]

                # TODO actually we only care about NULL values
                if metric_val in ('', '0.0'):
                    metric_values_count_null_or_zero[metric] += 1

    print('Results of validating metrics.')
    print('Counts of records per metric where metric value is null or equal to zero.')
    print('0 = for the metric means all records have a value')
    print('value > 0 for the metric means some records don`t have a value or value is equal to zero')
    for metric in metric_values_count_null_or_zero:
        print('\t', metric, metric_values_count_null_or_zero[metric])


def transform_data(src_fn, out_path):
    # [1] first we dump out to csv each of the 4 sheets from the xlsx
    convert_data(
        xlsx_fn=src_fn,
        out_path=out_path,
        limit_output_columns=True
    )

    # [2] then we merge these 4 csv`s into a single csv
    write_merged_csv(out_path)

    # TODO think on NULL vs zero

    # [3] then we filter this single csv to remove records where for industry_band a significant number of the
    # waves are all null or zero
    filter_empty_industry_bands(out_path)

    # [4] then we validate metrics
    validate_filtered_metrics(out_path)


if __name__ == "__main__":
    transform_data(
        src_fn='/home/james/Desktop/Work/FGreen/Supplied_Data_210521/basic data.xlsx',
        out_path='/home/james/Desktop'
    )