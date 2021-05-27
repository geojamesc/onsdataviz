import os
import pandas as pd


def format_cell(in_cell_val):
    """
    takes an input value e.g. 0.098888 and converts to a % with 1 digit to right of decimal point
    :param in_cell_val:
    :return:
    """
    return "{:.1f}".format((in_cell_val * 100))


def convert_data(xlsx_fn):

    # dict of columns in each worksheet we want to format the values of using our format_cell func
    sheet_names_w_pcnt_col_indexes = {
        'TradingStatus_TS': [4, 5, 6, 7, 8, 10, 11, 12],
        'FinancialPerformance_TS': [4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15],
        'WorkforceStatus_TS': [3, 4, 5, 6, 7, 8, 11],
        'CashFlow_TS': [4, 5, 6, 7, 8, 9, 11, 12]
    }

    viz_priority_columns = {
        'TradingStatus_TS': [8, 11, 12],
        'FinancialPerformance_TS': [7, 14, 15],
        'WorkforceStatus_TS': [3, 6, 7],  # TODO note has no number column for pd index used below
        'CashFlow_TS': [12]
    }

    if os.path.exists(xlsx_fn):
        for sheet in sheet_names_w_pcnt_col_indexes:
            # build up a dict mapping column index to conversion function
            # that can be supplied as the convertors param in the pandas
            # read_excel call to run conversion on the cell values
            the_convertors = {}
            columns_to_be_formatted = sheet_names_w_pcnt_col_indexes[sheet]
            for col in columns_to_be_formatted:
                # i.e. the_convertors[4] = format_cell
                the_convertors[col] = format_cell

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
                index_col=pd_index_col,
                na_values='*',  # NULL values
                converters=the_convertors  # convert specified cols as per our defined above the_convertors dict
            )

            # write the dataframe out as a CSV file
            out_fn = os.path.join('/home/james/Desktop' , ''.join([sheet.replace('_TS', '').lower(), '.csv']))
            with open(out_fn, 'w', newline='') as outpf:
                df.to_csv(outpf)


if __name__ == "__main__":
    convert_data(xlsx_fn='/home/james/Desktop/Work/FGreen/Supplied_Data_210521/basic data.xlsx')
