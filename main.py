import argparse
import os
import time

from pandas_read import (json_input_reader as json_pandas,
        csv_input_reader as csv_pandas,
        sqlite_input_reader as sqlite_pandas)

from polars_read import (json_input_reader as json_polars,
        csv_input_reader as csv_polars,
        sqlite_input_reader as sqlite_polars)

from analysis import (save_data_in_db, meta_data_from_pandas,
        meta_data_from_polars)


def main():
    print('start')
    """parse options for different
    dataformat conversions
    """
    parser = argparse.ArgumentParser(description="Description of your program")
    parser.add_argument("--input_type", "-i", choices=["csv", "json", "sqlite", "automatic"], \
                        default="csv", help="Input file format (csv or json)")
    parser.add_argument("--mid_type", "-m", choices=["pandas", "polars"], \
                        default="pandas", help="Midstruct (pandas df or polars df)")
    parser.add_argument("--output_type", "-o", choices=["csv", "json", "sqlite", "None"], \
                        default="csv", help="Output file format (csv or json)")
    args = parser.parse_args()
    folder_path = "input"
    for filename in os.listdir(folder_path):
        _df, meta_data_dict = mid_process(args ,filename)
        save_data_in_db(meta_data_dict, filename)


def append_meta_data_dict(meta_data_dict, args, elapsed_time_input, elapsed_time_process,\
                         elapsed_time_output, elapsed_time_full):
    """ appending meta data
    """
    meta_data_dict["read_time"] = elapsed_time_input
    meta_data_dict["process_time"] = elapsed_time_process
    meta_data_dict["save_time"] = elapsed_time_output
    meta_data_dict["full_time"] = elapsed_time_full
    meta_data_dict["input_type"] = args.input_type
    meta_data_dict["mid_type"] =  args.mid_type
    meta_data_dict["output_type"] = args.output_type
    return meta_data_dict


def input_process_with_pandas(args, filename):
    """input matching
    """
    match args.input_type:
        case "csv":
            if filename.endswith(".csv"):
                df = csv_pandas(filename)
                print("Processing CSV input -> pandas")
        case "json":
            if filename.endswith(".json"):
                df = json_pandas(filename)
                print("Processing JSON input -> pandas")
        case "sqlite":
            if filename.endswith(".db"):
                df = sqlite_pandas(filename)
                print("Processing SQLite input -> pandas")
        case _:
            # handle unknown input types
            print(f"Unknown input type: {args.input_type} - selcte a supported type")
    return df

def mid_process(args, filename):
    """
    meta data analysis
    for pandas or polars
    """
    match args.mid_type:
        case "pandas":
            st_input = time.time()
            df = input_process_with_pandas(args, filename)
            et_input = time.time()
            elapsed_time_input = (et_input- st_input) * 1000
            st_process = time.time()
            # here is some additional processing for benchmarking...
            et_process = time.time()
            elapsed_time_process = (et_process- st_process) * 100
            st_output = time.time()
            output_process_with_pandas(args, df)
            et_output = time.time()
            elapsed_time_output = (et_output- st_output) * 1000
            elapsed_time_full = (et_output- st_input) * 1000
            meta_data_dict = meta_data_from_pandas(df)
            meta_data_dict = append_meta_data_dict(meta_data_dict, args, elapsed_time_input, elapsed_time_process,\
                         elapsed_time_output, elapsed_time_full)
        case "polars":
            st_input = time.time()
            df = input_process_with_polars(args, filename)
            et_input = time.time()
            elapsed_time_input = (et_input- st_input) * 1000
            st_process = time.time()
            # here is some additional processing for benchmarking...
            et_process = time.time()
            elapsed_time_process = (et_process- st_process) * 100
            st_output = time.time()
            output_process_with_polars(args, df)
            et_output = time.time()
            elapsed_time_output = (et_output- st_output) * 1000
            elapsed_time_full = (et_output- st_input) * 1000
            meta_data_dict = meta_data_from_polars(df)
            meta_data_dict = append_meta_data_dict(meta_data_dict, args, elapsed_time_input, elapsed_time_process,\
                         elapsed_time_output, elapsed_time_full)
        case _ :
            print("not supported mid type")
    return df, meta_data_dict


def output_process_with_pandas(args, df):
    """output matching
    """
    match args.output_type:
        case "csv":
            print("Convert pandas df to CSV")
        case "json":
            print("Convert pandas df to JSON")
        case "sqlite":
            print("Convert pandas df to SQLite")
        case "None":
            print("No output type")
        case _:
            # handle unknown input types
            print(f"Unknown output type: {args.output_type} - selcte a supported type")



if __name__ == "__main__":
    main()