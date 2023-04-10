import sqlite3
import polars as pl

def polars_main():
    pass

def json_input_reader():
    pass

def sqlite_input_reader():
    pass

def csv_input_reader():
    df = pl.read_csv('data.csv')
    return df

if __name__ == "__main__":
    polars_main()