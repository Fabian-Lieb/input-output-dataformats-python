import sqlite3
import pandas as pd


def pandas_main():
    pass

def json_input_reader(filename):
    df = pd.read_json(f"input/{filename}")
    return df

def sqlite_input_reader(filename):
    conn = sqlite3.connect(f"input/{filename}")
    df = pd.read_sql('SELECT * FROM tablename', conn)
    conn.close()
    return df

def csv_input_reader(filename):
    df = pd.read_csv(f"input/{filename}")
    return df

if __name__ == "__main__":
    pandas_main()
