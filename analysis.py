import sqlite3
import os
import pandas as pd

def analysis_main():
    """benchmark analysis of different times
    """
    pass

def meta_data_from_pandas(df):
    """read meta data"""
    meta_data_dict = {}
    meta_data_dict["number_of_rows"] = df.shape[0]
    meta_data_dict["number_of_columns"] = df.shape[1]
    return meta_data_dict

def meta_data_from_polars():
    pass

def create_db_if_not_exist():
    # Connect to the database (create it if it doesn't exist)
    parent_dir = os.path.abspath(os.path.join(os.getcwd(), os.pardir))

    conn = sqlite3.connect(os.path.join(parent_dir, 'analysis.db'))
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS benchmark
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT,
                read_time REAL,
                process_time REAL,
                save_time REAL,
                full_time REAL,
                input_type TEXT,
                mid_type TEXT,
                output_type TEXT,
                number_of_rows INTEGER,
                number_of_columns INTEGER)''')
    conn.commit()
    conn.close()
    return parent_dir


def insert_data_in_db(data, parent_dir):
    conn = sqlite3.connect(os.path.join(parent_dir, 'analysis.db'))
    c = conn.cursor()
    query = '''INSERT INTO benchmark
           (filename, read_time, process_time, save_time,
           full_time, input_type, mid_type, output_type, number_of_rows, number_of_columns)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
    c.execute(query, (data['filename'], data['read_time'],\
                    data['process_time'], data['save_time'], data['full_time'],\
                    data['input_type'], data['mid_type'], data['output_type'],\
                    data['number_of_rows'], data['number_of_columns']))
    conn.commit()
    conn.close()

def save_data_in_db(meta_data_dict, filename):
    parent_dir = create_db_if_not_exist()
    meta_data_dict["filename"] = filename
    insert_data_in_db(meta_data_dict, parent_dir)


if __name__ == "__main__":
    analysis_main()

