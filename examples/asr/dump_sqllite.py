import sqlite3
import pandas as pd
import ast

# Connect to the SQLite database
conn = sqlite3.connect('3xt2waf6hw86e22.db')

# Query the data from the table
query = "SELECT id, status, request_id, task_id, asr_data FROM mp3Item"
df = pd.read_sql_query(query, conn)

# Define a function to safely apply ast.literal_eval
def safe_literal_eval(val):
    try:
        return ast.literal_eval(val)
    except ValueError:
        return val

# Convert the string representation of list to list
df['asr_data'] = df['asr_data'].apply(safe_literal_eval)

# Convert the Unicode escape sequences to normal strings for each element in the list
df['asr_data'] = df['asr_data'].apply(lambda x: [i.encode('utf-8').decode('unicode_escape') for i in x] if isinstance(x, list) and all(isinstance(i, str) for i in x) else x)

# Export the data to an Excel file
df.to_excel('3xt2waf6hw86e22.xlsx', index=False)

# Close the database connection
conn.close()