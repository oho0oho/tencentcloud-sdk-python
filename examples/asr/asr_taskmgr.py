import sqlite3
import os
import multiprocessing
import tencentcloud


# Specify the name of the database file
db_file = "mydatabase.db"

# Connect to the database or create a new one if it doesn't exist
conn = sqlite3.connect(db_file)

# Create a cursor object to execute SQL statements
cursor = conn.cursor()

# Create tables and indexes if they don't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS mp3Item (
        id TEXT PRIMARY KEY,
        name TEXT,
        status TEXT,
        request_id TEXT,
        task_id TEXT,
        WENAN TEXT
    )
''')

cursor.execute('''
    CREATE INDEX IF NOT EXISTS idx_mp3Item_name ON mp3Item (name)
''')

# Commit the changes
conn.commit()

# Specify the directory to check
directory = "/path/to/directory"

# Get all the file names (without extensions) in the directory
file_names = [os.path.splitext(file)[0] for file in os.listdir(directory)]

# Check if each file name exists in the database table
for file_name in file_names:
    cursor.execute("SELECT COUNT(*) FROM mp3Item WHERE name = ?", (file_name,))
    result = cursor.fetchone()[0]
    
    # If the file name doesn't exist, initialize a new record and insert it into the database
    if result == 0:
        cursor.execute("INSERT INTO mp3Item (name, status) VALUES (?, ?)", (file_name, "OnDisk"))
        conn.commit()



# Define a function to process the data from a queue
def process_data(queue):
    while True:
        data = queue.get()
        if data is None:
            break
        # Process the data here

# Create 4 queues for each status
queue_OnDisk = multiprocessing.Queue()  # OnDisk
queue_OnCos = multiprocessing.Queue()  # OnCos
queue_AsrIng = multiprocessing.Queue()  # AsrIng
queue_GetResult = multiprocessing.Queue()  # GetResult

# Get all the data from the database table
cursor.execute("SELECT * FROM mp3Item")
data = cursor.fetchall()

# Insert each data into the corresponding queue based on its status
for row in data:
    _, _, status, _, task_id, key = row
    if status == "OnDisk":
        queue_OnDisk.put(key)
    elif status == "OnCos":
        queue_OnCos.put(key)
    elif status == "AsrIng":
        queue_AsrIng.put(key)
    elif status == "GetResult":
        queue_GetResult.put(key)
    else:
        logging.error(f"Invalid status: {status}")

# Define a function to upload mp3 to COS
def upload_mp3(queue_OnDisk, queue_OnCos):
    while True:
        key = queue_OnDisk.get()
        if key is None:
            break
        # Upload the mp3 file to COS
        upload_file(key)
        # Update the status in the database
        cursor.execute("UPDATE mp3Item SET status = ? WHERE name = ?", ("OnCos", key))
        conn.commit()
    conn.close()

# Define a function to send request to ASR
def send_req(queue_OnCos, queue_AsrIng):
    while True:
        key = queue_OnCos.get()
        if key is None:
            break
        # Send request to ASR
        request_id, task_id = create_rec("16k_zh", key)
        if request_id is not None:
            # Update the status in the database
            cursor.execute("UPDATE mp3Item SET status = ?, request_id = ?, task_id = ? WHERE name = ?", ("AsrIng", request_id, task_id, key))
            conn.commit()
            queue_AsrIng.put(key)
        else:
            logging.error(f"Failed to create rec task for {key}")
    conn.close()


# Define a function to get result from ASR
def get_result(queue_AsrIng, queue_GetResult):
    while True:
        key = queue_AsrIng.get()
        if key is None:
            break
        # Get result from ASR
        success, result = query_rec_task(task_id)
        if success:
            # Update the status in the database
            cursor.execute("UPDATE mp3Item SET status = ?, WENAN = ? WHERE name = ?", ("GetResult", result, key))
            conn.commit()
            queue_GetResult.put(key)
        else:
            logging.error(f"Failed to get result for {key}")
    conn.close()


# Create 3 processes to process the data from each queue
process1 = multiprocessing.Process(target=upload_mp3, args=(queue_OnDisk,queue_OnCos))
process2 = multiprocessing.Process(target=send_req, args=(queue_OnCos,queue_AsrIng))
process3 = multiprocessing.Process(target=get_result, args=(queue_AsrIng,queue_GetResult))


# Start the processes
process1.start()
process2.start()
process3.start()


# Wait for all the queues to be empty
while not (queue_OnDisk.empty() and queue_OnCos.empty() and queue_AsrIng.empty() and queue_GetResult.empty() ):
    pass

# Put None into each queue to signal the processes to exit
queue_OnDisk.put(None)
queue_OnCos.put(None)
queue_AsrIng.put(None)
queue_GetResult.put(None)

# Wait for all the processes to finish
process1.join()
process2.join()
process3.join()


# Close the connection
conn.close()