import sqlite3
import os
import multiprocessing
import logging
import tencentcloud
import myasr_client

# Define a function to upload a file to COS
def upload_file(file_name):
    # Upload the file to COS
    pass
    
# from key to url
def key2url(key):
    return f"https://kuai-video-1255989664.cos.ap-guangzhou.myqcloud.com/3xt2waf6hw86e22/www_video_mp3/{key}.mp3"

# Define a function to upload a file to COS
def upload_file(file_name):
    # Upload the file to COS
    


# Define a function to upload mp3 to COS
def upload_mp3(queue_OnDisk, queue_OnCos):
    # Initialize the logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    # open the database
    db_file = "3xt2waf6hw86e22.db"
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    logger.debug("Uploading mp3 to COS")
    while True:
        key = queue_OnDisk.get()
        if key is None:
            break
        # Upload the mp3 file to COS
        upload_file(key)
        # Update the status in the database
        cursor.execute("UPDATE mp3Item SET status = ? WHERE id = ?", ("OnCos", key))
        conn.commit()
    conn.close()


# Define a function to send request to ASR
def send_req(queue_OnCos, queue_AsrIng):
    # Initialize the logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    # open the database
    db_file = "3xt2waf6hw86e22.db"
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    logger.debug("Sending request to ASR")
    my_create_rec = myasr_client.MyASRClient("ap-guangzhou")
    while True:
        key = queue_OnCos.get()
        if key is None:
            break
        # Send request to ASR
        request_id, task_id = my_create_rec.create_rec_task(key2url(key))
        if request_id is not None:
            # Update the status in the database
            cursor.execute("UPDATE mp3Item SET status = ?, request_id = ?, task_id = ? WHERE id = ?", ("AsrIng", request_id, task_id, key))
            conn.commit()
            queue_AsrIng.put(key,task_id)
        else:
            logging.error(f"Failed to create rec task for {key}")
    conn.close()


# Define a function to get result from ASR
def get_result(queue_AsrIng, queue_GetResult):
    # Initialize the logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    # open the database
    db_file = "3xt2waf6hw86e22.db"
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    logger.debug("Getting result from ASR")
    my_describe_task = myasr_client.MyASRClient("ap-guangzhou")
    while True:
        if not queue_AsrIng.empty():
            data = queue_AsrIng.get()
            if data is not None:
                key, task_id = data
                # Rest of your code
            else:
                break
        else:
            break
        # Get result from ASR
        success, result = my_describe_task.query_rec_task(task_id)
        if success:
            # Update the status in the database
            cursor.execute("UPDATE mp3Item SET status = ?, asr_data = ? WHERE id = ?", ("GetResult", result, key))
            conn.commit()
            queue_GetResult.put(key)
        else:
            logging.error(f"Failed to get result for {key}")
    conn.close()




if __name__ == '__main__':
    # Initialize the logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    # open the database
    db_file = "3xt2waf6hw86e22.db"
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    # Create tables and indexes if they don't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS mp3Item (
            id TEXT PRIMARY KEY,
            status TEXT,
            request_id TEXT,
            task_id TEXT,
            asr_data TEXT
        )
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_mp3Item_id ON mp3Item (id)
    ''')
    conn.commit()


    # Specify the directory to check
    # directory = "D:\test\3xt2waf6hw86e22\www_video_mp3"
    directory = "D:\\test\\www_video_mp3"

    # Get all the file names (without extensions) in the directory
    file_names = [os.path.splitext(file)[0] for file in os.listdir(directory)]

    # Check if each file name exists in the database table and insert into table 
    for file_name in file_names:
        cursor.execute("SELECT COUNT(*) FROM mp3Item WHERE id = ?", (file_name,))
        result = cursor.fetchone()[0]
        # If the file name doesn't exist, initialize a new record and insert it into the database
        if result == 0:
            cursor.execute("INSERT INTO mp3Item (id, status) VALUES (?, ?)", (file_name, "OnDisk"))
            conn.commit()


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
        key, status, _, task_id, _ = row
        if status == "OnDisk":
            queue_OnDisk.put(key)
        elif status == "OnCos":
            queue_OnCos.put(key)
        elif status == "AsrIng":
            queue_AsrIng.put((key, task_id))
        elif status == "GetResult":
            queue_GetResult.put(key)
        else:
            logging.error(f"Invalid status: {status}")



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