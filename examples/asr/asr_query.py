import json
import logging
import sqlite3
import time
import myasr_client
import asr_taskmgr


def query_asr_result(task_id):
    # Create an instance of the ASR client
    client = myasr_client.MyASRClient("ap-guangzhou")

    # Query the ASR result using the task ID
    success, result = client.query_rec_task(task_id)

    # Print the result
    if success:
        print(result)


def get_result(key, task_id):
    # Initialize the logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    
    # open the database
    db_file = "aaa.db"
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
    logger.debug("Get result from ASR begin!")
    my_describe_task = myasr_client.MyASRClient("ap-guangzhou")
    
    logger.debug(f"Getting result for ,{task_id}")
    success, result = my_describe_task.query_rec_task(task_id)
    if success:
        # Convert the list to a JSON string
        result_str = json.dumps(result)
        # Update the status in the database
        cursor.execute("UPDATE mp3Item SET status = ?, asr_data = ? WHERE id = ?", ("GetResult", result_str, key))
        conn.commit()
        logger.debug(f"Got  result for {key}:{result}:{result_str}")
    else:
        logger.error(f"Failed to get result for {key}")
        
    conn.close()
    logging.debug("Get result from ASR end!")

def get_result_bydb():
    # Initialize the logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    
    # open 2 databases
    db_file1 = "aaa.db"
    conn1 = sqlite3.connect(db_file1)
    cursor1 = conn1.cursor()
    db_file2 = "bbb.db"
    conn2 = sqlite3.connect(db_file2)
    cursor2 = conn2.cursor()
    # Create tables and indexes if they don't exist
    cursor2.execute('''
        CREATE TABLE IF NOT EXISTS mp3Item (
            id TEXT PRIMARY KEY,
            status TEXT,
            request_id TEXT,
            task_id TEXT,
            asr_data TEXT
        )
    ''')
    cursor2.execute('''
        CREATE INDEX IF NOT EXISTS idx_mp3Item_id ON mp3Item (id)
    ''')
    conn2.commit()


    logger.debug("Get result (transfer db)from ASR begin!")
    my_describe_task = myasr_client.MyASRClient("ap-guangzhou")
 
    # Get all the data from the database table
    cursor1.execute("SELECT * FROM mp3Item")
    data = cursor1.fetchall()
    i = 0;
    j = 0;
    for row in data:
        i = i + 1
        j = j + 1
        if(i > 49):
            time.sleep(1)
            i = 0
        key, status, requst_id, task_id, _ = row
        logger.debug(f"Getting result for ,{task_id}")
        success, result = my_describe_task.query_rec_task(task_id)
        #print(result)
        result_str = json.dumps(result, ensure_ascii=False)
        #print(result_str)
        if success:
            cursor2.execute("INSERT INTO mp3Item (id, status, request_id, task_id, asr_data) VALUES (?, ?, ?, ?, ?)", (key,status,requst_id,task_id,result_str))
            conn2.commit()
            # Convert the list to a JSON string
            #result_str = json.dumps(result)
            # Update the status in the database
            #cursor2.execute("UPDATE mp3Item SET id = ?, status = ?, asr_data = ? WHERE id = ?", ("GetResult", result_str, key))
            #conn.commit()
            logger.debug(f"No. {j} Got  result for {key}:")
        else:
            logger.error(f"No. {j} Failed to get result for {key}")
        
    conn1.close()
    logging.debug("Get result (transfer db) from ASR end!")

if __name__ == "__main__":
    # Replace 'task_id' with the actual task ID you want to query
    # task_id = '8645928744'
    # id = '3xb42w8c8uj9fmq'
    # query_asr_result(task_id)
    # get_result(id,task_id)

    get_result_bydb()