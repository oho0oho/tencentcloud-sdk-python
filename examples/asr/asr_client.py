import logging
import json
import time
import traceback
import tencentcloud
from tencentcloud.common import credential
from tencentcloud.asr.v20190614 import asr_client, models
from tencentcloud.common.exception import TencentCloudSDKException

# Initialize the logger
logger = logging.getLogger(__name__)

# Configure your Tencent Cloud API credentials
cred = credential.ProfileCredential().get_credential()
# Configure your Tencent Cloud API credentials
region = "YOUR_REGION"

def create_rec(engine_type, file_url):
    client = asr_client.AsrClient(cred, region)
    req = models.CreateRecTaskRequest()
    params = {"ChannelNum": 1, "ResTextFormat": 2, "SourceType": 0, "ConvertNumMode": 1}
    req._deserialize(params)
    req.EngineModelType = engine_type
    req.Url = file_url
    try:
        resp = client.CreateRecTask(req)
        logger.info(resp)
        requesid = resp.RequestId
        taskid = resp.Data.TaskId
        return requesid, taskid
    except Exception as err:
        logger.info(traceback.format_exc())
        return None, None

import subprocess
def upload_file(tmpAudio):
    objectName = tmpAudio.split('/')[-1]
    ret = subprocess.run(['coscmd', '-s', 'upload', tmpAudio, objectName], shell=False)
    if ret.returncode != 0:
        print("error:", ret)

def query_rec_task(taskid):
    client = asr_client.AsrClient(cred, region)
    req = models.DescribeTaskStatusRequest()
    params = '{"TaskId":' + str(taskid) + '}'
    req.from_json_string(params)
    result = ""
    while True:
        try:
            resp = client.DescribeTaskStatus(req)
            resp_json = resp.to_json_string()
            logger.info(resp_json)
            resp_obj = json.loads(resp_json)
            if resp_obj["Data"]["StatusStr"] == "success":
                result = resp_obj["Data"]["ResultDetail"]
                break
            if resp_obj["Data"]["Status"] == 3:
                return False, ""
            time.sleep(1)
        except TencentCloudSDKException as err:
            logger.info(err)
            return False, ""
    return True, result