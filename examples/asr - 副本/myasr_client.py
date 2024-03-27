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
logger.setLevel(logging.DEBUG)
# Create a stream handler
handler = logging.StreamHandler()
# Set the level of the handler
handler.setLevel(logging.DEBUG)
# Add the handler to the logger
logger.addHandler(handler)

class MyASRClient:
    def __init__(self, region):
        # Configure your Tencent Cloud API credentials
        logger.debug("Initializing MyASRClient")
        self.cred = credential.EnvironmentVariableCredential().get_credential()
        logger.debug("Creating ASR client,cred: %s",self.cred)
        self.region = region
        self.client = asr_client.AsrClient(self.cred, self.region)
        self.create_rec_task_req = models.CreateRecTaskRequest()
        self.query_rec_task_req = models.DescribeTaskStatusRequest()
        params = {"ChannelNum": 1, "ResTextFormat": 2, "SourceType": 0, "ConvertNumMode": 1}
        self.create_rec_task_req._deserialize(params)
        self.create_rec_task_req.EngineModelType = '16k_zh_large'

    def create_rec_task(self, file_url):
        logger.debug("Creating rec task")
        self.create_rec_task_req.Url = file_url
        try:
            resp = self.client.CreateRecTask(self.create_rec_task_req)
            logger.info(resp)
            request_id = resp.RequestId
            task_id = resp.Data.TaskId
            return request_id, task_id
        except Exception as err:
            logger.info(traceback.format_exc())
            return None, None

    def query_rec_task(self, task_id):
        logger.debug("Initializing query rec task request")
        params = '{"TaskId":' + str(task_id) + '}'
        self.query_rec_task_req.from_json_string(params)
        result = ""
        while True:
            try:
                resp = self.client.DescribeTaskStatus(self.query_rec_task_req)
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
