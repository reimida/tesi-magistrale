import os,io,cv2,sys,time,json,boto3,timeit,random,logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from amazon_kinesis_video_consumer_library.kinesis_video_fragment_processor import KvsFragementProcessor
from amazon_kinesis_video_consumer_library.ebmlite import loadSchema

# Set up basic logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

REGION='eu-west-1'
#KVS_STREAM01_NAME = 'kvs_camera_stream'   # Stream must be in specified region
DATA_STREAM_NAME = "ppe-datastream"

s3 = boto3.client("s3", region_name=REGION)
kds_client=boto3.client('kinesis')
kvs_fragment_processor = KvsFragementProcessor()
session = boto3.Session(region_name=REGION)
kvs_client = session.client("kinesisvideo")

schema = loadSchema('matroska.xml')
reko_client=boto3.client('rekognition', region_name='eu-west-1')
min_confidence=70


#ideally, max_threads = fragmentframes/frames_ratio
max_threads=5
executor = ThreadPoolExecutor(max_threads)

def lambda_handler(event, context):
    futures = []

    try:
        logger.info('Lambda function started.')
        logger.debug(f'Event received: {event}')
        
        ######## GET IOT EVENT ##############
        print(event)
        msg_payload = event
        
        if "Stream" not in msg_payload["Camera"]:
            logger.error('Missing or invalid Stream in the input payload.')
            return
            
        ##########GET STREAM TYPE###############
        KVS_STREAM01_NAME= msg_payload["Camera"]["Stream"]
        logger.info(f'Processing stream: {KVS_STREAM01_NAME}')
        
        ######## GET THE KVS ENDPOINT PROVIDING VIDEO STREAM ########
        try:
                get_media_endpoint = kvs_client.get_data_endpoint(
                    APIName="GET_MEDIA",
                    StreamName=KVS_STREAM01_NAME
                )['DataEndpoint']
        except ClientError as e:
                logger.error(f"Failed to get data endpoint for stream {KVS_STREAM01_NAME}: {e}")
                return
            
        print(get_media_endpoint)
        
        ### CONNECT A CLIENT TO THE ENPOINT ##########
        kvs_media_client = session.client('kinesis-video-media', endpoint_url=get_media_endpoint)
        
        print(kvs_media_client)
        
        ###### RECEIVE VIDEO FRAGMENT FROM THE KVS STREAM #########
        try:
                get_media_response = kvs_media_client.get_media(
                    StreamName=KVS_STREAM01_NAME,
                    StartSelector={'StartSelectorType': 'NOW'}
                )
        except ClientError as e:
                logger.error(f"Failed to get media from stream {KVS_STREAM01_NAME}: {e}")
                return
        
        print(get_media_response)    
        
        ###### OBTAIN FRAMES FROM FRAGMENT ############    
        frames = process(get_media_response, KVS_STREAM01_NAME)
        frames_nr = len(frames)
        if frames_nr == 0:
                logger.warning("No frames extracted from the media fragment.")
                return
        
        logger.info(f'{frames_nr} frames extracted from the media fragment.')
        
        ##### ANYLIZE AND SEND TO KINESIS DATA STREAM###############
        for idx,fr in enumerate(frames):
            
            img_jpg = cv2.imencode('.jpg', fr)[1]
            
        ####################OPTIONAL: SAVE FRAMES TO S3 BUCKET#######################
    #        print(img_jpg)
            path = "images/" + str(int(time.time()))+"_"+str(random.randint(1,100))+".jpg"
    #        print(path)
            if idx==0 or idx==4:
                try:
                        s3.put_object(Body=bytearray(img_jpg), Bucket="ggtestdeployment", Key=path, ContentEncoding='base64')
                        logger.info(f'Frame {idx} saved to S3 at {path}.')
                
                except ClientError as e:
                        logger.error(f"Failed to upload frame {idx} to S3: {e}")
                        # Decide whether to continue or log and return
                        continue
                    
            #### SEND FRAMES ASYNCHRONOUSLY TO REKOGNITION ########### 
            try:
                    futures.append(executor.submit(push_reko, img_jpg, frames_nr, msg_payload))
                    logger.debug(f'Frame {idx} submitted for Rekognition analysis.')
            except Exception as e:
                    logger.error(f"Failed to submit frame {idx} for Rekognition analysis: {e}")
            #futures[idx].result() #don't wait here for results
            #response = push_reko(img_jpg) #sequential: decreases efficiency
            
        
        ##### PUT EACH ANALYZED FRAMES INSIDE KINESIS DATA STREAM FOR FURTHER PROCESSING ##########
        for idx, future in enumerate(futures):
            try:
                    result = future.result()
                    print(result)
                    kds_client.put_record(
                        StreamName=DATA_STREAM_NAME,
                        Data=json.dumps(result),
                        PartitionKey="ppe-gateway001-"
                    )
                    logger.info(f'Result for frame {idx} sent to Kinesis Data Stream.')
            except ClientError as e:
                    logger.error(f"Failed to put record for frame {idx} into Kinesis Data Stream: {e}")
        
    ### TRACK ANY OTHER POSSIBLE ERROR #########
    except Exception as e:
        logger.critical(f'Unhandled exception: {e}', exc_info=True)
    
    
def push_reko(img_jpg, frames_nr, msg_payload):
            
    response = reko_client.detect_protective_equipment(Image={'Bytes': bytearray(img_jpg)}, 
                        SummarizationAttributes={'MinConfidence': int(min_confidence), 'RequiredEquipmentTypes':['HEAD_COVER']})
    response.pop('ResponseMetadata','No Key found')
    response["TotalFrames"] = frames_nr
    response['SourceInfo'] = msg_payload
    
    return response
