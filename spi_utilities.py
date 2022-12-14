# videos: https://gist.github.com/jsturgis/3b19447b304616f18657

# Main imports:
import os
from kafka import KafkaProducer
from kafka import KafkaConsumer
from pymongo import MongoClient
import matplotlib as plt
plt.interactive(True)
import json
from json import JSONEncoder
from threading import Thread
import cv2, time
# import pp
import gridfs
import numpy as np
import hashlib
import imutils
import base64
from datetime import date
from simple_facerec import SimpleFacerec
from PIL import Image
import folium
from IPython.display import display
from ipyleaflet import (
    Map, Marker, MarkerCluster,
    TileLayer, ImageOverlay,
    Polyline, Polygon, Rectangle, Circle, CircleMarker,
    Popup,
    GeoJSON,
    DrawControl,
    basemaps
)
# for labeling marker as image:
from ipywidgets import HTML
import IPython
from ipywidgets import widgets
from sidecar import Sidecar
import traceback



def mkdir_if_none(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        
def get_marker_widget(img_path, name):
    
    try:
    
        url = img_path.replace("\\", "/")
        image = IPython.display.Image(url, width = 300)

        widget = widgets.Image(
            value=image.data,
            format='jpg', 
            width=300,
            height=400,
        )

        return widget
    except:
        print(traceback.format_exc())
        safe_message = HTML()
        safe_message.value = name
        return safe_message


# the following module offers Kakfa Producer, Consumer, and mongodb objects.
# Through here, you can initialize the class and just call modules you need only.
class SPI_Utils:
    
    def __init__(self, mode,
                       streaming_kafka_topic="spi_topic",
                       kafka_bootstrap_servers=["localhost:9092"],
                       json_requests_file_path=os.path.join(os.getcwd(), "json_requests.json"),
                       data_store_dir = os.path.join(os.getcwd(), "frame_data"),
                       face_store_dir = os.path.join(os.getcwd(), "faces")
                ):
        # setup main directory where we will store the images arriving from online/offline stream
        self.data_store_dir = data_store_dir
        mkdir_if_none(self.data_store_dir)
        self.face_store_dir = face_store_dir
        # setup default width for all images to be cropped down to
        self.width = 640
        
        # setup parallel processing server for processing videos simultaneously:
        # self.job_server = pp.Server()  # for parallel processing videos to MongoDB
        self.job_list = []
        self.results = []
        
        # initialize kafka topic to write and read from:
        self.streaming_kafka_topic = streaming_kafka_topic
        
        # initializing Kafka Producer:
        self.producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)
        
        # initializing Kafka Consumer:
        self.consumer = KafkaConsumer(self.streaming_kafka_topic, bootstrap_servers=kafka_bootstrap_servers)
        
        # initializing MongoDB client and its databases (db and images):      
        self.client = MongoClient()
        self.db = self.client.testdb
        self.data_collection = self.db.images
        self.fs = gridfs.GridFS(self.db)
        
        # get list of videos requested from agency:
        self.json_requests_dict = self.read_json_data_into_dict(json_requests_file_path)
        if mode != "consumer":
            if self.json_requests_dict is None:
                print("<<<ERRROR: No video requests tag was found is provided json.>>>")
            else:
                print("Requested video sources are:")
                for v in self.json_requests_dict:
                    print(f"  - {self.json_requests_dict[v]['video_path']}")
                print("\n")
        
        # if we are initiating this project for recogniton, then load up face embeddings and map:
        if mode == "recognition":
            # Encode faces from a folder
            self.sfr = SimpleFacerec()
            self.sfr.load_encoding_images(os.path.join(os.getcwd(), "faces"))
            
            # LDN_COORDINATES = (51.5074, 0.1278)
            us_center = [38.6252978589571, -97.3458993652344]
            zoom = 0
            self.spi_map = Map(center=us_center, zoom=zoom)            
            s = Sidecar(title='SPI Map')
            # show the map:
            display(self.spi_map)

        
    # load request from json file (this data serves as user request form for giving camera feeds):
    def read_json_data_into_dict(self, json_file_path):
        with open(json_file_path) as json_data:
            data = json.load(json_data)
        if "video_requests" in data:
            return data["video_requests"]
        else:
            return None
        
    # encode message into bytes for sending through kafka
    @staticmethod
    def encode_message(message):
        return json.dumps(message).encode('utf-8')
    
    # decode message and load into json format:
    @staticmethod
    def decode_message(message):
        message_string = message.decode('utf-8')       
        message_json = json.loads(message_string)
        return message_json
        
    # send kafka message through producer.
    def produce_kafka_messages(self):
        for vid_request in self.json_requests_dict:
            kafka_message = {
                             "video_path": self.json_requests_dict[vid_request]["video_path"],
                             "video_tag": vid_request,
                             "camera_id": self.json_requests_dict[vid_request]["camera_id"],
                             "camera_latitude": self.json_requests_dict[vid_request]["camera_latitude"],
                             "camera_longitude" : self.json_requests_dict[vid_request]["camera_longitude"]
                            }
            self.producer.send(self.streaming_kafka_topic, self.encode_message(kafka_message))
                               
    # get single message from kafka consumer:
    def consume_kafka_messages(self):
        for message in self.consumer:
            decoded_message = self.decode_message(message.value)
            yield decoded_message
    
    # send data to mongodb:
    def push_data_to_mongodb(self, frame, cam_id, cam_latitude, cam_longitude):
        # convert ndarray to string
        imageString = frame.tobytes()

        # store the frame
        image_encoded = self.fs.put(imageString, encoding='utf-8')

        # create our frame meta data
        meta = {
            'name': 'spi_frame',
            'frame': 
            {
                    'image_encoded': image_encoded,
                    'shape': frame.shape,
                    'dtype': str(frame.dtype),
                    'cam_id': cam_id,
                    'camera_latitude': cam_latitude,
                    'camera_longitude': cam_longitude
            }
        }

        # insert the meta data
        self.data_collection.insert_one(meta)
    
    
    # get data from mongodb:
    def get_data_from_mongodb(self):
        image = self.data_collection.find_one({'name': 'spi_frame'})['frame']

        # get the image from gridfs
        gOut = self.fs.get(image['image_encoded'])

        # convert bytes to ndarray
        img = np.frombuffer(gOut.read(), dtype=np.uint8)

        # reshape to match the image size
        frame = np.reshape(img, image['shape'])
        
        return frame
            
        
        
    # FOR MONGO: submit requested video to be processed and recognized by camera thread:
    def run_video_submission_job(self, message):
        src = message["video_path"]
        cam_id = message["camera_id"]
        cam_latitude = message["camera_latitude"]
        cam_longitude = message["camera_longitude"]
        threaded_camera = ThreadedCamera(self.data_store_dir, self.width, cam_id, cam_latitude, cam_longitude, src)
        print(f"Started processing: {src}")
        
            
            
    def mongodb_filler(self):
        kafka_consumer_w_messg_decoder = self.consume_kafka_messages()
        for decoded_message in kafka_consumer_w_messg_decoder:
            print("decoded_message:", decoded_message)
            self.run_video_submission_job(decoded_message)
            
        '''
        for decoded_message in kafka_consumer_w_messg_decoder:
            print("decoded_message:", decoded_message)
            
            # shift it to parallel job:
            self.job_list.append(self.job_server.submit(self.run_video_submission_job(decoded_message), (1,), modules=('pptest',)))
            print(f"Job sumbitted for video source: {decoded_message['video_path']}")
        for job in self.job_list:
            self.results.append(job())
        for result in self.results:
            print(f"Result from parallel job: {result}")
        '''
        
    # manual implementation of structured streaming:
    def perform_spark_streaming_and_processing(self, patience=5):
        
        processed_data = []
        frame_counter = 0
        
        # starting patience level: 
        waited_for = 0
        
        while True:
            
            time.sleep(5)
            
            ongoing_files = os.listdir(self.data_store_dir)
            new_files = [i for i in ongoing_files if i not in processed_data]
            print(f"New files #: {len(new_files)}")
            
            # setup auto-shutdown:
            if new_files == []:
                waited_for+=1
            else:
                waited_for = 0
            if waited_for >= patience:
                print("Waiting period for new video source has been reached. Exiting SPI Recognition Module.")
                
                break
        
            # start processing each image:
            for f in new_files:
                try:
                    with open(os.path.join(self.data_store_dir, f)) as json_file:
                        json_data = json.load(json_file)
                    # print(np.array(json_data["frame"]).shape)
                    frame = Image.fromarray(np.array(json_data["frame"]).astype(np.uint8))
                    # print(np.array(frame).shape)
                    cam_id = json_data["cam_id"]
                    cam_latitude = json_data["cam_latitude"]
                    cam_longitude = json_data["cam_longitude"]

                    # Detect Faces
                    face_locations, face_names = self.sfr.detect_known_faces(np.asarray(frame))
                    for face_loc, name in zip(face_locations, face_names):
                        #y1, x2, y2, x1 = face_loc[0], face_loc[1], face_loc[2], face_loc[3]
                        #cv2.putText(frame, name,(x1, y1 - 10), cv2.FONT_HERSHEY_DUPLEX, 1, (0, 0, 200), 2)
                        #cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 0, 200), 4)
                        if name != "Unknown":
                            print(f"Face detected at cam_id = {cam_id}, frame_num = {frame_counter}: {name} at Lat/Long = {cam_latitude}/{cam_longitude}")
                            frame_counter+=1

                            # mark:
                            #poi_message.value = f"{name}"
                            current_poi_img_path = os.path.join(self.face_store_dir, f"{name}.jpg")
                            current_poi_img_marker = get_marker_widget(current_poi_img_path, name)
                            mark = Marker(location=[cam_latitude, cam_longitude], title=f"{name}\n{cam_latitude}, {cam_longitude}", draggable=False)
                            mark.popup = current_poi_img_marker
                            self.spi_map+=mark
                            mark.interact(opacity=(0.0, 1.0, 0.01))

                            #plt.figure()
                            #plt.title(f"<<<{name}>>> Detected at {cam_latitude}/{cam_longitude} lat/long.")
                            #plt.imshow(frame[y1:y2, x1:x2])

                            # display(self.spi_map)
                    processed_data.append(f)
                except:
                    print(f"<<<ERROR with file: {f}>>>")
                    print(traceback.format_exc())
                    print("\n")
                
                
            
        
        
# encode frame when saving into json file:
class NumpyArrayEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return JSONEncoder.default(self, obj)
    

class BytesEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return obj.decode('utf-8')
        return json.JSONEncoder.default(self, obj)    

        
# Camera streaming class => saves arriving images into either (a) Mongo or (b) into local files as json (requiring auto cleaning every once in a while).
class ThreadedCamera(object):
    def __init__(self, data_store_dir, width, cam_id, cam_latitude, cam_longitude, src=0, fps=30):
        self.capture = cv2.VideoCapture(src)
        
        self.data_store_dir = data_store_dir
        self.base_name = hashlib.sha256(src.encode('utf-8')).hexdigest()
        self.frame_count = 0
        self.fps = fps
        
        self.width = width
        self.cam_id = cam_id
        self.cam_latitude = cam_latitude
        self.cam_longitude = cam_longitude
        
        # Start frame retrieval thread
        self.thread = Thread(target=self.update, args=())
        self.thread.daemon = True
        self.thread.start()
        
        
        
        
    def update(self):
        while True:
            if self.capture.isOpened():
                self.status, self.frame = self.capture.read()
            
            if self.status:
                #if ((self.frame_count) % int(self.fps/3)) != 0:
                #    self.frame_count+=1
                #    continue
                self.frame = imutils.resize(self.frame, width=self.width)
                self.frame_count+=1
                
                # save_frame_as = os.path.join(self.data_store_dir, f"{self.base_name}_{self.frame_count}.csv")
                # #numpy array from image
                # self.frame_reshaped = self.frame.reshape(self.frame.shape[0], -1) # instead of looping and slicing through channels shape = (50, 300)
                # np.savetxt(save_frame_as, self.frame_reshaped, delimiter=',') # save it as numpy array in csv file
                
                # for encoding:
                save_frame_as = os.path.join(self.data_store_dir, f"{self.base_name}_{self.frame_count}.json")
                
                # json file from image
                data = {
                        'frame' : self.frame,
                        'shape' : str(self.frame.shape),
                        'dtype' : str(self.frame.dtype),
                        'processed': str("False"),
                        'timestamp': str(date.today()),
                        'cam_id' : self.cam_id,
                        'cam_latitude' : self.cam_latitude,
                        'cam_longitude' : self.cam_longitude
                }
                with open(save_frame_as, 'w') as f:
                    encodedNumpyData = json.dump(data, f, cls=NumpyArrayEncoder)  # use dumps() to use it as local file
            


        
    '''
    while True:
        try:
            curr_frame = threaded_camera.fetch_frame()

            self.push_data_to_mongodb(curr_frame, cam_id, cam_latitude, cam_longitude) ##############################################################################

            got_frame = self.get_data_from_mongodb()
            got_frame = imutils.resize(got_frame, width=self.width)
            frame+=1
            print("got_frame.dtype:", got_frame.dtype)
            print("got_frame.shape:", got_frame.shape)
            save_frame_as = os.path.join(self.data_store_dir, f"{base_name}_{frame}.csv")
            #numpy array from image
            got_frame_reshaped = got_frame.reshape(got_frame.shape[0], -1) # instead of looping and slicing through channels shape = (50, 300)
            np.savetxt(save_frame_as, got_frame_reshaped, delimiter=',') # save it as numpy array in csv file
            
            if curr_frame is None:
                print(f"<<<WARNING: FRAME RETURNED NONE at frame={frame}")
                return f"Processed video source: {src}"
        except AttributeError:
            pass
    return f"Did not complete processing the video source: {src}"
    '''