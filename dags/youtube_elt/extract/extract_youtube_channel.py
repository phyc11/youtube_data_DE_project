import os
import json
from googleapiclient.discovery import build
from datetime import datetime
from kafka import KafkaProducer

def extract_youtube_channel_data():
    api_key = "AIzaSyAvB3LNRs4iQ3oauNFq45kuldSJ5RWn_NE"
    CHANNEL_ID = "UCtxD0x6AuNNqdXO9Wp5GHew"
    youtube = build('youtube', 'v3', developerKey=api_key)

    request = youtube.channels().list(
        part = 'snippet,statistics',
        id = CHANNEL_ID
    )

    response = request.execute()
    for item in response["items"]:
        channel_data = {
            "channel_id" : item["id"],
            "title" : item["snippet"]["title"],
            "description" : item["snippet"]["description"],
            "published_at" : item["snippet"]["publishedAt"],
            "country" : item["snippet"]["country"],
            "view_count" : item["statistics"].get("viewCount", 0),
            "subscriber_count" : item["statistics"].get("subscriberCount", 0),
            "video_count" : item["statistics"].get("videoCount", 0),
            "date": datetime.utcnow().date().isoformat() 
        }

    producer = KafkaProducer(bootstrap_servers = 'broker:29092', value_serializer = lambda v: json.dumps(v).encode('utf-8'))
    producer.send("youtube_channel_data", channel_data)
    producer.flush()
    producer.close()