import os
import json
from googleapiclient.discovery import build
from datetime import datetime
from kafka import KafkaProducer


def extract_youtube_video_data():
    api_key = "AIzaSyAvB3LNRs4iQ3oauNFq45kuldSJ5RWn_NE"
    CHANNEL_ID = "UCtxD0x6AuNNqdXO9Wp5GHew"

    youtube = build('youtube', 'v3', developerKey=api_key)

    request = youtube.channels().list(
        part="contentDetails",
        id=CHANNEL_ID
    )

    response = request.execute()
    upload_playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    video_ids = []
    next_page_token = None

    while True:
        request = youtube.playlistItems().list(
            part = "snippet",
            playlistId = upload_playlist_id,
            maxResults = 50,
            pageToken = next_page_token
        )

        response = request.execute()

        for item in response["items"]:
            video_id = item["snippet"]["resourceId"]["videoId"]
            video_ids.append(video_id)
        
        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break
    
    all_video_data = []
    for i in range(0, len(video_ids), 50):
        batch_video_ids = video_ids[i:i+50]
        video_request = youtube.videos().list(
            part = "snippet,statistics",
            id = ",".join(batch_video_ids)
        )
        video_response = video_request.execute()
        for item in video_response["items"]:
            video_data = {
                "channel_id": CHANNEL_ID,
                "video_id": item["id"],
                "title": item["snippet"]["title"],
                "description": item["snippet"]["description"],
                "published_at": item["snippet"]["publishedAt"],
                "view_count": item["statistics"].get("viewCount", 0),
                "like_count": item["statistics"].get("likeCount", 0),
                "comment_count": item["statistics"].get("commentCount", 0)
            }
            all_video_data.append(video_data)

    producer = KafkaProducer(bootstrap_servers = 'broker:29092',
                            value_serializer = lambda v: json.dumps(v).encode('utf-8'))
    
    for video in all_video_data:
        producer.send('youtube_video_data', video)

    producer.flush()
    producer.close()


