import os
import json
from googleapiclient.discovery import build
from datetime import datetime, timedelta
from kafka import KafkaProducer


def extract_youtube_comments():
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
            part="snippet",
            playlistId=upload_playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )

        response = request.execute()

        for item in response["items"]:
            video_id = item["snippet"]["resourceId"]["videoId"]
            video_ids.append(video_id)
        
        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break
    
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    all_comments_today = []
    
    for video_id in video_ids:
        next_page_token = None
        found_old_comment = False
        
        while True and not found_old_comment:
            comment_request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=100,
                pageToken=next_page_token,
                textFormat="plainText",
                order="time"  
            )
            
            comment_response = comment_request.execute()
            
            for item in comment_response["items"]:
                comment_snippet = item["snippet"]["topLevelComment"]["snippet"]
                comment_published_at = comment_snippet["publishedAt"]
                
                comment_date_str = comment_published_at.split('T')[0]
                
                if comment_date_str == today_str:
                    comment_data = {
                        "channel_id": CHANNEL_ID,
                        "video_id": video_id,
                        "comment_id": item["id"],
                        "text": comment_snippet["textDisplay"],
                        "author": comment_snippet["authorDisplayName"],
                        "published_at": comment_published_at,
                        "like_count": comment_snippet["likeCount"]
                    }
                    all_comments_today.append(comment_data)
                else:
                    found_old_comment = True
                    break
            
            if found_old_comment:
                break
                
            next_page_token = comment_response.get("nextPageToken")
            if not next_page_token:
                break
    
    producer = KafkaProducer(
        bootstrap_servers='broker:29092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    for comment in all_comments_today:
        producer.send('youtube_comments_data', comment)

    producer.flush()
    producer.close()
    



