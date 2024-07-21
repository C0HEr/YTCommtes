from googleapiclient.discovery import build
from confluent_kafka import Producer

api_service_name = "youtube"
api_version = "v3"
api_key = "AIzaSyDJMefX1gxPIbq6ev6XGx0hszArigFhmXg"
video_id= "q-_ezD9Swz4" 

comments = []

def video_comments(video_id):
    youtube = build('youtube', 'v3', developerKey=api_key)
    video_response = youtube.commentThreads().list(part='snippet,replies', videoId=video_id).execute()

    while video_response:
        for item in video_response['items']:
            comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
            comments.append(comment)
            replycount = item['snippet']['totalReplyCount']
            if replycount > 0:
                for reply in item['replies']['comments']:
                    reply = reply['snippet']['textDisplay']
                    comments.append(reply)

        if 'nextPageToken' in video_response:
            video_response = youtube.commentThreads().list(
                part='snippet,replies',
                videoId=video_id,
                pageToken=video_response['nextPageToken']
            ).execute()
        else:
            break

def kafka_produce(comments):
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    for comment in comments:
        producer.produce('youtube-comments', comment)
        producer.flush()

if __name__ == "__main__":
    video_comments(video_id)
    kafka_produce(comments)
