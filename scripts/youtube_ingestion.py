import os
import json
from googleapiclient.discovery import build
from datetime import datetime

def fetch_channel_data(api_key, channel_id):
    """Fetch channel statistics"""
    youtube = build('youtube', 'v3', developerKey=api_key)
    
    # Get channel stats
    request = youtube.channels().list(
        part='statistics,snippet',
        id=channel_id
    )
    response = request.execute()
    
    return response['items'][0]

def fetch_videos_data(api_key, channel_id, max_results=50):
    """Fetch recent videos from channel"""
    youtube = build('youtube', 'v3', developerKey=api_key)
    
    # Get channel uploads playlist
    channel_response = youtube.channels().list(
        part='contentDetails',
        id=channel_id
    ).execute()
    
    playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    # Get videos from playlist
    videos = []
    next_page_token = None
    
    while len(videos) < max_results:
        playlist_request = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=min(50, max_results - len(videos)),
            pageToken=next_page_token
        )
        playlist_response = playlist_request.execute()
        
        video_ids = [item['snippet']['resourceId']['videoId'] 
                     for item in playlist_response['items']]
        
        # Get video statistics
        videos_request = youtube.videos().list(
            part='statistics,snippet,contentDetails',
            id=','.join(video_ids)
        )
        videos_response = videos_request.execute()
        
        videos.extend(videos_response['items'])
        
        next_page_token = playlist_response.get('nextPageToken')
        if not next_page_token:
            break
    
    return videos

def save_data(data, filename):
    """Save data to JSON file"""
    os.makedirs('data/raw', exist_ok=True)
    filepath = f'data/raw/{filename}'
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"Data saved to {filepath}")

if __name__ == "__main__":
    API_KEY = os.getenv('YOUTUBE_API_KEY')
    CHANNEL_ID = os.getenv('YOUTUBE_CHANNEL_ID')
    
    channel_data = fetch_channel_data(API_KEY, CHANNEL_ID)
    videos_data = fetch_videos_data(API_KEY, CHANNEL_ID, max_results=100)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    save_data(channel_data, f'channel_data_{timestamp}.json')
    save_data(videos_data, f'videos_data_{timestamp}.json')