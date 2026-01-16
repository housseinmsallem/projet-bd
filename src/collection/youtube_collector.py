import os
import json
import time
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime, timedelta

class YouTubeCollector:
    def __init__(self, api_key, storage_path="data/raw"):
        self.youtube = build('youtube', 'v3', developerKey=api_key)
        self.storage_path = storage_path
        os.makedirs(storage_path, exist_ok=True)

    def search_videos(self, query, max_results=50, published_after=None, published_before=None):
        """Search for videos matching keywords."""
        videos = []
        next_page_token = None
        
        print(f"Searching for: {query} (Max: {max_results})")
        
        while len(videos) < max_results:
            try:
                # API limit is 50 per page
                fetch_limit = min(50, max_results - len(videos))
                
                request = self.youtube.search().list(
                    part="snippet",
                    q=query,
                    type="video",
                    maxResults=fetch_limit,
                    publishedAfter=published_after,
                    publishedBefore=published_before,
                    pageToken=next_page_token,
                    relevanceLanguage="en"
                )
                response = request.execute()
                
                items = response.get('items', [])
                if not items:
                    break
                    
                for item in items:
                    videos.append({
                        'video_id': item['id']['videoId'],
                        'title': item['snippet']['title'],
                        'description': item['snippet']['description'],
                        'published_at': item['snippet']['publishedAt'],
                        'channel_id': item['snippet']['channelId'],
                        'channel_title': item['snippet']['channelTitle']
                    })
                
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
                    
            except HttpError as e:
                print(f"An HTTP error {e.resp.status} occurred: {e.content}")
                break
            except Exception as e:
                print(f"An error occurred: {e}")
                break
                
        return videos

    def collect_historical_data(self, query, start_year=2014, end_year=2024, videos_per_year=10):
        """Collect data year by year to ensure coverage."""
        all_videos = []
        for year in range(start_year, end_year + 1):
            start_date = f"{year}-01-01T00:00:00Z"
            end_date = f"{year}-12-31T23:59:59Z"
            print(f"Collecting for {year}...")
            videos = self.search_videos(
                query, 
                max_results=videos_per_year, 
                published_after=start_date, 
                published_before=end_date
            )
            all_videos.extend(videos)
        return all_videos

    def get_video_details(self, video_ids):
        """Get statistics and more details for a list of video IDs."""
        video_details = []
        
        # Youtube API allows max 50 ids per request
        chunk_size = 50
        for i in range(0, len(video_ids), chunk_size):
            chunk = video_ids[i:i+chunk_size]
            try:
                request = self.youtube.videos().list(
                    part="snippet,statistics,contentDetails",
                    id=','.join(chunk)
                )
                response = request.execute()
                
                for item in response.get('items', []):
                    stats = item.get('statistics', {})
                    snippet = item.get('snippet', {})
                    video_details.append({
                        'video_id': item['id'],
                        'title': snippet.get('title'),
                        'description': snippet.get('description'),
                        'published_at': snippet.get('publishedAt'),
                        'channel_title': snippet.get('channelTitle'),
                        'channel_id': snippet.get('channelId'),
                        'tags': snippet.get('tags', []),
                        'view_count': int(stats.get('viewCount', 0)),
                        'like_count': int(stats.get('likeCount', 0)),
                        'comment_count': int(stats.get('commentCount', 0)),
                        'duration': item['contentDetails']['duration'],
                        'definition': item['contentDetails']['definition']
                    })
            except Exception as e:
                print(f"Error fetching details for chunk: {e}")
                
        return video_details

    def get_comments(self, video_id, max_comments=1000):
        """Get top level comments for a video."""
        comments = []
        next_page_token = None
        
        while len(comments) < max_comments:
            try:
                request = self.youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=min(1000, max_comments - len(comments)),
                    textFormat="plainText",
                    pageToken=next_page_token
                )
                response = request.execute()
                
                for item in response.get('items', []):
                    comment = item['snippet']['topLevelComment']['snippet']
                    comments.append({
                        'video_id': video_id,
                        'comment_id': item['id'],
                        'author': comment['authorDisplayName'],
                        'text': comment['textDisplay'],
                        'like_count': comment['likeCount'],
                        'published_at': comment['publishedAt']
                    })
                
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
                    
            except HttpError as e:
                if e.resp.status == 403: # Comments disabled
                    pass
                else:
                    print(f"Error fetching comments for {video_id}: {e}")
                break
            except Exception:
                break
        
        return comments

    def save_data(self, data, filename):
        """Save data to JSON file in storage path."""
        file_path = os.path.join(self.storage_path, filename)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        print(f"Saved {len(data)} records to {file_path}")

if __name__ == "__main__":
    # Example usage
    API_KEY = os.environ.get("YOUTUBE_API_KEY")
    if not API_KEY:
        print("Please set YOUTUBE_API_KEY environment variable.")
        # Create dummy data for testing if no key
        print("Generating mock data for structure verification...")
        mock_videos = [{"video_id": "123", "title": "Mock Video", "description": "Test #Israel #Palestine"}]
        collector = YouTubeCollector(api_key="dummy")
        collector.save_data(mock_videos, "videos_sample.json")
    else:
        collector = YouTubeCollector(API_KEY)
        videos = collector.search_videos("Israel Palestine conflict", max_results=500)
        collector.save_data(videos, "videos_sample.json")
