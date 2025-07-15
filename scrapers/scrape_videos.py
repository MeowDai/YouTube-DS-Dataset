import os
import json
import sqlite3
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
from datetime import datetime
from isodate import parse_duration

STATE_FILE = 'scraper_state.json' # File to store the state of the scraper
KEYWORDS_FILE = 'search_keywords.json' # File containing search keywords
DATABASE_FILE = 'ds_edu_videos.db' # SQLite database file to store results
API_KEYS_FILE = 'api_keys.json' # File containing YouTube API keys

# Load API keys
with open(API_KEYS_FILE, 'r') as file:
    api_keys = json.load(file)["keys"]

current_key_index = 0

with open(KEYWORDS_FILE, 'r') as file:
    search_keywords = json.load(file)["keywords"]

# Get the current date and time
collected_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# build the YouTube API client
try:
    youtube = build('youtube', 'v3', developerKey=api_keys[current_key_index])
except Exception as e:
    print(f"Error initializing YouTube API client: {e}")
    exit()

# Load state from file if it exists
if os.path.exists(STATE_FILE):
    with open(STATE_FILE, 'r') as file:
        state = json.load(file)
        current_page_token = state.get('nextPageToken', None)
        start_keyword = state.get('keyword', None)
else:
    current_page_token = None
    start_keyword = None

# Function to handle API key rotation and retry requests
def execute_request(api_operation, *args, **kwargs):
    global current_key_index
    global youtube
    while True:
        try:
            # Dynamically build the request using the latest YouTube client
            api_resource = getattr(youtube, api_operation[0])()
            request = getattr(api_resource, api_operation[1])(*args, **kwargs)
            return request.execute()
        except HttpError as e:
            if e.resp.status == 403 and 'quota' in str(e.content):
                print(f"Quota exceeded for API key {current_key_index + 1}. Switching to the next key.")
                current_key_index = (current_key_index + 1) % len(api_keys)
                if current_key_index == 0:
                    print("All API keys exhausted. Wait until quota resets.")
                    exit()
                # Update the YouTube client with the new API key
                youtube = build('youtube', 'v3', developerKey=api_keys[current_key_index])
                continue
            else:
                print(f"HTTP Error: {e.resp.status} - {e.content}")
                raise e

# Setup SQLite database
def setup_database():
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    return conn, cursor

# Create a table for a keyword if it doesn't exist
def create_table(cursor, keyword):
    table_name = keyword.replace(" ", "_")
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            video_id TEXT PRIMARY KEY,
            title TEXT,
            channel_title TEXT,
            published_at TEXT,
            description TEXT,
            tags TEXT,
            audio_language TEXT,
            textual_language TEXT,
            duration TEXT,
            definition TEXT,
            caption_availability TEXT,
            view_count INTEGER,
            like_count INTEGER,
            comment_count INTEGER,
            paid_product_placement TEXT,
            collected_at TEXT,
            keywords TEXT
        )
    """)

# Insert results into the keyword's table
def insert_results(cursor, table_name, results):
    placeholders = ", ".join(["?"] * len(results[0]))
    columns = ", ".join(results[0].keys())
    query = f"INSERT OR IGNORE INTO {table_name} ({columns}) VALUES ({placeholders})"
    cursor.executemany(query, [tuple(row.values()) for row in results])

try:
    conn, cursor = setup_database()

    # Start from the interrupted keyword if state exists
    if start_keyword:
        keywords_to_process = search_keywords[search_keywords.index(start_keyword):]
    else:
        keywords_to_process = search_keywords

    for keyword in keywords_to_process:
        table_name = keyword.replace(" ", "_").replace("-", "_")
        create_table(cursor, table_name)

        print(f"Processing keyword: {keyword}")
        while True:
            try:
                # Search for videos
                search_response = execute_request(
                    ("search", "list"),
                    part='snippet',
                    q=keyword,
                    type='video',
                    maxResults=50,
                    pageToken=current_page_token,
                    order='viewCount',
                    videoDuration='any'
                )
            except Exception as e:
                print(f"Error executing search request: {e}")
                exit()

            session_results = []
            video_ids = []

            # Collect video IDs and metadata from the search response
            try:
                for item in search_response.get('items', []):
                    video_id = item['id']['videoId']
                    video_data = {
                        'video_id': video_id,
                        'title': item['snippet']['title'],
                        'channel_title': item['snippet']['channelTitle'],
                        'published_at': item['snippet']['publishedAt'],
                    }
                    session_results.append(video_data)
                    video_ids.append(video_id)
                # Get next page token
                current_page_token = search_response.get('nextPageToken', None)
            except KeyError as e:
                print(f"Key error in search response: {e}")
                exit()
            except Exception as e:
                print(f"Unexpected error while processing search response: {e}")
                exit()

            # Fetch additional statistics for the videos using the video IDs
            if video_ids:
                try:
                    stats_response = execute_request(
                        ("videos", "list"),
                        part='statistics,snippet,contentDetails,paidProductPlacementDetails',
                        id=','.join(video_ids)
                    )

                    # Add statistics to the corresponding video data
                    for item in stats_response.get('items', []):
                        for video in session_results:
                            if video['video_id'] == item['id']:
                                video.update({
                                    'description': item['snippet'].get('description', 'N/A'),
                                    'tags': ', '.join(item['snippet'].get('tags', [])) if 'tags' in item['snippet'] else 'N/A',
                                    'audio_language': item['snippet'].get('defaultAudioLanguage', 'N/A'),
                                    'textual_language': item['snippet'].get('defaultLanguage', 'N/A'),
                                    'duration': str(parse_duration(item['contentDetails'].get('duration', 'PT0S'))),
                                    'definition': item['contentDetails'].get('definition', 'N/A'),
                                    'caption_availability': item['contentDetails'].get('caption', 'N/A'),
                                    'view_count': item['statistics'].get('viewCount', 'N/A'),
                                    'like_count': item['statistics'].get('likeCount', 'N/A'),
                                    'comment_count': item['statistics'].get('commentCount', 'N/A'),
                                    'paid_product_placement': item.get('paidProductPlacementDetails', {}).get('hasPaidProductPlacement', 'N/A'),
                                    'collected_at': collected_at,
                                    "keywords": keyword
                                })
                except Exception as e:
                    print(f"Error fetching video statistics: {e}")
                    exit()

            # Save session results to the database
            if session_results:
                insert_results(cursor, table_name, session_results)
                conn.commit()

            # Save current state to file
            with open(STATE_FILE, 'w') as file:
                json.dump({
                    'nextPageToken': current_page_token,
                    'keyword': keyword
                }, file)

            # Exit loop if no more pages
            if not current_page_token:
                next_index = search_keywords.index(keyword) + 1
                next_keyword = search_keywords[next_index] if next_index < len(search_keywords) else None

                with open(STATE_FILE, 'w') as file:
                    json.dump({
                        'nextPageToken': None,
                        'keyword': next_keyword
                    }, file)
                print(f"Scraping complete for keyword \"{keyword}\".")
                break

except KeyboardInterrupt:
    print("\nScraping interrupted by user.")
    conn.commit()
    conn.close()

# Clean up state file if scraping is complete
if not current_page_token:
    os.remove(STATE_FILE)
    print(f"Scraping complete. State file removed.")