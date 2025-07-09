import sqlite3
import json
import time
import logging
import random
import os
import signal
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Configure logging
LOG_FILE = 'channel_scraper.log'
STATE_FILE = 'channel_scraper_state.json'
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[
    logging.FileHandler(LOG_FILE),
    logging.StreamHandler()
])

# Load API keys
with open('api_keys.json', 'r') as file:
    api_keys = json.load(file)["keys"]
current_key_index = 0

# Initialize YouTube API client
def get_youtube_client():
    return build('youtube', 'v3', developerKey=api_keys[current_key_index], cache_discovery=False)

def switch_api_key():
    global current_key_index, youtube
    current_key_index = (current_key_index + 1) % len(api_keys)
    if current_key_index == 0:
        logging.error("All API keys are exhausted. Please wait for quota reset.")
        exit(1)
    youtube = get_youtube_client()
    logging.info(f"Switched to API key index {current_key_index}.")

youtube = get_youtube_client()

# Connect to the database
DATABASE_FILE = 'ds_edu_videos.db'
conn = sqlite3.connect(DATABASE_FILE, check_same_thread=False)
cursor = conn.cursor()

# Create the channels table
cursor.execute("""
CREATE TABLE IF NOT EXISTS channels (
    channel_id TEXT PRIMARY KEY,
    title TEXT,
    description TEXT,
    localized_title TEXT,
    localized_description TEXT,
    published_at TEXT,
    country TEXT,
    likes_playlist TEXT,
    uploads_playlist TEXT,
    view_count INTEGER,
    subscriber_count INTEGER,
    video_count INTEGER
);
""")

# Ensure videos table has channel_id column
cursor.execute("""
PRAGMA table_info(videos);
""")
columns = [row[1] for row in cursor.fetchall()]
if 'channel_id' not in columns:
    cursor.execute("""
    ALTER TABLE videos ADD COLUMN channel_id TEXT REFERENCES channels(channel_id);
    """)

# Load scraper state
if os.path.exists(STATE_FILE):
    with open(STATE_FILE, 'r') as file:
        state = json.load(file)
        last_processed_index = state.get('last_processed_index', -1)
else:
    state = {}
    last_processed_index = -1

# Fetch all video_ids from videos
cursor.execute("SELECT video_id FROM videos")
video_ids = [row[0] for row in cursor.fetchall()]

# Define signal handler for graceful exit
def handle_exit(signum, frame):
    logging.info("Process interrupted. Saving current state...")
    with open(STATE_FILE, 'w') as file:
        json.dump(state, file)
    conn.close()
    logging.info("State saved and connection closed. Exiting.")
    exit(0)

# Register signal handlers
signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)

# Function to fetch channel information
def fetch_channel_info(video_id):
    global youtube
    retries = 3
    while retries > 0:
        try:
            # Get channel ID from video
            video_response = youtube.videos().list(
                part="snippet",
                id=video_id,
                hl="en"
            ).execute()

            items = video_response.get('items', [])
            if items:
                snippet = items[0]['snippet']
                channel_id = snippet['channelId']

                # Fetch channel information
                channel_response = youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=channel_id,
                    hl="en"
                ).execute()

                channel_items = channel_response.get('items', [])
                if channel_items:
                    channel_info = channel_items[0]
                    snippet = channel_info['snippet']
                    content_details = channel_info['contentDetails']
                    statistics = channel_info['statistics']

                return {
                    "channel_id": channel_id,
                    "title": snippet.get('title', ''),
                    "description": snippet.get('description', ''),
                    "localized_title": snippet.get('localized', {}).get('title', ''),
                    "localized_description": snippet.get('localized', {}).get('description', ''),
                    "published_at": snippet.get('publishedAt', ''),
                    "country": snippet.get('country', None),
                    "likes_playlist": content_details['relatedPlaylists'].get('likes', None),
                    "uploads_playlist": content_details['relatedPlaylists'].get('uploads', None),
                    "view_count": statistics.get('viewCount', 0),
                    "subscriber_count": statistics.get('subscriberCount', 0),
                    "video_count": statistics.get('videoCount', 0)
                }
        except HttpError as e:
            if e.resp.status == 403:
                logging.error("API quota exhausted. Switching API key.")
                switch_api_key()
                retries -= 1
            elif e.resp.status == 404:
                logging.warning(f"Video or channel not found. Video ID: {video_id}")
                return None
            else:
                logging.error(f"HTTP error: {e}")
                retries -= 1
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            return None

# Start scraping
for current_index in range(last_processed_index + 1, len(video_ids)):
    video_id = video_ids[current_index]
    logging.info(f"Processing video ID {video_id} ({current_index + 1}/{len(video_ids)})")
    channel_data = fetch_channel_info(video_id)

    if channel_data:
        # Insert channel info into channels table
        cursor.execute("""
            INSERT OR REPLACE INTO channels (channel_id, title, description, localized_title, localized_description, published_at, country, likes_playlist, uploads_playlist, view_count, subscriber_count, video_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            channel_data['channel_id'],
            channel_data['title'],
            channel_data['description'],
            channel_data['localized_title'],
            channel_data['localized_description'],
            channel_data['published_at'],
            channel_data['country'],
            channel_data['likes_playlist'],
            channel_data['uploads_playlist'],
            channel_data['view_count'],
            channel_data['subscriber_count'],
            channel_data['video_count']
        ))

        # Update channel_id in videos table
        cursor.execute("""
            UPDATE videos SET channel_id = ? WHERE video_id = ?
        """, (channel_data['channel_id'], video_id))

        conn.commit()

    # Update scraper state
    state['last_processed_index'] = current_index
    with open(STATE_FILE, 'w') as file:
        json.dump(state, file)

    # Random delay to simulate user behavior
    time.sleep(random.uniform(1, 2))

# Clean up state file
if os.path.exists(STATE_FILE):
    os.remove(STATE_FILE)

# Close the database connection
conn.close()

logging.info("Channel information scraping completed.")
