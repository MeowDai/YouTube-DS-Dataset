import sqlite3
import os
import time
import random
import logging
import signal
import json
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import socket

# Configure logging
LOG_FILE = 'comments_scraper.log' # Log file for comments scraping
STATE_FILE = 'comments_scraper_state.json' # File to store the state of the comments scraper
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[
    logging.FileHandler(LOG_FILE),
    logging.StreamHandler()
])

# Load API keys from api_keys.json
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
        logging.error("All API keys exhausted. Please wait until quotas reset.")
        exit(1)  # Exit the script when all keys are exhausted
    youtube = get_youtube_client()
    logging.info(f"Switched to API key index {current_key_index}.")

youtube = get_youtube_client()

# Connect to the database
COMMENTS_DATABASE_FILE = 'ds_edu_videos.db'
conn_comments = sqlite3.connect(COMMENTS_DATABASE_FILE, check_same_thread=False)
cursor_comments = conn_comments.cursor()

# Create tables for comments and replies
cursor_comments.execute("""
CREATE TABLE IF NOT EXISTS comments (
    thread_id TEXT PRIMARY KEY,
    video_id TEXT NOT NULL,
    top_level_text TEXT,
    top_level_like_count INTEGER,
    top_level_published_at TEXT,
    top_level_updated_at TEXT,
    total_reply_count INTEGER,
    FOREIGN KEY (video_id) REFERENCES videos(video_id)
);
""")

cursor_comments.execute("""
CREATE TABLE IF NOT EXISTS replies (
    reply_id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    video_id TEXT NOT NULL,
    reply_text TEXT,
    reply_like_count INTEGER,
    reply_published_at TEXT,
    reply_updated_at TEXT,
    FOREIGN KEY (thread_id) REFERENCES comments (thread_id),
    FOREIGN KEY (video_id) REFERENCES videos(video_id)
);
""")

# Load scraper state
if os.path.exists(STATE_FILE):
    with open(STATE_FILE, 'r') as file:
        state = json.load(file)
        last_processed_index = state.get('last_processed_index', -1)
else:
    state = {}
    last_processed_index = -1

# Fetch all video IDs
cursor_comments.execute("SELECT video_id FROM videos")
video_ids = [row[0] for row in cursor_comments.fetchall()]

# Define signal handler for graceful exit
def handle_exit(signum, frame):
    logging.info("Process interrupted. Saving current state...")
    with open(STATE_FILE, 'w') as file:
        json.dump(state, file)
    conn_comments.close()
    logging.info("State saved and connections closed. Exiting.")
    exit(0)

# Register signal handlers
signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)

# Function to fetch comments
def fetch_comments(video_id):
    global youtube
    total_top_level = 0
    total_replies = 0
    retries = 3  # Number of retries for transient errors
    while retries > 0:
        try:
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=video_id,
                maxResults=100,
                order="time"  # Get comments in chronological order
            )
            while request:
                response = request.execute()
                items = response.get('items', [])
                total_top_level += len(items)
                for item in items:
                    thread_id = item['id']
                    snippet = item['snippet']['topLevelComment']['snippet']
                    top_level_text = snippet.get('textDisplay', '')
                    top_level_like_count = snippet.get('likeCount', 0)
                    top_level_published_at = snippet.get('publishedAt', '')
                    top_level_updated_at = snippet.get('updatedAt', '')
                    total_reply_count = item['snippet'].get('totalReplyCount', 0)

                    # Insert top-level comment into comments table
                    cursor_comments.execute("""
                        INSERT OR REPLACE INTO comments (thread_id, video_id, top_level_text, top_level_like_count, top_level_published_at, top_level_updated_at, total_reply_count)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (thread_id, video_id, top_level_text, top_level_like_count, top_level_published_at, top_level_updated_at, total_reply_count))

                    # Handle replies
                    if 'replies' in item:
                        replies = item['replies']['comments']
                        total_replies += len(replies)
                        for reply in replies:
                            reply_id = reply['id']
                            reply_snippet = reply['snippet']
                            reply_text = reply_snippet.get('textDisplay', '')
                            reply_like_count = reply_snippet.get('likeCount', 0)
                            reply_published_at = reply_snippet.get('publishedAt', '')
                            reply_updated_at = reply_snippet.get('updatedAt', '')

                            # Insert reply into replies table
                            cursor_comments.execute("""
                                INSERT OR REPLACE INTO replies (reply_id, thread_id, video_id, reply_text, reply_like_count, reply_published_at, reply_updated_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                            """, (reply_id, thread_id, video_id, reply_text, reply_like_count, reply_published_at, reply_updated_at))

                    conn_comments.commit()

                # Get next page
                request = youtube.commentThreads().list_next(request, response)
                time.sleep(random.uniform(1, 2))  # Random delay

            total_comments = total_top_level + total_replies
            logging.info(f"Video ID {video_id}: Top-level comments = {total_top_level}, Replies = {total_replies}, Total = {total_comments}")
            break

        except HttpError as e:
            error_reason = e.error_details[0]['reason'] if hasattr(e, 'error_details') and e.error_details else None
            if error_reason == "commentsDisabled":
                logging.warning(f"Comments are disabled for video_id: {video_id}")
                break
            elif e.resp.status == 403:
                logging.error(f"API quota exceeded for key index {current_key_index}. Switching to next key.")
                switch_api_key()
                retries -= 1
            elif e.resp.status == 404:
                logging.error(f"Video not found for video_id: {video_id}")
                break
            else:
                logging.error(f"HTTP error for video_id {video_id}: {e}")
                retries -= 1
        except (socket.error, ConnectionResetError) as e:
            logging.error(f"Network error for video_id {video_id}: {e}. Retrying...")
            retries -= 1
            time.sleep(5)  # Wait before retrying
        except Exception as e:
            logging.error(f"Unexpected error for video_id {video_id}: {e}")
            break

# Resume scraping from the last processed video ID
for current_index in range(last_processed_index + 1, len(video_ids)):
    video_id = video_ids[current_index]
    logging.info(f"Fetching comments for video_id: {video_id} ({current_index + 1}/{len(video_ids)})")
    fetch_comments(video_id)

    # Update scraper state
    state['last_processed_index'] = current_index
    with open(STATE_FILE, 'w') as file:
        json.dump(state, file)

    # Random delay to simulate user behavior
    time.sleep(random.uniform(1, 3))

# Clean up state file
if os.path.exists(STATE_FILE):
    os.remove(STATE_FILE)

# Close the database connection
conn_comments.close()

logging.info("Comment fetching completed.")
