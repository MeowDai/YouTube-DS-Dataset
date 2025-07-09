import sqlite3
import os
import json
import time
import random
import logging
import signal
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound, VideoUnavailable

# Database file paths
DATABASE_FILE = 'ds_edu_videos.db'
STATE_FILE = 'transcript_scraper_state.json'
LOG_FILE = 'transcript_scraper.log'
# Define max retry count
MAX_RETRIES = 3

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[
    logging.FileHandler(LOG_FILE),
    logging.StreamHandler()
])

# Load scraping state
if os.path.exists(STATE_FILE):
    with open(STATE_FILE, 'r') as file:
        state = json.load(file)
        last_processed_index = state.get('last_processed_index', -1)
else:
    state = {}
    last_processed_index = -1

# Connect to database containing video_id
conn = sqlite3.connect(DATABASE_FILE)
cursor = conn.cursor()

# Create table for storing transcripts
cursor.execute("""
CREATE TABLE IF NOT EXISTS transcripts (
    video_id TEXT PRIMARY KEY,
    transcript TEXT,
    type TEXT,
    translatable TEXT
);
""")

# Get all video_ids
cursor.execute("SELECT video_id FROM videos")
video_ids = [row[0] for row in cursor.fetchall()]

# Batch size for processing
BATCH_SIZE = 100

# Define exit handler
def handle_exit(signum, frame):
    logging.info("Process interrupted. Saving current state...")
    with open(STATE_FILE, 'w') as file:
        json.dump(state, file)
    conn.close()
    conn.close()
    logging.info("State saved and connections closed. Exiting.")
    exit(0)

# Register signal handlers
signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)

# Continue from last processed index
for index in range(last_processed_index + 1, len(video_ids)):
    video_id = video_ids[index]
    retries = 0
    success = False

    while retries < 3 and not success:
        try:
            # ScraperAPI dynamic proxy request
            logging.info(f"Fetching transcript for video_id: {video_id} (index {index})")

            # Use proxy to call YouTubeTranscriptApi
            transcript_list = YouTubeTranscriptApi.list_transcripts(video_id, proxies={"http": "http://your_proxy_here", "https": "http://your_proxy_here"})
            transcript = transcript_list.find_transcript(['en'])
            transcript_data = transcript.fetch()

            # Join transcript into a single string
            transcript_text = " ".join([item['text'] for item in transcript_data])

            # Determine transcript type
            transcript_type = "auto-generated" if transcript.is_generated else "creator-uploaded"

            transcript_translatable = "true" if transcript.is_translatable else "false"

            # Insert transcript into database
            cursor.execute("""
                INSERT OR IGNORE INTO transcripts (video_id, transcript, type, translatable)
                VALUES (?, ?, ?, ?)
            """, (video_id, transcript_text, transcript_type, transcript_translatable))
            conn.commit()

            # logging.info(f"Transcript fetched for video_id: {video_id} (index {index})")
            success = True

        except (TranscriptsDisabled, NoTranscriptFound, VideoUnavailable) as specific_error:
            if isinstance(specific_error, TranscriptsDisabled):
                logging.info(f"Transcripts are disabled for video_id: {video_id}")
            elif isinstance(specific_error, NoTranscriptFound):
                logging.info(f"No transcript found for video_id: {video_id}")
            elif isinstance(specific_error, VideoUnavailable):
                logging.info(f"Video unavailable for video_id: {video_id}")
            success = True  # No need to retry for these specific errors
        except Exception as e:
            logging.error(f"Error fetching transcript for video_id {video_id} (attempt {retries + 1}): {e}")
            retries += 1
            if retries < MAX_RETRIES:
                time.sleep(60)  # Wait 1 minute before retrying
            else:
                logging.error(f"Max retries reached for video_id {video_id}. Skipping.")
                break

    # Update scraping state
    state['last_processed_index'] = index
    with open(STATE_FILE, 'w') as file:
        json.dump(state, file)

    # Random delay to simulate user behavior
    time.sleep(random.uniform(1, 5))

    # Pause after each batch
    if (index + 1) % BATCH_SIZE == 0:
        logging.info(f"Batch {index // BATCH_SIZE + 1} completed. Pausing for 5 minutes...")
        time.sleep(300)  # Pause for 5 minutes

# Close database connections
conn.close()

# Clean up state file
if os.path.exists(STATE_FILE):
    os.remove(STATE_FILE)

logging.info("Transcript fetching completed.")
