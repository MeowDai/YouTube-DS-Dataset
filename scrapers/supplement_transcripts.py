import sqlite3
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound, CouldNotRetrieveTranscript, YouTubeRequestFailed
import logging
import time
import random

# Connect to database
db_path = "ds_edu_videos.db" 
# Configure logging
logging.basicConfig(filename="transcript_update.log", level=logging.INFO) 

while True:
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        cursor = conn.cursor()
        # Create table to store failed video IDs
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS failed_videos (
                video_id TEXT PRIMARY KEY
            );
        """)
        conn.commit()

        # Get all video_ids that need transcripts
        cursor.execute("""
            SELECT mv.video_id 
            FROM videos mv
            LEFT JOIN video_transcripts vt ON mv.video_id = vt.video_id
            LEFT JOIN failed_videos fv ON mv.video_id = fv.video_id
            WHERE vt.video_id IS NULL AND fv.video_id IS NULL;
        """)
        missing_videos = [row[0] for row in cursor.fetchall()]
        print(f"Found {len(missing_videos)} videos with missing transcripts.")

        # Store transcript data
        transcript_data = []

        for video_id in missing_videos:
            try:
                # Get transcript list
                time.sleep(random.uniform(1,4))  # Avoid frequent requests
                transcript_list = YouTubeTranscriptApi.list_transcripts(video_id, proxies={"https": ""})
                user_transcript = None
                auto_transcript = None

                # First, look for user-uploaded transcripts
                for transcript in transcript_list:
                    if not transcript.is_generated:  # User-uploaded transcript
                        user_transcript = transcript
                        break  # Prefer user-uploaded transcript
                
                # If no user-uploaded transcript, look for auto-generated transcript
                if not user_transcript:
                    for transcript in transcript_list:
                        # Select auto-generated transcript
                        if transcript.is_generated:
                            auto_transcript = transcript
                            break 

                # Select transcript to translate
                selected_transcript = user_transcript if user_transcript else auto_transcript
                if selected_transcript:
                    original_language = selected_transcript.language
                    if original_language == "en":
                        transcript_type = "auto-generated" if user_transcript else "manual-created"
                        transcript_text = " ".join([entry["text"] for entry in selected_transcript.fetch()])
                    else:
                        translated_transcript = selected_transcript.translate('en').fetch()
                        transcript_text = " ".join([entry["text"] for entry in translated_transcript])
                        
                        # Determine transcript type
                        transcript_type = (
                            f"manual-created (Translated from {original_language})"
                            if user_transcript else 
                            f"auto-generated (Translated from {original_language})"
                        )
                    
                    # Insert into database
                    cursor.execute("""
                        INSERT INTO transcripts (video_id, transcript, type)
                        VALUES (?, ?, ?)
                    """, (video_id, transcript_text, transcript_type))

                    conn.commit() 

                    logging.info(f"Updated transcript for video {video_id}")
                    print(f"Updated transcript for video {video_id}")
                    
                    transcript_data.append((video_id, transcript_text, transcript_type))
                else:
                    cursor.execute("INSERT INTO failed_videos (video_id) VALUES (?)", (video_id,))
                    conn.commit()
                    logging.warning(f"No translatable transcript found for video {video_id}")
                    print("No translatable transcript found for video {video_id}")
                
            except (TranscriptsDisabled, NoTranscriptFound):
                
                cursor.execute("INSERT INTO failed_videos (video_id) VALUES (?)", (video_id,))
                conn.commit()
                logging.warning(f'No translatable transcript in English found for video {video_id}')
                print(f'No translatable transcript in English found for video {video_id}')

        # Commit changes and close database
        conn.close()

        print(f"Updated {len(transcript_data)} new translated transcripts.")
        break # Exit loop after successful completion

    except Exception as e:
        print(f"An error occurred: {e}. Retrying in 60 seconds...")
        logging.error(f"Main loop error: {e}")
        time.sleep(60)  # Wait 60 seconds before retrying on error
