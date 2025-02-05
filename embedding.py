import sqlite3
from rake_nltk import Rake
from sentence_transformers import SentenceTransformer
import numpy as np
import os
import nltk

# Download stopwords resource for Rake
nltk.download('stopwords')
nltk.download('punkt_tab')

# Initialize Rake for keyword extraction
rake = Rake()

# Database connection
db_path = 'youtube_video_data.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Query to join the two tables and retrieve necessary columns
query = """
    SELECT mv.title, mv.description, vt.transcript, mv.video_id
    FROM merged_videos mv
    LEFT JOIN video_transcripts vt ON mv.video_id = vt.video_id
"""
cursor.execute(query)
rows = cursor.fetchall()

# Function to extract keywords using Rake
def extract_keywords(text, max_length=100):
    if not text:
        return ""
    rake.extract_keywords_from_text(text) # extract key
    keywords = rake.get_ranked_phrases()
    return " ".join(keywords[:max_length])

# Prepare inputs for the embedding model
video_ids = []
sentences = []
input_lengths = []
comparison = []  # Store original vs extracted keywords for comparison
for row in rows:
    title = row[0] if row[0] else ""
    description = row[1] if row[1] else ""
    transcript = row[2] if row[2] else ""

    # Extract keywords
    description_keywords = extract_keywords(description, max_length=10)
    transcript_keywords = extract_keywords(transcript, max_length=20)

    # Combine text for embedding
    combined_text = f"Title: {title}\nDescription Keywords: {description_keywords}\nTranscript Keywords: {transcript_keywords}"
    sentences.append(combined_text)
    video_ids.append(row[3])
    input_lengths.append(len(combined_text))

    # Store comparison
    comparison.append({
        "video_id": row[3],
        "original_description": description,
        "extracted_description_keywords": description_keywords,
        "original_transcript": transcript,
        "extracted_transcript_keywords": transcript_keywords
    })

# Display input lengths for verification
for video_id, length in zip(video_ids, input_lengths):
    print(f"Video ID: {video_id}, Input Length: {length}")

# Display comparison of original text and extracted keywords
for item in comparison:
    print(f"Video ID: {item['video_id']}")
    print(f"Original Description: {item['original_description']}")
    print(f"Extracted Description Keywords: {item['extracted_description_keywords']}")
    print(f"Original Transcript: {item['original_transcript']}")
    print(f"Extracted Transcript Keywords: {item['extracted_transcript_keywords']}")
    print("-" * 80)

# Load the SentenceTransformer model
model_dir = r'F:\classification\KaLM-embedding-multilingual-mini-instruct-v1.5'
model = SentenceTransformer(model_dir)  # Do NOT set trust_remote_code
model.max_seq_length = 1024

# Define the prompt for the embedding model
prompt = (
    "Instruct: Classify the following video by whether it serves any form of educational purpose, "
    "such as tutorials, seminars, conferences, workshops, lectures, or other formats designed to teach "
    "or explain concepts, skills, or knowledge.\nQuery: "
)

# Generate embeddings
embeddings = model.encode(
    sentences,
    prompt=prompt,
    normalize_embeddings=True,
    batch_size=256,
    show_progress_bar=True
)

# Save embeddings to a file
output_dir = "embeddings_output1"
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, "video_embeddings.npy")
np.save(output_path, embeddings)

# Save mapping of video IDs to embeddings
mapping_path = os.path.join(output_dir, "video_id_mapping.txt")
with open(mapping_path, "w") as f:
    for video_id in video_ids:
        f.write(video_id + "\n")

print(f"Embeddings saved to {output_path}")
print(f"Video ID mapping saved to {mapping_path}")

conn.close()
