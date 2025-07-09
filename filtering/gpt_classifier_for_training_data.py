import sqlite3
from openai import OpenAI
import os
import pandas as pd
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import time

# Set your OpenAI API Key
client = OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY"),  # This is the default and can be omitted
)
# Database connection
db_path = "youtube_video_data.db"  # Replace with the actual path to your SQLite database
conn = sqlite3.connect(db_path)

# Check if training_sample table exists
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='training_sample';")
exists = cursor.fetchone()

if not exists:
    # Load all video data excluding already labeled ones
    query = """
    SELECT 
        mv.video_id,
        mv.title,
        mv.description,
        vt.transcript,
        mv.keywords
    FROM 
        merged_videos mv
    LEFT JOIN 
        video_transcripts vt ON mv.video_id = vt.video_id
    LEFT JOIN 
        manual_labels tl ON mv.video_id = tl.video_id
    WHERE 
        tl.video_id IS NULL  -- Exclude already labeled data
        AND vt.transcript IS NOT NULL  -- Remove videos without transcripts
    """
    
    df = pd.read_sql_query(query, conn)

    # Create a dictionary to track keyword occurrences
    keyword_list = ["relational theory", "relational theory relations", "relational theory tuples", "relational theory attributes", 
            "tuple relational calculus", "relational algebra", "data visualization", "database optimization", "database optimization indexing", 
            "database optimization query execution plans", "database optimization query optimization", "database scalability", 
            "database scalability replication", "database scalability sharding", "NoSQL database management systems", "data independence", 
            "logical data independence", "physical data independence", "logical and physical data independence", "database management system components", 
            "functions and stored procedures", "data modeling", "data modeling conceptual modeling", "data modeling mapping conceptual models to logical models", 
            "data modeling creating tables and columns", "database normalization", "database normalization functional dependency", 
            "database normalization candidate", "database normalization super keys", "database normalization normal forms up to BCNF", 
            "database normalization multivalued dependency", "database normalization join dependency", "object-oriented data models", 
            "semi-structured traditional data models", "SQL", "SQL select", "SQL project", "SQL join", "SQL insert", "SQL update", "SQL delete", 
            "SQL aggregation", "SQL group by", "SQL subqueries", "SQL common table expressions", "transaction processing", "concurrency control", 
            "isolation levels", "concurrency control and isolation levels", "database back-ups", "database recovery", "database back-ups and recovery", 
            "distributed database management systems", "data mining", "data mining algorithms", "data mining associative pattern", "data mining sequential pattern", 
            "data mining associative and sequential patterns", "data mining data cleaning", "data mining market basket analysis", "data privacy", "data ethics", 
            "data privacy and ethics", "data security", "database access management", "data security and database access management", "data warehousing"]

    keyword_counts = {keyword: 0 for keyword in keyword_list}

    def count_keywords(row):
        if row["keywords"]:
            video_keywords = row["keywords"].split(",")
            for keyword in video_keywords:
                keyword = keyword.strip()
                if keyword in keyword_counts:
                    keyword_counts[keyword] += 1

    # Count keyword occurrences
    df.apply(count_keywords, axis=1)

    # Compute sampling weights based on keyword distribution
    df["sampling_weight"] = df["keywords"].apply(lambda x: sum(1 / keyword_counts[k] for k in x.split(",") if k.strip() in keyword_counts) if x else 0)

    # Normalize weights
    df["sampling_weight"] /= df["sampling_weight"].sum()

    # Sample 3000 rows based on proportional keyword distribution
    final_sample = df.sample(n=min(len(df), 3000), weights="sampling_weight", random_state=42)

    # Store final_sample in a database table
    conn.execute("""
    CREATE TABLE training_sample (
        video_id TEXT PRIMARY KEY,
        title TEXT,
        description TEXT,
        transcript TEXT,
        keywords TEXT
    )
    """)

    # Output keyword distribution in sampled data
    sampled_keyword_counts = {keyword: 0 for keyword in keyword_list}
    def count_keywords_sampled(row):
        if row["keywords"]:
            video_keywords = row["keywords"].split(",")
            for keyword in video_keywords:
                keyword = keyword.strip()
                if keyword in sampled_keyword_counts:
                    sampled_keyword_counts[keyword] += 1
    final_sample.apply(count_keywords_sampled, axis=1)
    print("Keyword distribution in final sample:", sampled_keyword_counts)

    final_sample.to_sql("training_sample", conn, if_exists="replace", index=False)
    conn.commit()
    print("Training sample stored in database.")
    final_sample = pd.read_sql_query("SELECT * FROM training_sample", conn)
else:
    print("Training sample already exists, loading from database...")
    final_sample = pd.read_sql_query("SELECT * FROM training_sample", conn)

# Create table for storing only video_id and gpt_prediction if it does not exist
conn.execute("""
CREATE TABLE IF NOT EXISTS gpt4o_training_labels (
    video_id TEXT PRIMARY KEY,
    gpt_label INTEGER
)
""")
conn.commit()

def truncate_text(text, word_limit):
    """Truncate text to a specified word limit."""
    if not text:
        return ""
    words = text.split()
    return " ".join(words[:word_limit])

def call_gpt_4o(title, description, transcript):
    """Call OpenAI GPT-4o API for classification."""
    global total_input_tokens, total_cached_input_tokens, total_output_tokens

    # Truncate description and transcript
    description = truncate_text(description, 150)  # Cap at 150 words
    transcript = truncate_text(transcript, 500)  # Cap at 500 words

    prompt = f"""
        Given the following YouTube video INFORMATION, we are looking to see if it matches our KEYWORD LIST. Reply with “1” if and only if INFORMATION is an “instructional video” on any data system topic that matches KEYWORD LIST. Otherwise, reply “0”.
        Instructional Video Definition: A video is instructional if it is designed to educate, train, or inform viewers by demonstrating a process, explaining a concept, or providing expert insights.  
        Exclusions: Do not consider news reports, marketing/promotional material or legal interpretations/explanations.
        ---- INFORMATION START -----
        Title: {title}
        Description: {description}  
        Video Transcript: {transcript}  
        ---- INFORMATION END -----
        ---- KEYWORD LIST START -----
        ["relational theory", "relational theory relations", "relational theory tuples", "relational theory attributes", 
        "tuple relational calculus", "relational algebra", "data visualization", "database optimization", "database optimization indexing", 
        "database optimization query execution plans", "database optimization query optimization", "database scalability", 
        "database scalability replication", "database scalability sharding", "NoSQL database management systems", "data independence", 
        "logical data independence", "physical data independence", "logical and physical data independence", "database management system components", 
        "functions and stored procedures", "data modeling", "data modeling conceptual modeling", "data modeling mapping conceptual models to logical models", 
        "data modeling creating tables and columns", "database normalization", "database normalization functional dependency", 
        "database normalization candidate", "database normalization super keys", "database normalization normal forms up to BCNF", 
        "database normalization multivalued dependency", "database normalization join dependency", "object-oriented data models", 
        "semi-structured traditional data models", "SQL", "SQL select", "SQL project", "SQL join", "SQL insert", "SQL update", "SQL delete", 
        "SQL aggregation", "SQL group by", "SQL subqueries", "SQL common table expressions", "transaction processing", "concurrency control", 
        "isolation levels", "concurrency control and isolation levels", "database back-ups", "database recovery", "database back-ups and recovery", 
        "distributed database management systems", "data mining", "data mining algorithms", "data mining associative pattern", "data mining sequential pattern", 
        "data mining associative and sequential patterns", "data mining data cleaning", "data mining market basket analysis", "data privacy", "data ethics", 
        "data privacy and ethics", "data security", "database access management", "data security and database access management", "data warehousing"]
        ---- KEYWORD LIST END -----
    """

    # Call OpenAI API
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "You are a helpful AI model that classifies YouTube videos."},
            {"role": "user", "content": prompt}
        ]
    )

    print(type(response))
    print(response)


    # Parse the response
    gpt_label = response.choices[0].message.content.strip()

    return int(gpt_label), input_tokens, output_tokens

# Load existing predictions to avoid reprocessing
existing_predictions = pd.read_sql_query("SELECT video_id FROM gpt4o_training_labels", conn)
processed_videos = set(existing_predictions["video_id"].tolist())

true_labels = []
predicted_labels = []

for i, row in final_sample.iterrows():
    if row["video_id"] in processed_videos:
        continue  # Skip already processed videos
    time.sleep(3)
    gpt_label, input_tokens, output_tokens = call_gpt_4o(
        title=row["title"],
        description=row["description"],
        transcript=row["transcript"]
    )

    conn.execute("""
    INSERT INTO gpt4o_training_labels (video_id, gpt_label)
    VALUES (?, ?)
    ON CONFLICT(video_id) DO NOTHING
    """, (row["video_id"], gpt_label))
    conn.commit()

    print(f"Processed video {i + 1}/{len(final_sample)}...")


# Close the database connection
conn.close()
