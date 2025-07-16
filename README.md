# Educational Content on YouTube: The Case of Data Systems

This repository contains the core scripts and data used in the MSc thesis project on the analysis of educational YouTube videos in the domain of data systems, including multi-level data scraping, relevance filtering, and SQL subtopic classification using large language models.

## Repository Overview

### `scrapers/`
This folder includes all scripts for collecting data from YouTube using the YouTube Data API:

- `scrape_videos.py`: Collects video-level metadata based on predefined search queries.
- `scrape_channels.py`: Retrieves channel-level metadata for each video.
- `scrape_comments.py`: Downloads top-level comments and their replies.
- `scrape_transcripts.py`: Fetches available English transcripts for each video.
- `supplement_transcripts.py`: Attempts to retrieve translated English transcripts for non-English videos using YouTube’s caption translation functionality.

All data is stored in a structured **SQLite database**, which is available via [OSF](https://doi.org/10.17605/OSF.IO/FTN2S).
### `filtering/`
Scripts related to dataset relevance filtering:

- `gpt_classifier_for_training_data.py`: Uses GPT-4o to label a training set of videos as relevant or irrelevant to data systems education.
- `embedding_gte-Qwen2-7B-instruct.ipynb` ([Colab Link](https://colab.research.google.com/drive/1KoGi1imRf9sWOe_OrlZ9uZVQ_kWNC1wC?usp=sharing)): Encodes structured text (title, description, transcript keywords) for each video using the selected instruction-tuned embedding model `gte-Qwen2-7B-instruct`.
- `classification_gte-Qwen2-7B-instruct.ipynb`: Trains and evaluates classifiers (e.g., XGBoost) using the generated embeddings and both GPT-labeled and manually annotated relevance labels, then predicts relevance for the rest of the dataset.

### `sql_subtopics_classification/`
Resources and outputs for textbook-based SQL subtopic classification:

- `eval_data.json`: Contains manually inspected samples of textbook passages and their corresponding LLM classification results for evaluation.
- `sql_subtopics_classification_results_qwen3.jsonl`: Contains the full set of LLM-generated subtopic classification results for all SQL-related YouTube videos.

Associated Colab notebooks:

- `prompt_integration.ipynb` ([Colab Link](https://colab.research.google.com/drive/17t-URq0vzV0T3nn5cMtzmeCPhecEWJCy?usp=sharing)): Combines each video’s textual information with a prompt template for LLM-based classification input.

- `classification_Qwen3-8B.ipynb` ([Colab Link](https://colab.research.google.com/drive/1cgV7WK8w4nRAJX6wmGTC4_ZC5IzmQe0T?usp=sharing)): Uses Qwen3-8B to classify SQL videos into textbook-derived subtopics using the generated prompts.
