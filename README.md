# AI News Intelligence Pipeline (Phase 1 MVP)

This project implements a modular AI news intelligence pipeline for YouTube videos using Python, SQLite, and a local Ollama model.

## Features

- Uses YouTube Data API v3 for video metadata retrieval by search query or channel ID
- Retrieves transcript with fallback priority: manual captions, auto captions, then description
- Cleans and normalizes transcript text with deterministic rules
- Splits text into overlapping chunks before any LLM call
- Summarizes each chunk with Ollama using strict JSON output
- Stores videos, transcripts, chunks, chunk summaries, and video summaries in SQLite
- Supports idempotent re-runs by upserting records and replacing per-video derived chunks safely

## Project Structure

```text
project_root/
├── main.py
├── config.py
├── requirements.txt
├── README.md
├── db/
│   ├── schema.sql
│   └── database.py
├── youtube/
│   ├── fetch_videos.py
│   └── fetch_transcript.py
├── processing/
│   ├── cleaner.py
│   └── chunker.py
├── llm/
│   ├── ollama_client.py
│   └── summarizer.py
├── pipeline/
│   └── pipeline.py
└── outputs/
    ├── export_reader.py
    └── export_notebooklm.py
```

## Setup

1. Install Python 3.10 or newer.
2. Create and activate a virtual environment.
3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Create a YouTube Data API v3 key in Google Cloud and enable the YouTube Data API v3.
5. Set `YOUTUBE_API_KEY`.

You can place it in `.env` at the project root:

```text
YOUTUBE_API_KEY=your_api_key_here
```

PowerShell for the current session:

```powershell
$env:YOUTUBE_API_KEY = "your_api_key_here"
```

Persist it on Windows:

```powershell
setx YOUTUBE_API_KEY "your_api_key_here"
```

6. Install Ollama from the official site:
   [https://ollama.com/download](https://ollama.com/download)

7. Pull the default model:

```bash
ollama pull llama3.2:3b
```

8. Make sure the Ollama local server is running. By default this project calls:

```text
http://localhost:11434/api/generate
```

## Run

Search by query:

```bash
python main.py --query "AI news" --max-videos 1 --video-workers 2 --chunk-workers 2
```

Fast resume without recomputing existing summaries:

```bash
python main.py --query "AI news" --max-videos 3 --resume-only-missing
```

Skip any video already stored in SQLite:

```bash
python main.py --query "AI news" --max-videos 3 --skip-existing-videos
```

Skip LLM calls entirely:

```bash
python main.py --query "AI news" --max-videos 3 --skip-llm
```

Fetch from a channel:

```bash
python main.py --channel-id "UC_x5XG1OV2P6uZZ5FSM9Ttw" --max-videos 2 --video-workers 2 --chunk-workers 2
```

Export the reader-oriented output:

```bash
python main.py --query "AI news" --max-videos 2 --resume-only-missing --export-reader-markdown
```

Export the NotebookLM pack:

```bash
python main.py --query "AI news" --max-videos 2 --resume-only-missing --export-notebooklm-json --export-notebooklm-markdown
```

NotebookLM export format:

- JSON writes a stable `schema_version` payload into `outputs/notebooklm/`.
- Each video is exported as one document with `video`, `retrieval`, `summary`, `analysis`, and `evidence` sections.
- Markdown mirrors the same structure for direct NotebookLM ingestion: source metadata, retrieval status, aggregated evidence, chunk evidence, and cleaned transcript.
- Metadata-only unavailable videos stay in the pack with explicit warnings and empty evidence lists rather than fabricated content.

## Notes

- `YOUTUBE_API_KEY` is required for query and channel-based video retrieval.
- The pipeline never sends full raw transcript text to the LLM.
- Text is always cleaned and chunked before summarization.
- Chunk summaries are stored as structured JSON-derived fields in SQLite.
- Video summaries are aggregated in Python without a second LLM call.
- Multi-video and multi-chunk execution can be parallelized with CLI worker flags.
- `--resume-only-missing` reuses stored transcripts, chunks, and summaries when available.
- `--skip-llm` avoids Ollama calls for fast metadata/transcript ingestion runs.
- `--skip-existing-videos` avoids reprocessing videos already present in the database.
- `--export-reader-json` and `--export-reader-markdown` write a concise human-readable digest into `outputs/reader/`.
- `--export-notebooklm-json` and `--export-notebooklm-markdown` write a stable knowledge-pack format into `outputs/notebooklm/`.
- SQLite database file is created under `data/news_intelligence.db`.

## Handling Videos Without Retrievable Content

Some YouTube videos do not expose usable captions and also do not provide a usable description. In that case the pipeline does not drop the video from the run.

- The video is retained in SQLite as a metadata-only record so search/query coverage is preserved.
- `transcript_source` remains `none` when neither captions nor fallback description can be retrieved.
- `content_status` is set to `unavailable` and `content_warning` explains that transcript and description could not be retrieved.
- The pipeline does not fabricate chunks, extracted entities, or summary points from missing content.
- A fixed metadata-only summary is stored instead so CLI and exports can clearly distinguish these records from normal summaries.
- Reader and NotebookLM exports include these videos with the warning attached, rather than silently excluding them.
- This keeps reruns idempotent and allows future reprocessing if transcript acquisition improves later.

In practice, treat `content_status=unavailable` as "tracked but not analyzable yet." These records are useful for auditability, gap tracking, and deciding whether to retry collection later.
