PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS videos (
    video_id TEXT PRIMARY KEY,
    title TEXT,
    channel TEXT,
    published_at TEXT,
    url TEXT,
    description TEXT,
    transcript_source TEXT,
    transcript_length INTEGER
);

CREATE TABLE IF NOT EXISTS transcripts (
    video_id TEXT PRIMARY KEY,
    raw_text TEXT,
    cleaned_text TEXT,
    FOREIGN KEY (video_id) REFERENCES videos(video_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS chunks (
    chunk_id TEXT PRIMARY KEY,
    video_id TEXT,
    chunk_no INTEGER,
    text TEXT,
    UNIQUE(video_id, chunk_no),
    FOREIGN KEY (video_id) REFERENCES videos(video_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS chunk_summaries (
    chunk_id TEXT PRIMARY KEY,
    summary TEXT,
    key_points TEXT,
    entities TEXT,
    category TEXT,
    signal_score REAL,
    FOREIGN KEY (chunk_id) REFERENCES chunks(chunk_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS video_summaries (
    video_id TEXT PRIMARY KEY,
    short_summary TEXT,
    detailed_summary TEXT,
    FOREIGN KEY (video_id) REFERENCES videos(video_id) ON DELETE CASCADE
);
