PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS videos (
    video_id TEXT PRIMARY KEY,
    title TEXT,
    channel TEXT,
    published_at TEXT,
    url TEXT,
    description TEXT,
    transcript_source TEXT,
    transcript_length INTEGER,
    content_status TEXT,
    content_warning TEXT,
    metadata_only_reason TEXT,
    retrieval_diagnostics TEXT
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

CREATE TABLE IF NOT EXISTS sources (
    source_id TEXT PRIMARY KEY,
    source_name TEXT NOT NULL,
    source_type TEXT NOT NULL,
    base_url TEXT,
    collection_method TEXT NOT NULL,
    cadence_type TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 100,
    trust_level TEXT NOT NULL,
    notebooklm_default_policy TEXT NOT NULL,
    reader_default_policy TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS items (
    item_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    source_type TEXT NOT NULL,
    external_id TEXT,
    title TEXT,
    author TEXT,
    published_at TEXT,
    url TEXT NOT NULL,
    body_kind TEXT NOT NULL,
    content_status TEXT NOT NULL,
    content_warning TEXT,
    retrieval_diagnostics TEXT,
    language TEXT,
    trust_level TEXT,
    evidence_strength TEXT NOT NULL,
    quality_tier TEXT,
    reader_eligibility TEXT,
    notebooklm_eligibility TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(source_type, external_id),
    FOREIGN KEY (source_id) REFERENCES sources(source_id)
);

CREATE TABLE IF NOT EXISTS item_contents (
    item_id TEXT PRIMARY KEY,
    raw_text TEXT,
    cleaned_text TEXT,
    cleaning_version TEXT,
    cleaning_diagnostics TEXT,
    FOREIGN KEY (item_id) REFERENCES items(item_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS item_chunks (
    chunk_id TEXT PRIMARY KEY,
    item_id TEXT NOT NULL,
    chunk_no INTEGER NOT NULL,
    text TEXT NOT NULL,
    UNIQUE(item_id, chunk_no),
    FOREIGN KEY (item_id) REFERENCES items(item_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS item_chunk_summaries (
    chunk_id TEXT PRIMARY KEY,
    summary TEXT,
    key_points_json TEXT,
    entities_json TEXT,
    category_json TEXT,
    signal_score REAL,
    FOREIGN KEY (chunk_id) REFERENCES item_chunks(chunk_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS item_summaries (
    item_id TEXT PRIMARY KEY,
    short_summary TEXT,
    detailed_summary TEXT,
    summary_version TEXT,
    FOREIGN KEY (item_id) REFERENCES items(item_id) ON DELETE CASCADE
);
