# AI News Intelligence Pipeline

YouTube 中心で始まったローカル AI 情報収集パイプラインを、source 共通モデルへ段階移行するためのプロジェクトです。全面刷新は行わず、既存 YouTube フローを活かしたまま、`collect / migrate / analyze / export` の分離、canonical item、quality gate、Reader / NotebookLM の出力ポリシーを追加しています。

## 設計方針

- 既存 YouTube パイプラインは当面残し、破壊的変更を避ける
- source 共通の `items` 系テーブルを追加し、旧 `videos` 系テーブルは当面併存させる
- transcript をそのまま LLM に送らず、cleaning と chunking の後に JSON 要約する
- `metadata_only` と `unavailable` は削除せず保持する
- NotebookLM と Reader は同じ扱いにせず、policy で出し分ける
- idempotent 保存、resume / skip / retry の考え方を維持する

## 主な構成

```text
project_root/
  main.py
  config.py
  config/
    sources.yaml
    policies.yaml
  db/
    schema.sql
    database.py
    repository.py
  sources/
    base.py
    youtube/provider.py
  normalization/
    canonicalize.py
  evaluation/
    quality.py
    notebooklm_policy.py
    reader_policy.py
  pipeline/
    pipeline.py
    collect.py
    migrate.py
    analyze.py
    export.py
    metadata_only_report.py
  processing/
    cleaner.py
    chunker.py
  llm/
    ollama_client.py
    summarizer.py
  outputs/
    export_reader.py
    export_notebooklm.py
```

## セットアップ

1. Python 3.10+ を用意する
2. 必要なら仮想環境を作る
3. 依存を入れる

```bash
pip install -r requirements.txt
```

4. `.env` または環境変数で `YOUTUBE_API_KEY` を設定する

```text
YOUTUBE_API_KEY=your_api_key_here
```

5. Ollama を起動し、既定モデルを取得する

```bash
ollama pull llama3.2:3b
```

## 推奨実行順

items 系を主分析対象として使う場合は、次の順序を推奨します。

1. 旧運用データがあるなら `migrate --backfill-items-from-videos --dry-run`
2. 問題なければ `migrate --backfill-items-from-videos`
3. 新規取得は `collect`
4. quality と summary 反映は `analyze`
5. 利用先に応じて `export`

本格 provider 追加の前に migrate を一度実行して、旧 `videos` 系データを `items` 系へそろえることを推奨します。

## 新CLI

### 1. collect

source provider から canonical item を収集し、`items` / `item_contents` に保存します。現時点の provider 実装は `youtube.default` のみです。

```bash
python main.py collect --source youtube.default --query "AI news" --max-items 10
python main.py collect --source youtube.default --channel-id "UC_x5XG1OV2P6uZZ5FSM9Ttw" --max-items 5
```

### 2. migrate

旧 `videos / transcripts / chunks / chunk_summaries / video_summaries` 系から、`items / item_contents / item_chunks / item_chunk_summaries / item_summaries` 系へ安全にバックフィルします。

```bash
python main.py migrate --backfill-items-from-videos
python main.py migrate --backfill-items-from-videos --only-missing
python main.py migrate --backfill-items-from-videos --dry-run
```

`dry-run` は件数見積りだけを返し、実データは更新しません。実行結果サマリでは次を表示します。

- `scanned`
- `created`
- `skipped_existing`
- `warning_count`
- `error_count`

### 3. analyze

保存済み item に対して quality 判定、chunking、chunk summary、item summary を実行します。`content_status != available` の item から evidence を捏造しません。

```bash
python main.py analyze --item-source youtube.default --only-missing
python main.py analyze --item-source youtube.default --retry-ineligible
python main.py analyze --item-source youtube.default --retry-low-quality
python main.py analyze --item-source youtube.default --only-missing --skip-llm
```

`--only-missing` は item 基準で次を見ます。

- `item_contents` が欠けている
- `cleaned_text` が空
- `item_chunks` がない
- `item_summaries` がない
- quality 評価が未実施

### 4. export

Reader と NotebookLM を別 policy で出力します。export はまず `items` 系を参照し、items が存在しない legacy video のみ fallback します。

```bash
python main.py export --format reader
python main.py export --format reader-json
python main.py export --format notebooklm-json
python main.py export --format notebooklm-markdown
```

## 旧CLI互換

従来の YouTube 専用 CLI も当面維持しています。サブコマンドを使わない legacy 実行でも、既存 `videos` 系を更新しつつ、新しい `items` 系にも同期します。

```bash
python main.py --query "AI news" --max-videos 3 --resume-only-missing
python main.py --channel-id "UC_x5XG1OV2P6uZZ5FSM9Ttw" --max-videos 2 --skip-llm
python main.py --report-metadata-only
python main.py --retry-metadata-only --max-videos 5 --resume-only-missing
```

## canonical item モデル

収集結果は次の考え方で canonical item に正規化されます。

- `source_id`
- `source_type`
- `external_id`
- `title`
- `author`
- `published_at`
- `url`
- `raw_text`
- `cleaned_text`
- `body_kind`
- `content_status`
- `content_warning`
- `retrieval_diagnostics`
- `language`
- `trust_level`
- `evidence_strength`

### `body_kind`

- `full_text`
- `partial_text`
- `description_only`
- `post_text`
- `metadata_only`

### `evidence_strength`

- `strong`
- `medium`
- `weak`
- `none`

`description_only` は transcript 成功と同列に扱わず、`metadata_only` は保持するが NotebookLM 本体には原則入れません。

## Quality / Policy

`config/policies.yaml` に JSON-compatible YAML として policy を置いています。

### quality 評価

- `quality_tier`: `high / medium / low / reject`
- `reader_eligibility`: `eligible / eligible_with_warning / ineligible`
- `notebooklm_eligibility`: `eligible / eligible_with_warning / ineligible`

判定観点:

- retrieval quality: 本文取得可否、重大 diagnostics、文字化け率
- content quality: URL 比率、CTA/販促語比率、本文長
- knowledge utility: `body_kind` と `content_status`

### NotebookLM policy

- `notebooklm-pack.v1` を維持
- `metadata_only` と `unavailable` は本体から除外
- `description_only` は conditional 扱い
- 低品質 item を無条件で混ぜない
- 欠損 item から chunks / entities / key points を生成しない

### Reader policy

- NotebookLM より広く出す
- `metadata_only` / `description_only` / 低品質 item に warning を付ける
- `reader_warning_flags` で警告理由を残す

## DB / Migration

既存テーブルは削除していません。今回の追加は安全な append-only です。旧テーブルと新テーブルは当面併存します。

### 既存テーブル

- `videos`
- `transcripts`
- `chunks`
- `chunk_summaries`
- `video_summaries`

### 新規テーブル

- `sources`
- `items`
- `item_contents`
- `item_chunks`
- `item_chunk_summaries`
- `item_summaries`

初回起動時に [db/schema.sql](/C:/Users/shunyjp/OneDrive/Documents/検証勉強/newsfeed1/db/schema.sql) が適用されます。`migrate --backfill-items-from-videos` は旧系を削除せずに items 系へ安全に複製する補助コマンドです。

## Export の扱い

### Reader

- metadata-only item も warning 付きで残せる
- `reader_warning_flags` に body kind / quality / unavailable などの理由を残す
- まず items 系を読み、items 不在の legacy video のみ fallback する

### NotebookLM

- 出力フォーマットは `notebooklm-pack.v1`
- policy 上不適格な item は pack 本体から除外する
- evidence がない item に対して evidence を補完しない
- まず items 系を読み、items 不在時のみ legacy fallback を使う

## テスト

標準ライブラリの `unittest` で回せます。

```bash
python -m unittest discover -s tests -v
```

今回追加した確認項目:

- item の idempotent 保存
- canonicalize の変換
- migrate dry-run の件数見積り
- migrate 実行と idempotent 再実行
- description fallback の `description_only` 移行
- metadata-only の NotebookLM 不適格移行
- `description_only` の quality 判定
- `metadata_only` の NotebookLM 除外
- `analyze --only-missing` の item 欠損判定
- Reader export で warning 付きで残ること
- NotebookLM export で不適格 item が除外されること
- items 系優先 export と legacy fallback
- 既存 metadata-only / retry 系の基本挙動

## 制約と保留

- source registry は導入済みだが、provider 実装は現時点では `youtube.default` のみ
- `config/*.yaml` は依存追加を避けるため JSON-compatible YAML で管理している
- backfill は安全側マッピングを優先しており、legacy 情報にない強い意味付けは追加しない
- retry policy の外部設定化は今後の拡張余地がある
- `text` source provider、外部ニュース source、ASR、ベクトル DB は今回未実装
## Operational Start

`production_min` source set is the minimum operations preset. It limits the initial scope to official blogs, technical docs, and Nikkei xTECH candidate collection so one flaky source is less likely to stop the whole run.

Current article-body behavior:
- `blog.openai` and `docs.python_insider` try public article-body extraction first and fall back to `description_only` when only a public summary is available.
- `nikkei.xtech.candidate` tries public HTML extraction first and keeps `metadata_only` if no public or authenticated body can be retrieved.
- Explicit PR-style titles such as `PR`, `【PR】`, `Advertorial`, and `Sponsored` are excluded during candidate collection.

One-off run:
```bash
python main.py collect --source-set production_min --max-items 3
python main.py analyze --source-set production_min --only-missing --skip-llm --report-file reports/manual/analyze-production_min.json
python main.py export --source-set production_min --format notebooklm-json --compare
```

Dry-run style checks:
```bash
python main.py migrate --backfill-items-from-videos --dry-run
python main.py analyze --source-set production_min --only-missing --skip-llm
```

Daily operation order:
```bash
python main.py collect --source-set production_min --max-items 5
python main.py analyze --source-set production_min --only-missing --report-file reports/manual/analyze-production_min.json
python main.py export --source-set production_min --format reader-json
python main.py export --source-set production_min --format notebooklm-json --compare
```

Operational artifacts are stored under `reports/YYYY-MM-DD/collect`, `reports/YYYY-MM-DD/analyze`, and `reports/YYYY-MM-DD/export`. A latest copy is also written under `reports/latest/`.
CLI output now prints both the dated artifact/report path and the corresponding `reports/latest/...` copy path.

## Analyze Report Notes

- `retry_success_definition = quality_tier_improved`
  A retry counts as success only when the resulting `quality_tier` is better than the previous one.
- `blocked_reason_counts`
  Counts retry candidates blocked by retry policy gates such as max retries reached, cooldown active, or override disabled.
- `source_retry_distribution`
  Per-source view of analyzed items, retry candidates, and executed retries. If candidates are high but executed retries stay low, that source is usually being held back by policy gates or repeated low-quality outcomes.
