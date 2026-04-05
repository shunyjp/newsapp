import OpenAI from "openai";

const configuredSummaryProvider = (process.env.SUMMARY_PROVIDER || "auto").toLowerCase();
const openaiSummaryModel = process.env.OPENAI_SUMMARY_MODEL || "gpt-5";
const geminiSummaryModel = process.env.GEMINI_SUMMARY_MODEL || "gemini-2.5-flash";
const ttsModel = process.env.OPENAI_TTS_MODEL || "gpt-4o-mini-tts";
const translationLimit = Number.parseInt(process.env.TRANSLATION_LIMIT || "12", 10);

function getOpenAIClient() {
  return process.env.OPENAI_API_KEY ? new OpenAI({ apiKey: process.env.OPENAI_API_KEY }) : null;
}

function getGeminiApiKey() {
  return process.env.GEMINI_API_KEY || "";
}

function hasGemini() {
  return Boolean(getGeminiApiKey());
}

function getProviderOrder() {
  const hasOpenAI = Boolean(process.env.OPENAI_API_KEY);
  const hasGeminiProvider = hasGemini();

  if (configuredSummaryProvider === "openai") {
    return hasOpenAI ? ["openai"] : hasGeminiProvider ? ["gemini"] : [];
  }

  if (configuredSummaryProvider === "gemini" || configuredSummaryProvider === "anthropic") {
    return hasGeminiProvider ? ["gemini"] : hasOpenAI ? ["openai"] : [];
  }

  const order = [];
  if (hasGeminiProvider) {
    order.push("gemini");
  }
  if (hasOpenAI) {
    order.push("openai");
  }
  return order;
}

function containsJapanese(text = "") {
  return /[\u3040-\u30ff\u3400-\u9fff]/.test(text);
}

function needsJapaneseTranslation(text = "") {
  return Boolean(text) && !containsJapanese(text);
}

function buildDigestPrompt(newsData) {
  const articleLines = newsData.articles.slice(0, 15).map((article, index) => {
    return [
      `${index + 1}. [${article.topicLabel}] ${article.title}`,
      `source: ${article.source || "unknown"}`,
      `source_type: ${article.sourceType || "unknown"}`,
      `trust_score: ${article.trustScore ?? 0}`,
      `date: ${article.pubDate || "unknown"}`,
      `summary: ${article.contentSnippet || "n/a"}`,
      `link: ${article.link}`
    ].join("\n");
  });

  const viewpointBlock = newsData.viewpoints
    .map((viewpoint) => `- ${viewpoint.label}: ${viewpoint.hint}`)
    .join("\n");

  return [
    "You are writing a Japanese business news digest.",
    "Use only the supplied articles.",
    "Do not invent facts.",
    "Write a practical digest for business readers.",
    "Structure the answer with clear sections and bullet points.",
    "",
    "Viewpoints:",
    viewpointBlock,
    "",
    "Articles:",
    articleLines.join("\n\n")
  ].join("\n");
}

function buildNotebookLmResearchPrompt(newsData) {
  const articleLines = newsData.articles.slice(0, 20).map((article, index) => {
    return [
      `${index + 1}. [${article.topicLabel}] ${article.title}`,
      `source: ${article.source || "unknown"}`,
      `source_type: ${article.sourceType || "unknown"}`,
      `region: ${article.region || "unknown"}`,
      `trust_score: ${article.trustScore ?? 0}`,
      `date: ${article.pubDate || "unknown"}`,
      `summary: ${(article.contentSnippet || "n/a").slice(0, 500)}`,
      `link: ${article.link}`
    ].join("\n");
  });

  return [
    "Return exactly one JSON object in Japanese. Do not output Markdown. Do not use code fences.",
    "Use only the supplied articles and metadata.",
    "Do not invent facts, dates, companies, or relationships.",
    "If evidence is thin, say so explicitly.",
    "Every string value must be complete and self-contained.",
    "Every array item must be complete. No empty bullets.",
    "",
    "Required JSON shape:",
    "{",
    '  "coverage_note": ["...", "..."],',
    '  "last_7_days_japan": [{"title":"...","what_happened":"...","why_it_matters":"...","sources":["1","3"]}],',
    '  "last_7_days_us": [{"title":"...","what_happened":"...","why_it_matters":"...","sources":["2"]}],',
    '  "sap_relevance": ["...", "..."],',
    '  "core_systems_relevance": ["...", "..."],',
    '  "sier_terms_to_know": [{"term":"...","meaning":"...","why_sier_should_care":"..."}],',
    '  "terms_and_background": [{"term":"...","meaning":"...","technical_background":"..."}],',
    '  "last_month_context": ["...", "..."],',
    '  "notebooklm_reading_order": ["...", "..."]',
    "}",
    "",
    "Constraints:",
    "- last_7_days_japan: 3 to 6 items when possible",
    "- last_7_days_us: 3 to 6 items when possible",
    "- sap_relevance: 2 to 5 bullets",
    "- core_systems_relevance: 2 to 5 bullets",
    "- sier_terms_to_know: 3 to 8 items",
    "- terms_and_background: 3 to 8 items",
    "- last_month_context: 3 to 6 bullets",
    "- notebooklm_reading_order: 5 to 10 bullets",
    "- sources must reference the numbered article ids from the supplied list",
    "",
    "Articles:",
    articleLines.join("\n\n")
  ].join("\n");
}

function buildNotebookLmArticleEvidence(newsData, limit = 20) {
  return newsData.articles.slice(0, limit).map((article, index) => {
    return [
      `${index + 1}. [${article.topicLabel}] ${article.title}`,
      `source: ${article.source || "unknown"}`,
      `source_type: ${article.sourceType || "unknown"}`,
      `date: ${article.pubDate || "unknown"}`,
      `summary: ${(article.contentSnippet || "n/a").slice(0, 500)}`,
      `link: ${article.link}`
    ].join("\n");
  });
}

function buildNotebookLmSierTermsPrompt(newsData) {
  const articleLines = buildNotebookLmArticleEvidence(newsData, 20);

  return [
    "Return exactly one JSON object in Japanese. Do not output Markdown. Do not use code fences.",
    "Focus only on practical terms and concepts that a Japanese SIer should understand from the supplied articles.",
    "Do not list company names, people names, slogans, or vague business words.",
    "Prefer architecture, integration, migration, operations, security, data, ERP, SAP, cloud, and AI implementation terms.",
    "If evidence is thin, return fewer items rather than generic filler.",
    "",
    "Required JSON shape:",
    "{",
    '  "sier_terms_to_know": [{"term":"...","meaning":"...","why_sier_should_care":"...","sources":["1","3"]}],',
    '  "terms_and_background": [{"term":"...","meaning":"...","technical_background":"...","sources":["2"]}],',
    '  "last_month_context": ["...", "..."]',
    "}",
    "",
    "Constraints:",
    "- sier_terms_to_know: 3 to 6 items when possible",
    "- terms_and_background: 3 to 6 items when possible",
    "- last_month_context: 2 to 4 bullets",
    "- sources must reference the numbered article ids from the supplied list",
    "",
    "Articles:",
    articleLines.join("\n\n")
  ].join("\n");
}

const SIER_TERM_CATALOG = [
  {
    term: "LLM",
    patterns: [/\bLLM\b/i, /large language model/i, /大規模言語モデル/],
    meaning: "大量の文書を学習し、要約・生成・分類・対話を行う基盤モデルです。",
    whySierShouldCare: "業務アプリへの組み込み、社内文書検索、問い合わせ自動化、既存システム連携の設計判断に直結します。"
  },
  {
    term: "RAG",
    patterns: [/\bRAG\b/i, /retrieval-augmented generation/i, /検索拡張生成/],
    meaning: "外部データベースや文書検索と生成AIを組み合わせ、根拠付きで回答させる構成です。",
    whySierShouldCare: "社内ナレッジ活用、FAQ、保守マニュアル検索、権限制御付きAI導入の中心パターンだからです。"
  },
  {
    term: "AI Agent",
    patterns: [/\bagent\b/i, /ai agent/i, /エージェント/],
    meaning: "複数の手順を自律的に実行し、ツールや外部APIも使いながら業務を進めるAI実装です。",
    whySierShouldCare: "運用自動化、調査補助、ワークフロー処理、SaaS連携の新しい実装単位になるためです。"
  },
  {
    term: "MCP",
    patterns: [/\bMCP\b/i, /model context protocol/i],
    meaning: "AIと外部ツールやデータソースを安全に接続するための連携方式です。",
    whySierShouldCare: "AIを既存システム、社内DB、業務APIへつなぐ接続標準として設計に影響するためです。"
  },
  {
    term: "ERP",
    patterns: [/\bERP\b/i, /基幹システム/, /enterprise resource planning/i],
    meaning: "会計、販売、購買、生産などを統合管理する企業基幹システムです。",
    whySierShouldCare: "AI活用でも最終的な効果はERPや周辺業務への接続で決まるため、影響範囲の理解が重要です。"
  },
  {
    term: "S/4HANA",
    patterns: [/s\/4hana/i, /s4hana/i],
    meaning: "SAPの中核ERP製品で、会計やサプライチェーンなどの基幹業務を統合します。",
    whySierShouldCare: "SAP移行案件やAI活用の実装先として頻出で、周辺システムとの連携設計が重要だからです。"
  },
  {
    term: "SAP BTP",
    patterns: [/\bBTP\b/i, /sap business technology platform/i, /sap btp/i],
    meaning: "SAP拡張、統合、データ連携、AI活用を支えるプラットフォーム群です。",
    whySierShouldCare: "SAP標準を壊さずに拡張や連携を設計する際の有力な実装基盤になるためです。"
  },
  {
    term: "Data Center",
    patterns: [/data center/i, /データセンター/],
    meaning: "計算資源、ストレージ、ネットワーク設備を収容する基盤施設です。",
    whySierShouldCare: "生成AIでは電力、GPU、リージョン、可用性の制約がアーキテクチャ選定に直結するためです。"
  },
  {
    term: "GPU",
    patterns: [/\bGPU\b/i],
    meaning: "AI学習や推論で大量並列計算を担う主要な計算資源です。",
    whySierShouldCare: "性能、コスト、供給制約、配置場所がAI案件の実現性を左右するためです。"
  },
  {
    term: "Vector Database",
    patterns: [/vector database/i, /ベクトルデータベース/, /embedding/i, /埋め込み/],
    meaning: "文書や画像をベクトル化して近い情報を高速検索するためのデータ基盤です。",
    whySierShouldCare: "RAGや社内検索の精度、更新設計、アクセス制御の設計ポイントになるためです。"
  },
  {
    term: "Inference",
    patterns: [/inference/i, /推論/],
    meaning: "学習済みモデルに入力を与えて実際の出力を得る処理です。",
    whySierShouldCare: "本番運用コスト、応答速度、スケーリング設計に直接影響するためです。"
  },
  {
    term: "Fine-tuning",
    patterns: [/fine-tun/i, /ファインチューニング/],
    meaning: "既存モデルを自社データや特定業務向けに追加学習して調整する手法です。",
    whySierShouldCare: "RAGで足りるのか、追加学習が必要なのかを見極める設計判断に関わるためです。"
  },
  {
    term: "Prompt Injection",
    patterns: [/prompt injection/i, /プロンプトインジェクション/],
    meaning: "外部入力によってAIの指示系統を乱し、意図しない出力や行動を誘発する攻撃です。",
    whySierShouldCare: "社内文書検索やエージェント運用では、権限逸脱や情報漏えい防止の設計が必須だからです。"
  },
  {
    term: "Zero Trust",
    patterns: [/zero trust/i, /ゼロトラスト/],
    meaning: "境界内外を問わず、常に認証・認可を前提にするセキュリティ考え方です。",
    whySierShouldCare: "AIと社内データやSaaSをつなぐ際のアクセス設計、監査設計の土台になるためです。"
  },
  {
    term: "Multimodal",
    patterns: [/multimodal/i, /マルチモーダル/],
    meaning: "テキストだけでなく画像、音声、動画など複数形式を扱えるAI能力です。",
    whySierShouldCare: "帳票、図面、音声記録、現場画像などを含む業務システムへの適用範囲が広がるためです。"
  },
  {
    term: "API Integration",
    patterns: [/\bAPI\b/i, /api integration/i, /連携API/, /システム連携/],
    meaning: "アプリケーション同士をインターフェース経由で接続する基本手法です。",
    whySierShouldCare: "AI単体では価値が出にくく、既存業務システムとの接続設計が成果を左右するためです。"
  },
  {
    term: "Data Governance",
    patterns: [/data governance/i, /データガバナンス/, /データ品質/],
    meaning: "データ品質、管理責任、利用ルール、権限統制を定める運用と統制の考え方です。",
    whySierShouldCare: "AIに与えるデータの品質と権限管理が、そのまま回答品質とリスクに跳ね返るためです。"
  }
];

function buildArticleEvidenceSummary(article) {
  const summary = (article.contentSnippet || article.notebooklmText || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 180);
  return summary || (article.title || "No summary available.");
}

const TERM_TECHNICAL_BACKGROUND = {
  "LLM":
    "技術的には、Transformer 系アーキテクチャを用いた大規模事前学習モデルを指します。SIer 観点では、モデルサイズそのものよりも、コンテキスト長、ツール呼び出し、権限制御、監査、応答速度、運用コストをどう設計に落とし込むかが重要です。",
  "RAG":
    "技術的には、文書分割、埋め込み生成、ベクトル検索、再ランキング、プロンプト合成から成ります。SIer にとっては、検索精度より前に、文書更新頻度、権限境界、マスタデータとの整合、回答根拠の監査可能性をどう担保するかが設計論点になります。",
  "AI Agent":
    "技術的には、計画、状態管理、ツール実行、エラー回復、再試行、監査ログを持つ実行ユニットとして捉えると理解しやすいです。PoC では動いても、本番では権限分離、停止条件、人間承認、失敗時のロールバック設計が必要になります。",
  "MCP":
    "技術的には、AI が外部ツールやデータソースを共通形式で呼び出すための接続モデルです。SIer 観点では、個別 API 連携を都度作り込むより、接続方式の標準化、認可、監査、接続先差し替えをしやすくする土台として見ると価値が分かりやすいです。",
  "ERP":
    "技術的には、会計・販売・購買・在庫・生産などの整合性を保つトランザクション中心の基幹領域です。AI 活用では、ERP の外にAIを置くだけでは足りず、マスタ、業務権限、承認フロー、更新責任を壊さずにどう接続するかが本質です。",
  "S/4HANA":
    "技術的には、SAP の中核 ERP であり、業務プロセス標準化とリアルタイム処理を前提にした設計です。SIer にとっては、アドオン抑制、BTP 連携、周辺システム切り分け、データ責任分界が重要な論点になります。",
  "SAP BTP":
    "技術的には、SAP 拡張、統合、データ活用、AI 利用を支えるプラットフォーム群です。SIer 観点では、S/4HANA 本体を過度に改修せず、拡張を外出ししながら統制を保つための実装基盤として理解すると使いどころが整理しやすいです。",
  "Data Center":
    "技術的には、GPU、電力、冷却、バックボーン、リージョン配置、冗長化の制約が一体になった物理基盤です。AI 案件では、クラウド選定だけでなく、推論配置、データ所在地、災対、レイテンシ、電力制約まで含めて考える必要があります。",
  "GPU":
    "技術的には、学習と推論の性能を左右する並列計算資源です。SIer にとっては、どのモデルをどのレイテンシで、どのコスト帯で回すかという設計判断に直結し、PoC と本番の差が最も出やすい論点の一つです。",
  "Vector Database":
    "技術的には、埋め込みベクトルを使って意味検索を行うためのストアです。単に導入するだけでは不十分で、チャンク設計、更新タイミング、メタデータ設計、アクセス制御、再ランキングとセットで考える必要があります。",
  "Inference":
    "技術的には、学習済みモデルに対する本番実行処理です。SIer 観点では、スループット、レイテンシ、同時実行数、キャッシュ、モデル切り替え、監査ログまで含めて運用設計する必要があります。",
  "Fine-tuning":
    "技術的には、基盤モデルに追加学習をかけて特定業務へ寄せる手法です。多くの案件ではまず RAG やプロンプト設計で十分かを見極め、その上で教師データ準備、評価指標、再学習運用を回せるかを判断するのが実務的です。",
  "Prompt Injection":
    "技術的には、外部入力がモデルの指示系統を上書きしようとする攻撃です。検索連携やエージェント型の構成では特に重要で、入力分離、ツール権限制御、出力検証、監査ログが防御の中心になります。",
  "Zero Trust":
    "技術的には、通信経路や社内外を問わず毎回認証・認可を確認する考え方です。AI を社内データや SaaS とつなぐ場合、ネットワーク境界だけでは足りず、ユーザー、アプリ、データ単位での制御が必要になります.",
  "Multimodal":
    "技術的には、テキストだけでなく画像、音声、動画など複数形式の入力を統合して扱うモデル能力です。SIer 観点では、帳票、現場写真、会議音声、図面のような既存非構造データをどう業務フローに組み込むかが価値になります。",
  "API Integration":
    "技術的には、システム間をインターフェースで接続し、データと処理を連携する基本構造です。AI 案件では、単発のチャット体験より、業務 API とつないで起票、照会、更新までやり切れるかが実装価値を左右します。",
  "Data Governance":
    "技術的には、データ品質、責任分界、変更管理、アクセス権、保持期間を統制する仕組みです。生成AIでは、どのデータをどこまで使ってよいかがそのまま品質とリスクに直結するため、最初から設計に含める必要があります。"
};

function collectSierFallbackTerms(newsData, limit = 6) {
  const articles = newsData.articles.slice(0, 20);
  const scored = SIER_TERM_CATALOG.map((entry) => {
    const evidence = articles
      .map((article, index) => {
        const text = `${article.title || ""}\n${article.contentSnippet || ""}\n${article.notebooklmText || ""}`;
        if (!entry.patterns.some((pattern) => pattern.test(text))) {
          return null;
        }

        return {
          sourceId: `${index + 1}`,
          title: article.title || "Untitled article",
          source: article.source || "unknown",
          topicLabel: article.topicLabel || "unknown",
          summary: buildArticleEvidenceSummary(article)
        };
      })
      .filter(Boolean);

    return {
      ...entry,
      evidence,
      score: evidence.length
    };
  })
    .filter((entry) => entry.score > 0)
    .sort((left, right) => right.score - left.score || left.term.localeCompare(right.term))
    .slice(0, limit)
    .map((entry) => {
      const primary = entry.evidence[0];
      return {
        term: entry.term,
        meaning: entry.meaning,
        whySierShouldCare: primary
          ? `${entry.whySierShouldCare} 今回の収集では「${primary.title}」(${primary.source}) が具体例になっています。`
          : entry.whySierShouldCare,
        evidence: entry.evidence
      };
    });

  if (scored.length) {
    return scored;
  }

  return SIER_TERM_CATALOG.filter((entry) =>
    ["LLM", "RAG", "API Integration", "Data Governance", "ERP", "AI Agent"].includes(entry.term)
  )
    .slice(0, limit)
    .map((entry) => ({
      term: entry.term,
      meaning: entry.meaning,
      whySierShouldCare: entry.whySierShouldCare,
      evidence: []
    }));
}

function collectTechnicalBackgroundFallback(newsData, limit = 4) {
  return collectSierFallbackTerms(newsData, limit).map((entry) => {
    return {
      term: entry.term,
      meaning: entry.meaning,
      technical_background: TERM_TECHNICAL_BACKGROUND[entry.term] || entry.whySierShouldCare,
      evidence: entry.evidence
    };
  });
}

function buildLastMonthContextFallback(fallbackTerms, limit = 3) {
  return fallbackTerms
    .filter((item) => item.evidence?.length)
    .slice(0, limit)
    .map((item) => {
      const primary = item.evidence[0];
      return `${item.term} については、「${primary.title}」(${primary.source}) で動きが確認でき、今回のニュース群では ${item.term} が実装論ではなく実案件や製品更新の文脈で扱われていることが分かります。`;
    });
}

function formatNotebookLmFocusedFallback(structured) {
  const lines = [
    "## 1. Coverage note",
    "- This brief is based only on the articles collected in this run.",
    "- The full model-generated research brief was unavailable in this run, but the term analysis below was generated from the collected articles.",
    "",
    "## 2. Last 7 days: Japan and US",
    "- Please refer to the article bundle below for the full source material.",
    "",
    "## 3. Related viewpoints",
    "",
    "### Terms SIers should understand",
    ""
  ];

  for (const item of structured.sier_terms_to_know || []) {
    if (!item?.term) {
      continue;
    }
    lines.push(`- ${item.term}`);
    lines.push(`  Meaning: ${item.meaning || "Not enough evidence in collected articles."}`);
    lines.push(`  Why SIers should care: ${item.why_sier_should_care || "Not enough evidence in collected articles."}`);
    if (Array.isArray(item.sources) && item.sources.length) {
      lines.push(`  Sources: ${item.sources.join(", ")}`);
    }
    lines.push("");
  }

  lines.push("### Terms and technical background", "");

  for (const item of structured.terms_and_background || []) {
    if (!item?.term) {
      continue;
    }
    lines.push(`- ${item.term}`);
    lines.push(`  Meaning: ${item.meaning || "Not enough evidence in collected articles."}`);
    lines.push(`  Technical background: ${item.technical_background || "Not enough evidence in collected articles."}`);
    if (Array.isArray(item.sources) && item.sources.length) {
      lines.push(`  Sources: ${item.sources.join(", ")}`);
    }
    lines.push("");
  }

  lines.push("### Related developments over the last month", "");
  for (const item of structured.last_month_context || []) {
    if (item) {
      lines.push(`- ${item}`);
    }
  }

  return lines.join("\n").trim();
}

function formatNotebookLmResearchNotes(structured) {
  const lines = ["## Research Brief", ""];

  function pushBullets(items = []) {
    for (const item of items) {
      if (item) {
        lines.push(`- ${item}`);
      }
    }
    lines.push("");
  }

  function pushRegionalSection(title, items = []) {
    lines.push(title, "");
    for (const item of items) {
      if (!item?.title) {
        continue;
      }
      lines.push(`- ${item.title}`);
      lines.push(`  What happened: ${item.what_happened || "Not enough evidence in collected articles."}`);
      lines.push(`  Why it matters: ${item.why_it_matters || "Not enough evidence in collected articles."}`);
      if (Array.isArray(item.sources) && item.sources.length) {
        lines.push(`  Sources: ${item.sources.join(", ")}`);
      }
      lines.push("");
    }
  }

  lines.push("## 1. Coverage note", "");
  pushBullets(structured.coverage_note || []);

  lines.push("## 2. Last 7 days: Japan and US", "");
  pushRegionalSection("### Japan", structured.last_7_days_japan || []);
  pushRegionalSection("### United States", structured.last_7_days_us || []);

  lines.push("## 3. Related viewpoints", "");
  lines.push("### SAP", "");
  pushBullets(structured.sap_relevance || []);
  lines.push("### Core systems", "");
  pushBullets(structured.core_systems_relevance || []);
  lines.push("### Terms SIers should understand", "");

  for (const item of structured.sier_terms_to_know || []) {
    if (!item?.term) {
      continue;
    }
    lines.push(`- ${item.term}`);
    lines.push(`  Meaning: ${item.meaning || "Not enough evidence in collected articles."}`);
    lines.push(`  Why SIers should care: ${item.why_sier_should_care || "Not enough evidence in collected articles."}`);
    lines.push("");
  }

  lines.push("### Terms and technical background", "");

  for (const item of structured.terms_and_background || []) {
    if (!item?.term) {
      continue;
    }
    lines.push(`- ${item.term}`);
    lines.push(`  Meaning: ${item.meaning || "Not enough evidence in collected articles."}`);
    lines.push(`  Technical background: ${item.technical_background || "Not enough evidence in collected articles."}`);
    lines.push("");
  }

  lines.push("### Related developments over the last month", "");
  pushBullets(structured.last_month_context || []);

  lines.push("## 4. Suggested reading order for NotebookLM", "");
  pushBullets(structured.notebooklm_reading_order || []);

  return lines.join("\n").trim();
}

function parseJsonObject(text = "") {
  const fenced = text.match(/```(?:json)?\s*([\s\S]*?)```/i)?.[1];
  const target = fenced || text;
  const objectMatch = target.match(/\{[\s\S]*\}/);
  if (!objectMatch) {
    return null;
  }

  try {
    return JSON.parse(objectMatch[0]);
  } catch {
    return null;
  }
}

async function callGemini({ prompt, systemInstruction = "", maxOutputTokens = 2200, temperature = 0.2, responseMimeType }) {
  const apiKey = getGeminiApiKey();
  if (!apiKey) {
    return null;
  }

  const response = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${geminiSummaryModel}:generateContent?key=${apiKey}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      systemInstruction: systemInstruction ? { parts: [{ text: systemInstruction }] } : undefined,
      contents: [{ parts: [{ text: prompt }] }],
      generationConfig: {
        temperature,
        maxOutputTokens,
        responseMimeType
      }
    })
  });

  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(data.error?.message || `Gemini request failed with status ${response.status}`);
  }

  return (data.candidates || [])
    .flatMap((candidate) => candidate.content?.parts || [])
    .map((part) => part.text || "")
    .join("\n")
    .trim();
}

async function translateOneWithOpenAI(article) {
  const client = getOpenAIClient();
  if (!client) {
    return null;
  }

  const prompt = [
    "Translate the following article title and body into natural Japanese.",
    "Keep facts accurate.",
    'Return JSON only in the form {"titleJa":"...","contentJa":"..."}',
    "",
    `title:\n${article.title || ""}`,
    "",
    `content:\n${(article.contentSnippet || "").slice(0, 3500)}`
  ].join("\n");

  const response = await client.responses.create({
    model: openaiSummaryModel,
    input: prompt
  });

  return parseJsonObject(response.output_text || "");
}

async function translateOneWithGemini(article) {
  if (!hasGemini()) {
    return null;
  }

  const text = await callGemini({
    systemInstruction: "Translate the article title and body into natural Japanese. Keep facts accurate. Return JSON only.",
    prompt: [
      'Format: {"titleJa":"...","contentJa":"..."}',
      `title:\n${article.title || ""}`,
      `content:\n${(article.contentSnippet || "").slice(0, 3500)}`
    ].join("\n\n"),
    maxOutputTokens: 1800,
    responseMimeType: "application/json"
  });

  return parseJsonObject(text || "");
}

async function translateOneArticle(article) {
  const shouldTranslateTitle = needsJapaneseTranslation(article.title);
  const shouldTranslateBody = needsJapaneseTranslation(article.contentSnippet);
  if (!shouldTranslateTitle && !shouldTranslateBody) {
    return article;
  }

  let translated = null;
  const providers = getProviderOrder();

  for (const provider of providers) {
    try {
      translated = provider === "gemini" ? await translateOneWithGemini(article) : await translateOneWithOpenAI(article);
      if (translated) {
        break;
      }
    } catch {
      translated = null;
    }
  }

  if (!translated) {
    return article;
  }

  return {
    ...article,
    title: translated.titleJa || article.title,
    contentSnippet: translated.contentJa || article.contentSnippet
  };
}

export async function localizeNewsData(newsData) {
  const targets = newsData.articles.slice(0, translationLimit);
  const translatedTargets = await Promise.all(targets.map((article) => translateOneArticle(article)));
  const translatedMap = new Map(translatedTargets.map((article) => [article.link, article]));

  const translatedArticles = newsData.articles.map((article) => translatedMap.get(article.link) || article);
  const translatedByLink = new Map(translatedArticles.map((article) => [article.link, article]));

  return {
    ...newsData,
    articles: translatedArticles,
    groupedByViewpoint: newsData.groupedByViewpoint.map((group) => ({
      ...group,
      articles: group.articles.map((article) => translatedByLink.get(article.link) || article)
    }))
  };
}

async function generateOpenAISummary(newsData) {
  const client = getOpenAIClient();
  if (!client) {
    return null;
  }

  const response = await client.responses.create({
    model: openaiSummaryModel,
    input: buildDigestPrompt(newsData)
  });

  return response.output_text?.trim() || null;
}

async function generateGeminiSummary(newsData) {
  if (!hasGemini()) {
    return null;
  }

  return callGemini({
    systemInstruction: "Write a concise Japanese business news digest grounded only in the provided articles.",
    prompt: buildDigestPrompt(newsData),
    maxOutputTokens: 2200
  });
}

async function generateOpenAINotebookLmResearchNotes(newsData) {
  const client = getOpenAIClient();
  if (!client) {
    return null;
  }

  const response = await client.responses.create({
    model: openaiSummaryModel,
    input: buildNotebookLmResearchPrompt(newsData)
  });

  const structured = parseJsonObject(response.output_text || "");
  return structured ? formatNotebookLmResearchNotes(structured) : null;
}

async function generateGeminiNotebookLmResearchNotes(newsData) {
  if (!hasGemini()) {
    return null;
  }

  const text = await callGemini({
    systemInstruction: "Return exactly one JSON object grounded only in the provided articles.",
    prompt: buildNotebookLmResearchPrompt(newsData),
    maxOutputTokens: 5200,
    responseMimeType: "application/json"
  });

  const structured = parseJsonObject(text || "");
  return structured ? formatNotebookLmResearchNotes(structured) : null;
}

async function generateOpenAINotebookLmSierFallbackNotes(newsData) {
  const client = getOpenAIClient();
  if (!client) {
    return null;
  }

  const response = await client.responses.create({
    model: openaiSummaryModel,
    input: buildNotebookLmSierTermsPrompt(newsData)
  });

  const structured = parseJsonObject(response.output_text || "");
  return structured ? formatNotebookLmFocusedFallback(structured) : null;
}

async function generateGeminiNotebookLmSierFallbackNotes(newsData) {
  if (!hasGemini()) {
    return null;
  }

  const text = await callGemini({
    systemInstruction: "Return exactly one JSON object grounded only in the provided articles. Focus on practical SIer terminology.",
    prompt: buildNotebookLmSierTermsPrompt(newsData),
    maxOutputTokens: 2600,
    responseMimeType: "application/json"
  });

  const structured = parseJsonObject(text || "");
  return structured ? formatNotebookLmFocusedFallback(structured) : null;
}

export async function generateSummary(newsData) {
  const providers = getProviderOrder();

  for (const provider of providers) {
    const summary = provider === "gemini" ? await generateGeminiSummary(newsData) : await generateOpenAISummary(newsData);
    if (summary) {
      return summary;
    }
  }

  return buildFallbackSummary(newsData);
}

export async function generateNotebookLmResearchNotes(newsData) {
  const providers = getProviderOrder();

  for (const provider of providers) {
    const notes =
      provider === "gemini"
        ? await generateGeminiNotebookLmResearchNotes(newsData)
        : await generateOpenAINotebookLmResearchNotes(newsData);
    if (notes) {
      return notes;
    }
  }

  for (const provider of providers) {
    const notes =
      provider === "gemini"
        ? await generateGeminiNotebookLmSierFallbackNotes(newsData)
        : await generateOpenAINotebookLmSierFallbackNotes(newsData);
    if (notes) {
      return notes;
    }
  }

  const fallbackTerms = collectSierFallbackTerms(newsData, 6);
  const fallbackBackground = collectTechnicalBackgroundFallback(newsData, 4);
  const fallbackLastMonthContext = buildLastMonthContextFallback(fallbackTerms, 3);

  return [
    "## 1. Coverage note",
    "- This brief is based only on the articles collected in this run.",
    "- Nikkei and xTECH findings appear only when they were separately collected by this app.",
    "",
    "## 2. Last 7 days: Japan and US",
    "- Please refer to the article bundle below. No model-generated brief was available in this run.",
    "",
    "## 3. Related viewpoints",
    "",
    "### Terms SIers should understand",
    ...fallbackTerms.flatMap((item) => {
      const lines = [
        `- ${item.term}`,
        `  Meaning: ${item.meaning}`,
        `  Why SIers should care: ${item.whySierShouldCare}`
      ];
      if (item.evidence?.length) {
        lines.push(`  関連記事: ${item.evidence.slice(0, 2).map((evidence) => `「${evidence.title}」(${evidence.source})`).join(" / ")}`);
      }
      lines.push("");
      return lines;
    }),
    "",
    "### Terms and technical background",
    ...fallbackBackground.flatMap((item) => {
      const lines = [
        `- ${item.term}`,
        `  Meaning: ${item.meaning}`,
        `  Technical background: ${item.technical_background}`
      ];
      if (item.evidence?.length) {
        lines.push(`  Related article ids: ${item.evidence.slice(0, 2).map((evidence) => evidence.sourceId).join(", ")}`);
      }
      lines.push("");
      return lines;
    }),
    "",
    "### Related developments over the last month",
    ...fallbackLastMonthContext.map((item) => `- ${item}`)
  ].join("\n");
}

export async function generateAudio(summaryText) {
  const client = getOpenAIClient();
  if (!client) {
    return null;
  }

  const audioResponse = await client.audio.speech.create({
    model: ttsModel,
    voice: "alloy",
    input: summaryText.slice(0, 1900),
    instructions: "Calm, clear Japanese business news narration."
  });

  return Buffer.from(await audioResponse.arrayBuffer());
}

export function buildFallbackSummary(newsData) {
  const overview = newsData.topics.map((topic) => topic.label).join(" / ");
  const highlights = newsData.articles.slice(0, 5).map((article) => `- ${article.title} (${article.topicLabel})`);
  const sections = newsData.groupedByViewpoint.map((group) => {
    const lines = group.articles
      .slice(0, 3)
      .map((article) => `- ${article.title}\n  Summary: ${article.contentSnippet || "No related summary was available."}`);
    return `## ${group.label}\n${lines.join("\n")}`;
  });

  return [
    "# News Brief",
    "",
    `Topics: ${overview}`,
    `Generated at: ${new Date(newsData.generatedAt).toLocaleString("ja-JP")}`,
    "",
    "## Executive Summary",
    "This fallback summary is based on the latest collected articles. Review the sections below for the most relevant developments by viewpoint.",
    "",
    "## Highlights",
    ...highlights,
    "",
    ...sections
  ].join("\n");
}
