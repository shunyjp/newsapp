import Anthropic from "@anthropic-ai/sdk";
import OpenAI from "openai";

const configuredSummaryProvider = (process.env.SUMMARY_PROVIDER || "auto").toLowerCase();
const openaiSummaryModel = process.env.OPENAI_SUMMARY_MODEL || "gpt-5";
const anthropicSummaryModel = process.env.ANTHROPIC_SUMMARY_MODEL || "claude-sonnet-4-20250514";
const ttsModel = process.env.OPENAI_TTS_MODEL || "gpt-4o-mini-tts";
const translationLimit = Number.parseInt(process.env.TRANSLATION_LIMIT || "12", 10);

function getOpenAIClient() {
  return process.env.OPENAI_API_KEY ? new OpenAI({ apiKey: process.env.OPENAI_API_KEY }) : null;
}

function getAnthropicClient() {
  return process.env.ANTHROPIC_API_KEY ? new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY }) : null;
}

function getProviderOrder() {
  const hasOpenAI = Boolean(process.env.OPENAI_API_KEY);
  const hasAnthropic = Boolean(process.env.ANTHROPIC_API_KEY);

  if (configuredSummaryProvider === "openai") {
    return hasOpenAI ? ["openai"] : hasAnthropic ? ["anthropic"] : [];
  }

  if (configuredSummaryProvider === "anthropic") {
    return hasAnthropic ? ["anthropic"] : hasOpenAI ? ["openai"] : [];
  }

  const order = [];
  if (hasAnthropic) {
    order.push("anthropic");
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

  return `
あなたはエグゼクティブ向けニュース編集者です。
次のニュース群をもとに、日本語で「吸収しやすい」ニュースブリーフを作ってください。

要件:
- まず3-5行で全体サマリ
- 次に「今押さえるべきポイント」を3点
- その後、観点ごとに章を分ける: 技術動向 / ビジネス動向 / 企業事例 / 学習コンテンツ
- 各章は、重要トピック2-4点、何が起きたか、なぜ重要か、次に見るべきこと を簡潔に書く
- 最後に「おすすめアクション」を、情報収集・業務適用・学習の3区分で書く
- 見出しと箇条書きを使い、冗長にしない
- 推測しすぎず、記事情報から妥当な範囲で整理する
- trust_score が高い記事と curated-feed を優先して判断する

観点:
${viewpointBlock}

記事一覧:
${articleLines.join("\n\n")}
  `.trim();
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

async function translateOneWithOpenAI(article) {
  const client = getOpenAIClient();
  if (!client) {
    return null;
  }

  const prompt = `
以下のニュース記事のタイトルと本文を自然な日本語に翻訳してください。
- 意味を変えない
- 固有名詞、製品名、企業名は維持する
- 余計な解説を足さない
- JSONオブジェクトのみを返す
形式: {"titleJa":"...","contentJa":"..."}

title:
${article.title || ""}

content:
${(article.contentSnippet || "").slice(0, 3500)}
  `.trim();

  const response = await client.responses.create({
    model: openaiSummaryModel,
    input: prompt
  });

  return parseJsonObject(response.output_text || "");
}

async function translateOneWithAnthropic(article) {
  const client = getAnthropicClient();
  if (!client) {
    return null;
  }

  const response = await client.messages.create({
    model: anthropicSummaryModel,
    max_tokens: 1800,
    system: "ニュース記事のタイトルと本文を自然な日本語に翻訳してください。JSONオブジェクトのみを返してください。",
    messages: [
      {
        role: "user",
        content: [
          '形式: {"titleJa":"...","contentJa":"..."}',
          `title:\n${article.title || ""}`,
          `content:\n${(article.contentSnippet || "").slice(0, 3500)}`
        ].join("\n\n")
      }
    ]
  });

  const text = response.content
    .filter((item) => item.type === "text")
    .map((item) => item.text)
    .join("\n");

  return parseJsonObject(text);
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
      translated = provider === "anthropic" ? await translateOneWithAnthropic(article) : await translateOneWithOpenAI(article);
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

async function generateAnthropicSummary(newsData) {
  const client = getAnthropicClient();
  if (!client) {
    return null;
  }

  const response = await client.messages.create({
    model: anthropicSummaryModel,
    max_tokens: 2200,
    system: "あなたはエグゼクティブ向けニュース編集者です。日本語で簡潔かつ構造化されたニュース要約を書いてください。",
    messages: [
      {
        role: "user",
        content: buildDigestPrompt(newsData)
      }
    ]
  });

  return response.content
    .filter((item) => item.type === "text")
    .map((item) => item.text)
    .join("\n")
    .trim() || null;
}

export async function generateSummary(newsData) {
  const providers = getProviderOrder();

  for (const provider of providers) {
    const summary = provider === "anthropic" ? await generateAnthropicSummary(newsData) : await generateOpenAISummary(newsData);
    if (summary) {
      return summary;
    }
  }

  return buildFallbackSummary(newsData);
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
      .map((article) => `- ${article.title}\n  重要性: ${article.contentSnippet || "関連ニュースとして注視"}`);
    return `## ${group.label}\n${lines.join("\n")}`;
  });

  return [
    "# ニュースブリーフ",
    "",
    `対象テーマ: ${overview}`,
    `生成日時: ${new Date(newsData.generatedAt).toLocaleString("ja-JP")}`,
    "",
    "## 全体サマリ",
    "直近7日間のニュースから、選択テーマに関する最新動向を整理しました。特に上位記事を見ると、製品進化、企業導入、事業戦略、学習機会の4軸で変化が続いています。",
    "",
    "## 今押さえるべきポイント",
    ...highlights,
    "",
    ...sections
  ].join("\n");
}
