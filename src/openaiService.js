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

  return `
縺ゅ↑縺溘・繧ｨ繧ｰ繧ｼ繧ｯ繝・ぅ繝門髄縺代ル繝･繝ｼ繧ｹ邱ｨ髮・・〒縺吶・谺｡縺ｮ繝九Η繝ｼ繧ｹ鄒､繧偵ｂ縺ｨ縺ｫ縲∵律譛ｬ隱槭〒縲悟精蜿弱＠繧・☆縺・阪ル繝･繝ｼ繧ｹ繝悶Μ繝ｼ繝輔ｒ菴懊▲縺ｦ縺上□縺輔＞縲・
隕∽ｻｶ:
- 縺ｾ縺・-5陦後〒蜈ｨ菴薙し繝槭Μ
- 谺｡縺ｫ縲御ｻ頑款縺輔∴繧九∋縺阪・繧､繝ｳ繝医阪ｒ3轤ｹ
- 縺昴・蠕後∬ｦｳ轤ｹ縺斐→縺ｫ遶繧貞・縺代ｋ: 謚陦灘虚蜷・/ 繝薙ず繝阪せ蜍募髄 / 莨∵･ｭ莠倶ｾ・/ 蟄ｦ鄙偵さ繝ｳ繝・Φ繝・- 蜷・ｫ縺ｯ縲・㍾隕√ヨ繝斐ャ繧ｯ2-4轤ｹ縲∽ｽ輔′襍ｷ縺阪◆縺九√↑縺憺㍾隕√°縲∵ｬ｡縺ｫ隕九ｋ縺ｹ縺阪％縺ｨ 繧堤ｰ｡貎斐↓譖ｸ縺・- 譛蠕後↓縲後♀縺吶☆繧√い繧ｯ繧ｷ繝ｧ繝ｳ縲阪ｒ縲∵ュ蝣ｱ蜿朱寔繝ｻ讌ｭ蜍咎←逕ｨ繝ｻ蟄ｦ鄙偵・3蛹ｺ蛻・〒譖ｸ縺・- 隕句・縺励→邂・擅譖ｸ縺阪ｒ菴ｿ縺・∝・髟ｷ縺ｫ縺励↑縺・- 謗ｨ貂ｬ縺励☆縺弱★縲∬ｨ倅ｺ区ュ蝣ｱ縺九ｉ螯･蠖薙↑遽・峇縺ｧ謨ｴ逅・☆繧・- trust_score 縺碁ｫ倥＞險倅ｺ九→ curated-feed 繧貞━蜈医＠縺ｦ蛻､譁ｭ縺吶ｋ

隕ｳ轤ｹ:
${viewpointBlock}

險倅ｺ倶ｸ隕ｧ:
${articleLines.join("\n\n")}
  `.trim();
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

  return `
Return exactly one JSON object in Japanese. Do not output Markdown. Do not use code fences.

Rules:
- Use only the supplied articles and metadata.
- Do not invent facts, dates, companies, or relationships.
- If evidence is thin, say so explicitly.
- Every string value must be complete and self-contained.
- Every array item must be complete. No empty bullets. No dangling asterisks.
- Prefer concise but information-dense wording.

Required JSON shape:
{
  "coverage_note": ["...", "..."],
  "last_7_days_japan": [{"title":"...","what_happened":"...","why_it_matters":"...","sources":["1","3"]}],
  "last_7_days_us": [{"title":"...","what_happened":"...","why_it_matters":"...","sources":["2"]}],
  "sap_relevance": ["...", "..."],
  "core_systems_relevance": ["...", "..."],
  "terms_and_background": [{"term":"...","meaning":"...","technical_background":"..."}],
  "last_month_context": ["...", "..."],
  "notebooklm_reading_order": ["...", "..."]
}

Additional constraints:
- last_7_days_japan: 3 to 6 items when possible.
- last_7_days_us: 3 to 6 items when possible.
- sap_relevance: 2 to 5 bullets.
- core_systems_relevance: 2 to 5 bullets.
- terms_and_background: 3 to 8 items.
- last_month_context: 3 to 6 bullets.
- notebooklm_reading_order: 5 to 10 bullets.
- sources must reference the numbered article ids from the supplied list.

Articles:
${articleLines.join("\n\n")}
  `.trim();
}

function formatNotebookLmResearchNotes(structured) {
  const lines = ["## Research Brief", ""];

  const pushBullets = (items = []) => {
    for (const item of items) {
      if (item) {
        lines.push(`- ${item}`);
      }
    }
    lines.push("");
  };

  lines.push("## 1. Coverage note", "");
  pushBullets(structured.coverage_note || []);

  const pushRegionalSection = (title, items = []) => {
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
  };

  lines.push("## 2. Last 7 days: Japan and US", "");
  pushRegionalSection("### Japan", structured.last_7_days_japan || []);
  pushRegionalSection("### United States", structured.last_7_days_us || []);

  lines.push("## 3. Related viewpoints", "");
  lines.push("### SAP", "");
  pushBullets(structured.sap_relevance || []);
  lines.push("### Core systems", "");
  pushBullets(structured.core_systems_relevance || []);
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

  const prompt = `
莉･荳九・繝九Η繝ｼ繧ｹ險倅ｺ九・繧ｿ繧､繝医Ν縺ｨ譛ｬ譁・ｒ閾ｪ辟ｶ縺ｪ譌･譛ｬ隱槭↓鄙ｻ險ｳ縺励※縺上□縺輔＞縲・- 諢丞袖繧貞､峨∴縺ｪ縺・- 蝗ｺ譛牙錐隧槭∬｣ｽ蜩∝錐縲∽ｼ∵･ｭ蜷阪・邯ｭ謖√☆繧・- 菴呵ｨ医↑隗｣隱ｬ繧定ｶｳ縺輔↑縺・- JSON繧ｪ繝悶ず繧ｧ繧ｯ繝医・縺ｿ繧定ｿ斐☆
蠖｢蠑・ {"titleJa":"...","contentJa":"..."}

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

async function translateOneWithGemini(article) {
  if (!hasGemini()) {
    return null;
  }

  const text = await callGemini({
    systemInstruction: "繝九Η繝ｼ繧ｹ險倅ｺ九・繧ｿ繧､繝医Ν縺ｨ譛ｬ譁・ｒ閾ｪ辟ｶ縺ｪ譌･譛ｬ隱槭↓鄙ｻ險ｳ縺励※縺上□縺輔＞縲・SON繧ｪ繝悶ず繧ｧ繧ｯ繝医・縺ｿ繧定ｿ斐＠縺ｦ縺上□縺輔＞縲・,
    prompt: [
      '蠖｢蠑・ {"titleJa":"...","contentJa":"..."}',
      `title:\n${article.title || ""}`,
      `content:\n${(article.contentSnippet || "").slice(0, 3500)}`
    ].join("\n\n"),
    maxOutputTokens: 1800,
    responseMimeType: "application/json"
  });

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
    systemInstruction: "縺ゅ↑縺溘・繧ｨ繧ｰ繧ｼ繧ｯ繝・ぅ繝門髄縺代ル繝･繝ｼ繧ｹ邱ｨ髮・・〒縺吶よ律譛ｬ隱槭〒邁｡貎斐°縺､讒矩蛹悶＆繧後◆繝九Η繝ｼ繧ｹ隕∫ｴ・ｒ譖ｸ縺・※縺上□縺輔＞縲・,
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

  return [
    "# NotebookLM Research Brief",
    "",
    "## Coverage note",
    "- This brief is based only on the articles collected in this run.",
    "- Nikkei and xTECH findings appear only when they were separately collected by this app.",
    "",
    "## Last 7 days: Japan and US",
    "- Please refer to the article bundle below. No model-generated brief was available in this run."
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
      .map((article) => `- ${article.title}\n  驥崎ｦ∵ｧ: ${article.contentSnippet || "髢｢騾｣繝九Η繝ｼ繧ｹ縺ｨ縺励※豕ｨ隕・}`);
    return `## ${group.label}\n${lines.join("\n")}`;
  });

  return [
    "# 繝九Η繝ｼ繧ｹ繝悶Μ繝ｼ繝・,
    "",
    `蟇ｾ雎｡繝・・繝・ ${overview}`,
    `逕滓・譌･譎・ ${new Date(newsData.generatedAt).toLocaleString("ja-JP")}`,
    "",
    "## 蜈ｨ菴薙し繝槭Μ",
    "逶ｴ霑・譌･髢薙・繝九Η繝ｼ繧ｹ縺九ｉ縲・∈謚槭ユ繝ｼ繝槭↓髢｢縺吶ｋ譛譁ｰ蜍募髄繧呈紛逅・＠縺ｾ縺励◆縲ら音縺ｫ荳贋ｽ崎ｨ倅ｺ九ｒ隕九ｋ縺ｨ縲∬｣ｽ蜩・ｲ蛹悶∽ｼ∵･ｭ蟆主・縲∽ｺ区･ｭ謌ｦ逡･縲∝ｭｦ鄙呈ｩ滉ｼ壹・4霆ｸ縺ｧ螟牙喧縺檎ｶ壹＞縺ｦ縺・∪縺吶・,
    "",
    "## 莉頑款縺輔∴繧九∋縺阪・繧､繝ｳ繝・,
    ...highlights,
    "",
    ...sections
  ].join("\n");
}
