import fs from "node:fs/promises";
import path from "node:path";
import { Document, HeadingLevel, Packer, Paragraph, TextRun } from "docx";

const outputDir = path.join(process.cwd(), "outputs");

function slugify(value) {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/(^-|-$)/g, "");
}

async function ensureOutputDir() {
  await fs.mkdir(outputDir, { recursive: true });
}

export async function saveTextSummary(summaryText, topicIds) {
  await ensureOutputDir();
  const fileName = `${createBaseName(topicIds)}.md`;
  const fullPath = path.join(outputDir, fileName);
  await fs.writeFile(fullPath, summaryText, "utf8");
  return fullPath;
}

export async function saveWordSummary(summaryText, topicIds) {
  await ensureOutputDir();

  const paragraphs = summaryText.split("\n").map((line) => {
    if (line.startsWith("# ")) {
      return new Paragraph({ heading: HeadingLevel.HEADING_1, children: [new TextRun(line.replace(/^# /, ""))] });
    }
    if (line.startsWith("## ")) {
      return new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun(line.replace(/^## /, ""))] });
    }
    if (line.startsWith("- ")) {
      return new Paragraph({ bullet: { level: 0 }, children: [new TextRun(line.replace(/^- /, ""))] });
    }
    return new Paragraph(line);
  });

  const document = new Document({ sections: [{ children: paragraphs }] });
  const buffer = await Packer.toBuffer(document);
  const fileName = `${createBaseName(topicIds)}.docx`;
  const fullPath = path.join(outputDir, fileName);
  await fs.writeFile(fullPath, buffer);
  return fullPath;
}

export async function saveAudioSummary(audioBuffer, topicIds) {
  await ensureOutputDir();
  const fileName = `${createBaseName(topicIds)}.mp3`;
  const fullPath = path.join(outputDir, fileName);
  await fs.writeFile(fullPath, audioBuffer);
  return fullPath;
}

export async function saveNotebookLmBundle(newsData, topicIds, options = {}) {
  await ensureOutputDir();
  const generatedAt = newsData.generatedAt ? new Date(newsData.generatedAt).toISOString() : new Date().toISOString();
  const notebookArticles = (await Promise.all(newsData.articles.map(async (article) => {
    const resolved = await resolveNotebookLmBody(article);
    return {
      ...article,
      notebooklmResolvedBody: resolved.body,
      notebooklmResolvedUrl: resolved.url
    };
  }))).filter((article) => shouldIncludeNotebookLmArticle(article));
  const lines = [
    "# NotebookLM Source Bundle",
    "",
    "## Bundle Metadata",
    "",
    `- Generated at: ${generatedAt}`,
    `- Topics: ${(newsData.topics || []).map((topic) => topic.label).join(" / ") || topicIds.join(" / ") || "(none)"}`,
    `- Article count: ${notebookArticles.length}`,
    ""
  ];

  if (options.researchNotes) {
    lines.push("## Research Brief");
    lines.push("");
    lines.push(options.researchNotes.trim());
    lines.push("");
    lines.push("---");
    lines.push("");
  }

  for (const [index, article] of notebookArticles.entries()) {
    const contentStatus = article.contentStatus || article.content_status || "available";
    const contentWarning = article.contentWarning || article.content_warning || "";
    const notebooklmText = article.notebooklmResolvedBody || contentWarning || "No extracted body text was available.";
    const metadataOnly = contentStatus === "unavailable";

    lines.push(`## Article ${index + 1}: ${article.title}${metadataOnly ? " [Metadata Only]" : ""}`);
    lines.push("");
    lines.push(`- Topic: ${article.topicLabel || ""}`);
    lines.push(`- Source: ${article.source || ""}`);
    lines.push(`- Published: ${article.publishedAt || article.published_at || article.pubDate || ""}`);
    lines.push(`- URL: ${article.notebooklmResolvedUrl || article.url || article.link || ""}`);
    lines.push(`- Source type: ${article.sourceType || ""}`);
    lines.push(`- Content status: ${contentStatus}`);
    lines.push("");
    lines.push("### Body");
    lines.push("");
    lines.push(notebooklmText);
    lines.push("");
  }

  const documentText = `${lines.join("\n").trim()}\n`;
  const fileName = `${createBaseName(topicIds)}-notebooklm.txt`;
  const fullPath = path.join(outputDir, fileName);
  await fs.writeFile(fullPath, documentText, "utf8");
  return fullPath;
}

async function resolveNotebookLmBody(article) {
  const directBody = pickNotebookLmBody(article);
  if (directBody && !isLikelyTrivialBody(directBody, article)) {
    return {
      body: directBody,
      url: article.url || article.link || ""
    };
  }

  const fetched = await fetchPublicArticleBody(article.link || article.url || "");
  if (fetched.body) {
    return fetched;
  }

  return {
    body: directBody || article.contentWarning || article.content_warning || "No extracted body text was available.",
    url: article.url || article.link || ""
  };
}

function pickNotebookLmBody(article) {
  const candidates = [
    article.notebooklmText,
    article.notebooklm_text,
    article.rawText,
    article.raw_text,
    article.cleanedText,
    article.cleaned_text,
    article.contentSnippet,
    article.content_snippet
  ];

  for (const candidate of candidates) {
    if (typeof candidate === "string" && candidate.trim()) {
      return candidate.trim();
    }
  }

  return "";
}

function isLikelyTrivialBody(body, article) {
  const normalizedBody = normalizeComparableText(body);
  const normalizedTitle = normalizeComparableText(article.title || "");
  const normalizedSource = normalizeComparableText(article.source || "");

  if (!normalizedBody || normalizedBody.length < 120) {
    return true;
  }

  if (normalizedTitle && normalizedBody === normalizedTitle) {
    return true;
  }

  if (normalizedTitle && normalizedSource && normalizedBody === `${normalizedTitle} ${normalizedSource}`.trim()) {
    return true;
  }

  return false;
}

function shouldIncludeNotebookLmArticle(article) {
  const body = `${article.notebooklmResolvedBody || ""}`.trim();
  if (!body) {
    return false;
  }

  const normalizedBody = normalizeComparableText(body);
  const normalizedTitle = normalizeComparableText(article.title || "");
  const normalizedSource = normalizeComparableText(article.source || "");
  const normalizedBodyWithoutSource = normalizedSource
    ? normalizedBody.replace(new RegExp(`\\b${escapeRegExp(normalizedSource)}\\b`, "g"), "").replace(/\s+/g, " ").trim()
    : normalizedBody;

  if (normalizedBody === "no extracted body text was available.") {
    return false;
  }

  if (body.length < 100) {
    return false;
  }

  if (
    /企業での記事共有|会議資料への転載|注文印刷|有料登録すると続きをお読みいただけます|無料登録でも記事消費|ログイン|profile user|logout\b/i.test(body)
  ) {
    return false;
  }

  if (normalizedTitle && (normalizedBody === normalizedTitle || normalizedBodyWithoutSource === normalizedTitle)) {
    return false;
  }

  if (normalizedBody.split(" ").length <= 4 && body.length < 120) {
    return false;
  }

  if (/^(openai|mysql|architecture|devops|sql server)$/i.test(body.trim())) {
    return false;
  }

  return true;
}

function escapeRegExp(value) {
  return `${value || ""}`.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function normalizeComparableText(value) {
  return `${value || ""}`.replace(/\s+/g, " ").trim().toLowerCase();
}

function normalizeCharset(charset = "") {
  return charset
    .trim()
    .toLowerCase()
    .replace(/["']/g, "")
    .replace(/^shift-jis$/, "shift_jis")
    .replace(/^sjis$/, "shift_jis")
    .replace(/^x-sjis$/, "shift_jis")
    .replace(/^windows-31j$/, "shift_jis")
    .replace(/^ms932$/, "shift_jis");
}

function detectCharset(contentType = "", rawBytes = new Uint8Array()) {
  const headerMatch = contentType.match(/charset=([^;]+)/i);
  if (headerMatch?.[1]) {
    return normalizeCharset(headerMatch[1]);
  }

  const asciiHead = Buffer.from(rawBytes.slice(0, 4096)).toString("latin1");
  const metaMatch =
    asciiHead.match(/<meta[^>]+charset=["']?\s*([^"'>\s]+)/i) ||
    asciiHead.match(/<meta[^>]+content=["'][^"']*charset=([^"'>;\s]+)/i);

  return metaMatch?.[1] ? normalizeCharset(metaMatch[1]) : "utf-8";
}

function decodeDocument(rawBytes, contentType = "") {
  const charset = detectCharset(contentType, rawBytes);
  try {
    return new TextDecoder(charset).decode(rawBytes);
  } catch {
    return new TextDecoder("utf-8").decode(rawBytes);
  }
}

function decodeHtml(value = "") {
  return value
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
}

function stripTags(value = "") {
  return decodeHtml(value).replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
}

function extractBodyRoot(html) {
  const candidates = [
    /<article[^>]*>([\s\S]*?)<\/article>/i,
    /<main[^>]*>([\s\S]*?)<\/main>/i,
    /<div[^>]+class=["'][^"']*(?:article|content|body|main|post|entry)[^"']*["'][^>]*>([\s\S]*?)<\/div>/i
  ];

  for (const pattern of candidates) {
    const match = html.match(pattern);
    if (match?.[1]) {
      return match[1];
    }
  }

  return html;
}

function extractParagraphs(html) {
  const body = extractBodyRoot(
    html
      .replace(/<script[\s\S]*?<\/script>/gi, " ")
      .replace(/<style[\s\S]*?<\/style>/gi, " ")
      .replace(/<noscript[\s\S]*?<\/noscript>/gi, " ")
  );

  return [...body.matchAll(/<p\b[^>]*>([\s\S]*?)<\/p>/gi)]
    .map((match) => stripTags(match[1]))
    .filter((text) => text.length > 40)
    .slice(0, 60);
}

async function fetchPublicArticleBody(url) {
  if (!url) {
    return { body: "", url: "" };
  }

  try {
    const resolvedUrl = await resolveArticleUrl(url);
    const response = await fetch(resolvedUrl, {
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122 Safari/537.36",
        Accept: "text/html,application/xhtml+xml"
      },
      redirect: "follow"
    });

    if (!response.ok) {
      return { body: "", url: resolvedUrl };
    }

    const rawBytes = new Uint8Array(await response.arrayBuffer());
    const html = decodeDocument(rawBytes, response.headers.get("content-type") || "");
    const paragraphs = extractParagraphs(html);

    return {
      body: paragraphs.join("\n\n").trim(),
      url: response.url || resolvedUrl
    };
  } catch {
    return { body: "", url };
  }
}

async function resolveArticleUrl(url) {
  if (!/https:\/\/news\.google\.com\/rss\/articles\//i.test(url)) {
    return url;
  }

  try {
    const response = await fetch(url, {
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122 Safari/537.36",
        Accept: "text/html,application/xhtml+xml"
      },
      redirect: "follow"
    });

    const rawBytes = new Uint8Array(await response.arrayBuffer());
    const html = decodeDocument(rawBytes, response.headers.get("content-type") || "");
    const extracted = extractCanonicalArticleUrl(html);
    return extracted || response.url || url;
  } catch {
    return url;
  }
}

function extractCanonicalArticleUrl(html) {
  const candidates = [
    ...html.matchAll(/<link[^>]+rel=["']canonical["'][^>]+href=["']([^"']+)["']/gi),
    ...html.matchAll(/<meta[^>]+property=["']og:url["'][^>]+content=["']([^"']+)["']/gi),
    ...html.matchAll(/https?:\/\/[^"'\\<>\s]+/gi)
  ]
    .map((match) => match[1] || match[0])
    .map((value) => decodeHtml(value))
    .map((value) => value.replace(/\\u0026/g, "&"))
    .filter(Boolean);

  for (const candidate of candidates) {
    if (/^https?:\/\//i.test(candidate) && !/https?:\/\/(news\.google\.com|www\.google\.com|googleusercontent\.com|gstatic\.com)\b/i.test(candidate)) {
      return candidate;
    }
  }

  return "";
}

function createBaseName(topicIds) {
  const topicPart = topicIds.map(slugify).join("-");
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  return `news-digest-${topicPart}-${timestamp}`;
}
