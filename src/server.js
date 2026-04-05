import "dotenv/config";
import express from "express";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { TOPIC_DEFINITIONS, VIEWPOINT_DEFINITIONS } from "./config.js";
import { fetchTopicNewsWithImports } from "./newsService.js";
import { generateAudio, generateNotebookLmResearchNotes, generateSummary, localizeNewsData } from "./openaiService.js";
import { saveAudioSummary, saveNotebookLmBundle, saveTextSummary, saveWordSummary } from "./exporters.js";
import { addImportedArticle, listImportedArticles, removeImportedArticle } from "./importStore.js";
import {
  getNikkeiLoginStatus,
  loginToNikkeiAndPersistSession,
  submitNikkeiOtpAndPersistSession
} from "./nikkeiLoginService.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, "..");
const publicDir = path.join(projectRoot, "public");
const outputsDir = path.join(projectRoot, "outputs");
const app = express();
const port = Number.parseInt(process.env.PORT || "3000", 10);
const host = process.env.HOST || "0.0.0.0";

function pickDiverseArticles(articles, topicIds, limit = 12) {
  const maxPerTopic = topicIds.length > 1 ? Number.parseInt(process.env.MAX_DISPLAY_ARTICLES_PER_TOPIC || "4", 10) : limit;
  const buckets = new Map(topicIds.map((topicId) => [topicId, []]));
  const topicCounts = new Map(topicIds.map((topicId) => [topicId, 0]));
  const selected = [];
  const seen = new Set();

  articles.forEach((article) => {
    if (buckets.has(article.topicId)) {
      buckets.get(article.topicId).push(article);
    }
  });

  function tryAdd(article) {
    const key = `${article.link}|${article.title}`;
    if (seen.has(key)) {
      return;
    }
    if ((topicCounts.get(article.topicId) || 0) >= maxPerTopic) {
      return;
    }

    selected.push(article);
    seen.add(key);
    topicCounts.set(article.topicId, (topicCounts.get(article.topicId) || 0) + 1);
  }

  const xtechArticles = articles.filter((article) => article.matchedTrustedDomain === "xtech.nikkei.com");
  for (const article of xtechArticles.slice(0, topicIds.length > 1 ? 2 : 1)) {
    tryAdd(article);
  }

  for (const topicId of topicIds) {
    for (const article of (buckets.get(topicId) || []).slice(0, 2)) {
      tryAdd(article);
    }
  }

  for (const article of articles) {
    if (selected.length >= limit) {
      break;
    }
    tryAdd(article);
  }

  return selected.slice(0, limit);
}

app.use(express.json({ limit: "1mb" }));
app.use("/outputs", express.static(outputsDir));
app.use(express.static(publicDir));

app.get("/healthz", (_req, res) => {
  res.json({
    ok: true,
    cwd: process.cwd(),
    publicDir,
    outputsDir
  });
});

app.get("/api/options", async (_req, res) => {
  res.json({
    topics: TOPIC_DEFINITIONS,
    viewpoints: VIEWPOINT_DEFINITIONS,
    audioEnabled: Boolean(process.env.OPENAI_API_KEY),
    importEnabled: true,
    authenticatedSourceStatus: await getNikkeiLoginStatus()
  });
});

app.get("/api/auth/nikkei/status", async (_req, res) => {
  res.json(await getNikkeiLoginStatus());
});

app.post("/api/auth/nikkei/login", async (_req, res) => {
  try {
    const result = await loginToNikkeiAndPersistSession({ force: true });
    if (!result.ok) {
      if (result.reason === "otp-required") {
        return res.json({
          ok: false,
          otpRequired: true,
          result,
          status: await getNikkeiLoginStatus()
        });
      }

      return res.status(400).json({
        error: "日経グループサイトの自動ログインに失敗しました。",
        details: result.details || result.reason || "login_failed",
        status: await getNikkeiLoginStatus()
      });
    }

    return res.json({ ok: true, result, status: await getNikkeiLoginStatus() });
  } catch (error) {
    return res.status(500).json({
      error: "日経グループサイトの自動ログインでエラーが発生しました。",
      details: error instanceof Error ? error.message : String(error)
    });
  }
});

app.post("/api/auth/nikkei/otp", async (req, res) => {
  try {
    const result = await submitNikkeiOtpAndPersistSession(req.body?.code);
    if (!result.ok) {
      return res.status(400).json({
        error: "日経グループサイトのワンタイムパスワード認証に失敗しました。",
        details: result.details || result.reason || "otp_failed",
        status: await getNikkeiLoginStatus()
      });
    }

    return res.json({ ok: true, result, status: await getNikkeiLoginStatus() });
  } catch (error) {
    return res.status(500).json({
      error: "日経グループサイトのワンタイムパスワード認証でエラーが発生しました。",
      details: error instanceof Error ? error.message : String(error)
    });
  }
});

app.get("/api/imports", async (_req, res) => {
  const articles = await listImportedArticles();
  res.json({ articles });
});

app.post("/api/imports", async (req, res) => {
  try {
    const { url, title, content, topicId } = req.body || {};
    if (!url || !title || !content || !topicId) {
      return res.status(400).json({ error: "url, title, content, topicId は必須です。" });
    }

    const topic = TOPIC_DEFINITIONS.find((item) => item.id === topicId);
    if (!topic) {
      return res.status(400).json({ error: "topicId が不正です。" });
    }

    const article = {
      title: String(title).trim(),
      link: String(url).trim(),
      pubDate: new Date().toISOString(),
      source: new URL(String(url)).hostname,
      contentSnippet: String(content).replace(/\s+/g, " ").trim().slice(0, 4000),
      notebooklmText: String(content).trim(),
      topicId: topic.id,
      topicLabel: topic.label,
      query: "manual-import",
      sourceType: "imported-browser-article",
      baseTrust: 6,
      importedAt: new Date().toISOString()
    };

    await addImportedArticle(article);
    return res.json({ ok: true, article });
  } catch (error) {
    return res.status(500).json({
      error: "取り込み済み記事の保存に失敗しました。",
      details: error instanceof Error ? error.message : String(error)
    });
  }
});

app.delete("/api/imports", async (req, res) => {
  try {
    const link = String(req.body?.link || "").trim();
    if (!link) {
      return res.status(400).json({ error: "削除対象の link は必須です。" });
    }

    const removed = await removeImportedArticle(link);
    if (!removed) {
      return res.status(404).json({ error: "対象の取り込み済み記事が見つかりませんでした。" });
    }

    return res.json({ ok: true, link });
  } catch (error) {
    return res.status(500).json({
      error: "取り込み済み記事の削除に失敗しました。",
      details: error instanceof Error ? error.message : String(error)
    });
  }
});

app.post("/api/generate", async (req, res) => {
  try {
    const topicIds = Array.isArray(req.body?.topicIds) ? req.body.topicIds : [];
    const viewpointIds = Array.isArray(req.body?.viewpointIds) ? req.body.viewpointIds : [];
    const outputFormats = Array.isArray(req.body?.outputFormats) ? req.body.outputFormats : ["text"];

    if (!topicIds.length || !viewpointIds.length) {
      return res.status(400).json({ error: "topicIds と viewpointIds を少なくとも1つずつ指定してください。" });
    }

    const importedArticles = (await listImportedArticles()).filter((article) => topicIds.includes(article.topicId));
    const rawNewsData = await fetchTopicNewsWithImports({ topicIds, viewpointIds, importedArticles });
    const localizedNewsData = await localizeNewsData(rawNewsData);
    const summary = await generateSummary(localizedNewsData);
    const files = {};

    if (outputFormats.includes("text")) {
      const textPath = await saveTextSummary(summary, topicIds);
      files.text = `/outputs/${path.basename(textPath)}`;
    }

    if (outputFormats.includes("word")) {
      const wordPath = await saveWordSummary(summary, topicIds);
      files.word = `/outputs/${path.basename(wordPath)}`;
    }

    if (outputFormats.includes("audio")) {
      const audioBuffer = await generateAudio(summary);
      files.audio = audioBuffer ? `/outputs/${path.basename(await saveAudioSummary(audioBuffer, topicIds))}` : null;
    }

    if (outputFormats.includes("notebooklm")) {
      const researchNotes = await generateNotebookLmResearchNotes(localizedNewsData);
      const notebookPath = await saveNotebookLmBundle(rawNewsData, topicIds, { researchNotes });
      files.notebooklm = `/outputs/${path.basename(notebookPath)}`;
    }

    return res.json({
      summary,
      generatedAt: localizedNewsData.generatedAt,
      articleCount: localizedNewsData.articles.length,
      importedArticleCount: importedArticles.length,
      articles: pickDiverseArticles(localizedNewsData.articles, topicIds, 12),
      files,
      usedOpenAI: Boolean(process.env.OPENAI_API_KEY)
    });
  } catch (error) {
    console.error(error);
    return res.status(500).json({
      error: "ニュース生成中にエラーが発生しました。",
      details: error instanceof Error ? error.message : String(error)
    });
  }
});

app.get(/.*/, (_req, res) => {
  res.sendFile(path.join(publicDir, "index.html"));
});

app.listen(port, host, () => {
  console.log(`News digest app running on http://${host}:${port}`);
  console.log(`Resolved paths cwd=${process.cwd()} public=${publicDir} outputs=${outputsDir}`);
});
