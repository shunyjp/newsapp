import Parser from "rss-parser";
import { TOPIC_DEFINITIONS, VIEWPOINT_DEFINITIONS } from "./config.js";
import { enrichAuthenticatedArticles } from "./authenticatedFetchService.js";

const parser = new Parser({
  customFields: {
    item: ["media:content", "source"]
  }
});

const NEWS_LIMIT_PER_QUERY = Number.parseInt(process.env.NEWS_LIMIT_PER_QUERY || "4", 10);
const AI_NEWS_LIMIT_PER_QUERY = Number.parseInt(process.env.AI_NEWS_LIMIT_PER_QUERY || "8", 10);
const RECENT_DAYS = Number.parseInt(process.env.RECENT_DAYS || "30", 10);
const TARGET_DOMESTIC_RATIO = 4 / 7;
const MAX_SELECTED_ARTICLES = Number.parseInt(process.env.MAX_SELECTED_ARTICLES || "36", 10);
const AI_MAX_SELECTED_ARTICLES = Number.parseInt(process.env.AI_MAX_SELECTED_ARTICLES || "48", 10);
const MIN_DIRECT_XTECH = Number.parseInt(process.env.MIN_DIRECT_XTECH || "2", 10);
const MIN_DIRECT_NIKKEI_GROUP = Number.parseInt(process.env.MIN_DIRECT_NIKKEI_GROUP || "2", 10);
const NIKKEI_XTECH_ONLY_DOMAINS = new Set(["xtech.nikkei.com", "nikkei.com"]);

const DIRECT_NIKKEI_SOURCES = {
  "xtech.nikkei.com": {
    url: "https://xtech.nikkei.com/rss/index.rdf",
    mode: "rss",
    sourceType: "nikkei-direct-rss",
    sourceLabel: "Nikkei xTECH"
  },
  "bizgate.nikkei.com": {
    url: "https://bizgate.nikkei.com/search?keyword=%E7%94%9F%E6%88%90AI",
    mode: "html",
    sourceType: "nikkei-direct-search",
    sourceLabel: "NIKKEI BizGate",
    entryUrlPattern: /^https:\/\/bizgate\.nikkei\.com\/article\/.+/i
  },
  "financial.nikkei.com": {
    url: "https://financial.nikkei.com/search?keyword=AI",
    mode: "html",
    sourceType: "nikkei-direct-search",
    sourceLabel: "NIKKEI Financial",
    entryUrlPattern: /^https:\/\/financial\.nikkei\.com\/article\/.+/i
  },
  "nikkei.com": {
    url: "https://www.nikkei.com/search?keyword=AI",
    mode: "html",
    sourceType: "nikkei-direct-search",
    sourceLabel: "日本経済新聞",
    entryUrlPattern: /^https:\/\/www\.nikkei\.com\/article\/.+/i
  }
};

const AI_OFFICIAL_HTML_SOURCES = [
  {
    url: "https://openai.com/news/",
    sourceType: "ai-official-source",
    sourceLabel: "OpenAI Blog",
    matchedTrustedDomain: "openai.com",
    entryUrlPattern: /^https:\/\/openai\.com\/(?:index|news)\/.+/i
  },
  {
    url: "https://www.anthropic.com/news",
    sourceType: "ai-official-source",
    sourceLabel: "Anthropic News",
    matchedTrustedDomain: "anthropic.com",
    entryUrlPattern: /^https:\/\/www\.anthropic\.com\/news\/.+/i
  },
  {
    url: "https://deepmind.google/blog/",
    sourceType: "ai-official-source",
    sourceLabel: "Google DeepMind Blog",
    matchedTrustedDomain: "deepmind.google",
    entryUrlPattern: /^https:\/\/deepmind\.google\/blog\/.+/i
  },
  {
    url: "https://blogs.microsoft.com/blog/tag/ai/",
    sourceType: "ai-official-source",
    sourceLabel: "Microsoft AI Blog",
    matchedTrustedDomain: "microsoft.com",
    entryUrlPattern: /^https:\/\/blogs\.microsoft\.com\/blog\/\d{4}\/\d{2}\/\d{2}\/.+/i
  },
  {
    url: "https://aws.amazon.com/blogs/machine-learning/",
    sourceType: "ai-official-source",
    sourceLabel: "AWS Machine Learning Blog",
    matchedTrustedDomain: "aws.amazon.com",
    entryUrlPattern: /^https:\/\/aws\.amazon\.com\/blogs\/machine-learning\/.+/i
  },
  {
    url: "https://huggingface.co/blog",
    sourceType: "ai-official-source",
    sourceLabel: "Hugging Face Blog",
    matchedTrustedDomain: "huggingface.co",
    entryUrlPattern: /^https:\/\/huggingface\.co\/blog\/[^/#?]+\/?$/i
  }
];

function containsJapanese(text = "") {
  return /[\u3040-\u30ff\u3400-\u9fff]/.test(text);
}

function isNikkeiGroupArticle(article) {
  const domain = article.matchedTrustedDomain || "";
  const source = (article.source || "").toLowerCase();
  const link = (article.link || "").toLowerCase();
  return (
    domain.includes("nikkei.com") ||
    link.includes("nikkei.com") ||
    source.includes("日経") ||
    source.includes("nikkei")
  );
}

function classifyRegion(article) {
  const domain = (article.matchedTrustedDomain || "").toLowerCase();
  const source = (article.source || "").toLowerCase();
  const link = (article.link || "").toLowerCase();
  const text = `${article.title || ""} ${article.contentSnippet || ""}`;

  const domesticDomains = [
    "nikkei.com",
    "xtech.nikkei.com",
    "financial.nikkei.com",
    "bizgate.nikkei.com",
    "itmedia.co.jp",
    "ascii.jp"
  ];

  if (
    domesticDomains.some((item) => domain.includes(item) || link.includes(item)) ||
    source.includes("日経") ||
    source.includes("nikkei")
  ) {
    return "domestic";
  }

  if (containsJapanese(text) && !link.includes("sap.com") && !link.includes("infoq.com")) {
    return "domestic";
  }

  return "international";
}

function nikkeiCaseBusinessBoost(article, viewpointId = "") {
  if (!isNikkeiGroupArticle(article)) {
    return 0;
  }

  const text = `${article.title || ""} ${article.contentSnippet || ""}`.toLowerCase();
  const companyInitiativeKeywords = [
    "導入",
    "採用",
    "活用",
    "提携",
    "協業",
    "実証",
    "改革",
    "構築",
    "刷新",
    "展開",
    "拡大",
    "投資",
    "戦略",
    "implementation",
    "deployment",
    "adoption",
    "partnership",
    "initiative",
    "rollout",
    "investment",
    "strategy"
  ];
  const caseKeywords = [
    "事例",
    "導入事例",
    "ユーザー企業",
    "採用事例",
    "活用事例",
    "顧客",
    "現場",
    "成功例",
    "case study",
    "customer",
    "example",
    "deployment"
  ];

  let boost = 0;
  if (companyInitiativeKeywords.some((keyword) => text.includes(keyword))) {
    boost += 2;
  }
  if (caseKeywords.some((keyword) => text.includes(keyword))) {
    boost += 3;
  }
  if (/\[pr\]|\bpr\b/.test(text)) {
    boost -= 2;
  }

  if (viewpointId === "business" && boost > 0) {
    boost += 2;
  }
  if (viewpointId === "cases" && boost > 0) {
    boost += 3;
  }

  return boost;
}

function buildSearchUrl(query) {
  const url = new URL("https://news.google.com/rss/search");
  url.searchParams.set("q", `${query} when:7d`);
  url.searchParams.set("hl", "ja");
  url.searchParams.set("gl", "JP");
  url.searchParams.set("ceid", "JP:ja");
  return url.toString();
}

function buildDomainSearchUrl(query, domain) {
  return buildSearchUrl(`${query} site:${domain}`);
}

function resolveTopic(topicId) {
  return TOPIC_DEFINITIONS.find((topic) => topic.id === topicId);
}

function resolveViewpoint(viewpointId) {
  return VIEWPOINT_DEFINITIONS.find((viewpoint) => viewpoint.id === viewpointId);
}

function cleanText(value = "") {
  return value.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
}

function shouldIgnoreArticle(article) {
  const title = (article.title || "").trim();
  const normalized = title.toLowerCase();
  return normalized.includes("sap for me") || /^(【\s*ad\s*】|\[\s*ad\s*\]|ad[:：]\s*)/i.test(title);
}

function splitTitleAndSource(rawTitle = "") {
  const title = cleanText(rawTitle);
  const parts = title.split(" - ");
  if (parts.length < 2) {
    return { title, source: "" };
  }

  const source = parts.at(-1)?.trim() || "";
  return {
    title: parts.slice(0, -1).join(" - ").trim(),
    source
  };
}

function normalizeFeedItem(item, extra = {}) {
  const parsed = splitTitleAndSource(item.title || "");
  const article = {
    title: parsed.title,
    link: item.link || "",
    pubDate: item.pubDate || item.isoDate || "",
    source: cleanText(item.source?.["_"] || item.creator || parsed.source || extra.defaultSource || ""),
    contentSnippet: cleanText(item.contentSnippet || item.content || item.summary || ""),
    ...extra
  };

  return {
    ...article,
    region: extra.region || classifyRegion(article)
  };
}

function getQueryLimit(topic) {
  return topic?.id === "ai" ? AI_NEWS_LIMIT_PER_QUERY : NEWS_LIMIT_PER_QUERY;
}

function getMaxArticleLimit(topics) {
  const hasAiOnly = topics.length === 1 && topics[0]?.id === "ai";
  return hasAiOnly ? Math.max(AI_MAX_SELECTED_ARTICLES, topics.length * 14) : Math.max(MAX_SELECTED_ARTICLES, topics.length * 12);
}

function isGoogleNewsUrl(link = "") {
  return /^https:\/\/news\.google\.com\//i.test(link);
}

function getUrlHost(link = "") {
  try {
    return new URL(link).hostname.toLowerCase();
  } catch {
    return "";
  }
}

function isDirectDomainArticle(article, domain) {
  const host = getUrlHost(article.link || article.url || "");
  return Boolean(host) && host === domain;
}

function isDirectNikkeiGroupArticle(article) {
  const host = getUrlHost(article.link || article.url || "");
  return [
    "xtech.nikkei.com",
    "bizgate.nikkei.com",
    "financial.nikkei.com",
    "www.nikkei.com"
  ].includes(host);
}

async function fetchDirectNikkeiCandidates(domain, topic) {
  const settings = DIRECT_NIKKEI_SOURCES[domain];
  if (!settings) {
    return [];
  }

  const limit = getQueryLimit(topic);

  if (settings.mode === "rss") {
    const feed = await parser.parseURL(settings.url);
    return (feed.items || [])
      .filter((item) => isRecentEnough(item.pubDate || item.isoDate || ""))
      .slice(0, limit)
      .map((item) =>
        normalizeFeedItem(item, {
          topicId: topic.id,
          topicLabel: topic.label,
          query: settings.sourceLabel,
          sourceType: settings.sourceType,
          baseTrust: 5,
          matchedTrustedDomain: domain,
          source: settings.sourceLabel
        })
      );
  }

  const response = await fetch(settings.url, {
    headers: {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122 Safari/537.36",
      Accept: "text/html,application/xhtml+xml"
    },
    redirect: "follow"
  });
  const html = await response.text();
  const matches = [...html.matchAll(/<a\b[^>]+href=["']([^"'#]+)["'][^>]*>([\s\S]*?)<\/a>/gi)];
  const seen = new Set();
  const articles = [];

  for (const match of matches) {
    const href = match[1];
    const title = cleanText(match[2] || "");
    if (!title || /^(【\s*ad\s*】|\[\s*ad\s*\]|ad[:：]\s*)/i.test(title)) {
      continue;
    }

    let absoluteUrl = "";
    try {
      absoluteUrl = new URL(href, settings.url).toString();
    } catch {
      continue;
    }

    if (!settings.entryUrlPattern.test(absoluteUrl) || seen.has(absoluteUrl)) {
      continue;
    }

    seen.add(absoluteUrl);
    articles.push({
      title,
      link: absoluteUrl,
      pubDate: "",
      source: settings.sourceLabel,
      contentSnippet: "",
      topicId: topic.id,
      topicLabel: topic.label,
      query: settings.sourceLabel,
      sourceType: settings.sourceType,
      baseTrust: 5,
      matchedTrustedDomain: domain,
      region: "domestic"
    });

    if (articles.length >= limit) {
      break;
    }
  }

  return articles;
}

async function fetchOfficialAiCandidates(topic) {
  const limit = getQueryLimit(topic);
  const jobs = AI_OFFICIAL_HTML_SOURCES.map(async (settings) => {
    try {
      const response = await fetch(settings.url, {
        headers: {
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122 Safari/537.36",
          Accept: "text/html,application/xhtml+xml"
        },
        redirect: "follow"
      });
      const html = await response.text();
      const matches = [...html.matchAll(/<a\b[^>]+href=["']([^"'#]+)["'][^>]*>([\s\S]*?)<\/a>/gi)];
      const seen = new Set();
      const articles = [];

      for (const match of matches) {
        const href = match[1];
        const title = cleanText(match[2] || "");
        if (!title || shouldIgnoreArticle({ title })) {
          continue;
        }

        let absoluteUrl = "";
        try {
          absoluteUrl = new URL(href, settings.url).toString();
        } catch {
          continue;
        }

        if (!settings.entryUrlPattern.test(absoluteUrl) || seen.has(absoluteUrl)) {
          continue;
        }

        seen.add(absoluteUrl);
        articles.push({
          title,
          link: absoluteUrl,
          pubDate: "",
          source: settings.sourceLabel,
          contentSnippet: "",
          topicId: topic.id,
          topicLabel: topic.label,
          query: settings.sourceLabel,
          sourceType: settings.sourceType,
          baseTrust: 5,
          matchedTrustedDomain: settings.matchedTrustedDomain,
          region: "international"
        });

        if (articles.length >= limit) {
          break;
        }
      }

      return articles;
    } catch {
      return [];
    }
  });

  return (await Promise.all(jobs)).flat();
}

function inferViewpointScore(text, viewpoint) {
  const normalized = text.toLowerCase();
  const dictionaries = {
    technology: ["launch", "model", "ai", "agent", "security", "platform", "upgrade", "api", "architecture"],
    business: ["revenue", "market", "investment", "funding", "partnership", "acquisition", "price", "forecast"],
    cases: ["customer", "deployment", "adoption", "implementation", "case study", "example", "migrated"],
    learning: ["guide", "tutorial", "course", "training", "webinar", "how to", "learn", "certification"]
  };

  return (dictionaries[viewpoint.id] || []).reduce((score, keyword) => {
    return score + (normalized.includes(keyword) ? 1 : 0);
  }, 0);
}

function dedupeArticles(items) {
  const byKey = new Map();

  for (const item of items) {
    const key = `${item.link}|${item.title}`;
    const existing = byKey.get(key);
    if (!existing) {
      byKey.set(key, item);
      continue;
    }

    const existingScore =
      (existing.baseTrust || 0) +
      (existing.matchedTrustedDomain ? 3 : 0) +
      ((existing.sourceType || "").includes("authenticated-fetch") ? 2 : 0);
    const nextScore =
      (item.baseTrust || 0) +
      (item.matchedTrustedDomain ? 3 : 0) +
      ((item.sourceType || "").includes("authenticated-fetch") ? 2 : 0);

    if (nextScore > existingScore) {
      byKey.set(key, { ...existing, ...item });
    } else {
      byKey.set(key, { ...item, ...existing });
    }
  }

  return [...byKey.values()];
}

function isRecentEnough(pubDate, maxAgeDays = RECENT_DAYS) {
  if (!pubDate) {
    return true;
  }

  const publishedAt = new Date(pubDate);
  if (Number.isNaN(publishedAt.getTime())) {
    return true;
  }

  const ageMs = Date.now() - publishedAt.getTime();
  return ageMs <= maxAgeDays * 24 * 60 * 60 * 1000;
}

function trustScore(article, topic) {
  let score = article.baseTrust || 0;

  if (topic.trustedDomains?.some((domain) => article.link.includes(domain) || article.matchedTrustedDomain === domain)) {
    score += 3;
  }

  const sourceLower = (article.source || "").toLowerCase();
  if (sourceLower.includes("sap")) {
    score += 2;
  }
  if (sourceLower.includes("cio") || sourceLower.includes("computer weekly") || sourceLower.includes("infoq")) {
    score += 1;
  }
  if ((article.sourceType || "").includes("authenticated-fetch")) {
    score += 2;
  }
  if (article.sourceType === "ai-official-source") {
    score += 3;
  }
  if (topic?.id === "ai" && article.matchedTrustedDomain && ["openai.com", "anthropic.com", "deepmind.google", "microsoft.com", "aws.amazon.com", "huggingface.co"].includes(article.matchedTrustedDomain)) {
    score += 2;
  }

  score += nikkeiCaseBusinessBoost(article);

  return score;
}

function sortArticles(a, b) {
  if (b.trustScore !== a.trustScore) {
    return b.trustScore - a.trustScore;
  }
  return new Date(b.pubDate || 0) - new Date(a.pubDate || 0);
}

function addUniqueArticle(article, selected, selectedKeys) {
  const key = `${article.link}|${article.title}`;
  if (selectedKeys.has(key)) {
    return false;
  }
  selected.push(article);
  selectedKeys.add(key);
  return true;
}

function isNikkeiXtechOnlyArticle(article) {
  const domain = (article.matchedTrustedDomain || "").toLowerCase();
  const link = (article.link || "").toLowerCase();
  return (
    domain === "xtech.nikkei.com" ||
    domain === "nikkei.com" ||
    link.includes("://xtech.nikkei.com/") ||
    link.includes("://www.nikkei.com/")
  );
}

function applySourceModeFilter(articles, sourceMode) {
  if (sourceMode !== "nikkei_xtech_only") {
    return articles;
  }
  return articles.filter((article) => isNikkeiXtechOnlyArticle(article));
}

function rebalanceRegions(rankedArticles, selected, selectedKeys, maxArticles) {
  const targetDomestic = Math.round(maxArticles * TARGET_DOMESTIC_RATIO);
  const targetInternational = maxArticles - targetDomestic;
  let domesticCount = selected.filter((article) => article.region === "domestic").length;
  let internationalCount = selected.filter((article) => article.region === "international").length;

  for (const article of rankedArticles) {
    if (selected.length >= maxArticles) {
      break;
    }

    if (article.region === "domestic" && domesticCount >= targetDomestic) {
      continue;
    }
    if (article.region === "international" && internationalCount >= targetInternational) {
      continue;
    }

    if (addUniqueArticle(article, selected, selectedKeys)) {
      if (article.region === "domestic") {
        domesticCount += 1;
      } else {
        internationalCount += 1;
      }
    }
  }

  for (const article of rankedArticles) {
    if (selected.length >= maxArticles) {
      break;
    }
    addUniqueArticle(article, selected, selectedKeys);
  }
}

export async function fetchTopicNews({ topicIds, viewpointIds }) {
  return fetchTopicNewsWithImports({ topicIds, viewpointIds, importedArticles: [] });
}

export async function fetchTopicNewsWithImports({ topicIds, viewpointIds, importedArticles = [], sourceMode = "default" }) {
  const topics = topicIds.map(resolveTopic).filter(Boolean);
  const viewpoints = viewpointIds.map(resolveViewpoint).filter(Boolean);

  const rawResults = await Promise.all(
    topics.flatMap((topic) =>
      [
        ...topic.queries.map(async (query) => {
          const limit = getQueryLimit(topic);
          const feed = await parser.parseURL(buildSearchUrl(query));
          return (feed.items || []).slice(0, limit).map((item) =>
            normalizeFeedItem(item, {
              topicId: topic.id,
              topicLabel: topic.label,
              query,
              sourceType: "broad-search",
              baseTrust: 1
            })
          );
        }),
        ...(topic.trustedDomains || [])
          .filter((domain) => sourceMode !== "nikkei_xtech_only" || NIKKEI_XTECH_ONLY_DOMAINS.has(domain))
          .flatMap((domain) =>
          topic.queries.slice(0, 2).map(async (query) => {
            const limit = getQueryLimit(topic);
            const feed = await parser.parseURL(buildDomainSearchUrl(query, domain));
            return (feed.items || []).slice(0, Math.max(2, limit - 1)).map((item) =>
              normalizeFeedItem(item, {
                topicId: topic.id,
                topicLabel: topic.label,
                query: `${query} site:${domain}`,
                sourceType: "trusted-domain-search",
                baseTrust: 3,
                matchedTrustedDomain: domain
              })
            );
          })
        ),
        ...(topic.curatedFeeds || []).map(async (feedSource) => {
          const limit = getQueryLimit(topic);
          const feed = await parser.parseURL(feedSource.url);
          return (feed.items || [])
            .filter((item) => isRecentEnough(item.pubDate || item.isoDate || "", feedSource.maxAgeDays))
            .slice(0, limit)
            .map((item) =>
              normalizeFeedItem(item, {
                topicId: topic.id,
                topicLabel: topic.label,
                query: feedSource.label,
                sourceType: "curated-feed",
                baseTrust: feedSource.trust,
                defaultSource: feedSource.label
              })
            );
        }),
        ...[...new Set((topic.trustedDomains || []).filter((domain) => DIRECT_NIKKEI_SOURCES[domain]))]
          .filter((domain) => sourceMode !== "nikkei_xtech_only" || NIKKEI_XTECH_ONLY_DOMAINS.has(domain))
          .map(async (domain) => {
            return fetchDirectNikkeiCandidates(domain, topic);
          }),
        ...(topic.id === "ai" && sourceMode !== "nikkei_xtech_only" ? [fetchOfficialAiCandidates(topic)] : [])
      ].map(async (job) => {
        try {
          return await job;
        } catch (_error) {
          return [];
        }
      })
    )
  );

  const normalizedImportedArticles = importedArticles.map((article) => {
    const topic = resolveTopic(article.topicId);
    return {
      ...article,
      region: article.region || classifyRegion(article),
      trustScore: topic ? trustScore(article, topic) : article.baseTrust || 0
    };
  });

  const candidateArticles = await enrichAuthenticatedArticles([...rawResults.flat(), ...normalizedImportedArticles]);

  const rankedArticles = applySourceModeFilter(
    dedupeArticles(
    candidateArticles.map((article) => {
      const topic = resolveTopic(article.topicId);
      const normalizedArticle = {
        ...article,
        region: article.region || classifyRegion(article)
      };

      return {
        ...normalizedArticle,
        trustScore: topic ? trustScore(normalizedArticle, topic) : normalizedArticle.baseTrust || 0
      };
    })
  )
  )
    .filter((article) => !shouldIgnoreArticle(article))
    .sort(sortArticles);

  const maxArticles = getMaxArticleLimit(topics);
  const minPerTopic = topics.length > 1 ? 4 : maxArticles;
  const topicBuckets = new Map(topics.map((topic) => [topic.id, []]));

  rankedArticles.forEach((article) => {
    if (topicBuckets.has(article.topicId)) {
      topicBuckets.get(article.topicId).push(article);
    }
  });

  const selected = [];
  const selectedKeys = new Set();

  const xtechCandidates = rankedArticles.filter((article) => isDirectDomainArticle(article, "xtech.nikkei.com"));
  const nikkeiGroupCandidates = rankedArticles.filter((article) =>
    isDirectNikkeiGroupArticle(article) && !isDirectDomainArticle(article, "xtech.nikkei.com")
  );
  const minXtech = topics.length > 1 ? MIN_DIRECT_XTECH : 1;
  const minNikkeiGroup = topics.length > 1 ? MIN_DIRECT_NIKKEI_GROUP : 1;

  for (const article of xtechCandidates.slice(0, minXtech)) {
    addUniqueArticle(article, selected, selectedKeys);
  }

  for (const article of nikkeiGroupCandidates.slice(0, minNikkeiGroup)) {
    addUniqueArticle(article, selected, selectedKeys);
  }

  for (const topic of topics) {
    const bucket = topicBuckets.get(topic.id) || [];
    const domestic = bucket.filter((article) => article.region === "domestic").slice(0, 2);
    const international = bucket.filter((article) => article.region === "international").slice(0, 2);

    for (const article of [...domestic, ...international]) {
      addUniqueArticle(article, selected, selectedKeys);
    }

    for (const article of bucket.slice(0, minPerTopic)) {
      if (selected.length >= maxArticles) {
        break;
      }
      addUniqueArticle(article, selected, selectedKeys);
    }
  }

  rebalanceRegions(rankedArticles, selected, selectedKeys, maxArticles);

  const articles = selected.sort(sortArticles);

  const groupedByViewpoint = viewpoints.map((viewpoint) => {
    const scored = [...articles]
      .map((article) => ({
        ...article,
        viewpointScore:
          inferViewpointScore(`${article.title} ${article.contentSnippet}`, viewpoint) +
          nikkeiCaseBusinessBoost(article, viewpoint.id)
      }))
      .sort((a, b) => {
        if (b.trustScore !== a.trustScore) {
          return b.trustScore - a.trustScore;
        }
        if (b.viewpointScore !== a.viewpointScore) {
          return b.viewpointScore - a.viewpointScore;
        }
        return new Date(b.pubDate || 0) - new Date(a.pubDate || 0);
      });

    const perTopic = new Map(topics.map((topic) => [topic.id, []]));
    scored.forEach((article) => {
      if (perTopic.has(article.topicId)) {
        perTopic.get(article.topicId).push(article);
      }
    });

    const ranked = [];
    const rankedKeys = new Set();
    const viewpointLimit = topics.length > 1 ? 7 : (topics[0]?.id === "ai" ? 8 : 6);

    const xtechFirst = scored.find((article) => isDirectDomainArticle(article, "xtech.nikkei.com"));
    if (xtechFirst) {
      const key = `${xtechFirst.link}|${xtechFirst.title}`;
      ranked.push(xtechFirst);
      rankedKeys.add(key);
    }

    const nikkeiFirst = scored.find((article) => isDirectNikkeiGroupArticle(article) && !isDirectDomainArticle(article, "xtech.nikkei.com"));
    if (nikkeiFirst) {
      const key = `${nikkeiFirst.link}|${nikkeiFirst.title}`;
      if (!rankedKeys.has(key)) {
        ranked.push(nikkeiFirst);
        rankedKeys.add(key);
      }
    }

    for (const topic of topics) {
      const domestic = perTopic.get(topic.id).find((article) => article.region === "domestic");
      const international = perTopic.get(topic.id).find((article) => article.region === "international");

      for (const article of [domestic, international].filter(Boolean)) {
        const key = `${article.link}|${article.title}`;
        if (!rankedKeys.has(key)) {
          ranked.push(article);
          rankedKeys.add(key);
        }
      }
    }

    for (const article of scored) {
      const key = `${article.link}|${article.title}`;
      if (ranked.length >= viewpointLimit) {
        break;
      }
      if (!rankedKeys.has(key)) {
        ranked.push(article);
        rankedKeys.add(key);
      }
    }

    return {
      id: viewpoint.id,
      label: viewpoint.label,
      hint: viewpoint.hint,
      articles: ranked
    };
  });

  return {
    generatedAt: new Date().toISOString(),
    topics,
    viewpoints,
    articles,
    groupedByViewpoint
  };
}
