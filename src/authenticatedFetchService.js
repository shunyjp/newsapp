import { AUTHENTICATED_DOMAIN_SETTINGS } from "./config.js";
import { getSavedNikkeiCookieHeader, loginToNikkeiAndPersistSession } from "./nikkeiLoginService.js";

const AUTH_FETCH_LIMIT = Number.parseInt(process.env.AUTH_FETCH_LIMIT || "5", 10);

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

function findDomainSettings(articleOrLink) {
  const article = typeof articleOrLink === "string" ? { link: articleOrLink } : articleOrLink;
  return Object.entries(AUTHENTICATED_DOMAIN_SETTINGS).find(([domain]) => {
    return article.link?.includes(domain) || article.matchedTrustedDomain === domain;
  });
}

function extractMeta(html, name) {
  const pattern = new RegExp(`<meta[^>]+(?:property|name)=["']${name}["'][^>]+content=["']([^"']+)["']`, "i");
  return stripTags(html.match(pattern)?.[1] || "");
}

function extractTitle(html) {
  return extractMeta(html, "og:title") || stripTags(html.match(/<title[^>]*>([\s\S]*?)<\/title>/i)?.[1] || "");
}

function extractBodyRoot(html) {
  const candidates = [
    /<article[^>]*>([\s\S]*?)<\/article>/i,
    /<main[^>]*>([\s\S]*?)<\/main>/i,
    /<div[^>]+class=["'][^"']*(?:article|content|body|page-main|container_campx|cmn-article_body)[^"']*["'][^>]*>([\s\S]*?)<\/div>/i
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
    .filter((text) => !/利用規約|会員登録|ログイン|この記事は会員限定|広告|この記事をお読みいただくには/i.test(text))
    .slice(0, 14);
}

async function resolveCookieHeader(settings) {
  if (settings.cookieEnvKey && process.env[settings.cookieEnvKey]) {
    return process.env[settings.cookieEnvKey];
  }

  const savedCookie = await getSavedNikkeiCookieHeader();
  if (savedCookie) {
    return savedCookie;
  }

  const loginResult = await loginToNikkeiAndPersistSession();
  if (!loginResult.ok) {
    return "";
  }

  return getSavedNikkeiCookieHeader();
}

async function fetchAuthenticatedArticle(article) {
  const matched = findDomainSettings(article);
  if (!matched) {
    return {
      ...article,
      authenticatedFetch: false,
      authenticatedFetchReason: "unsupported_domain"
    };
  }

  const [domain, settings] = matched;
  const cookieHeader = await resolveCookieHeader(settings);
  if (!cookieHeader) {
    return {
      ...article,
      authenticatedFetch: false,
      authenticatedFetchReason: "missing_cookie"
    };
  }

  const response = await fetch(article.link, {
    headers: {
      Cookie: cookieHeader,
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122 Safari/537.36",
      Accept: "text/html,application/xhtml+xml"
    }
  });

  if (!response.ok) {
    return {
      ...article,
      authenticatedFetch: false,
      authenticatedFetchReason: `http_status:${response.status}`,
      authenticatedStatusCode: response.status
    };
  }

  const rawBytes = new Uint8Array(await response.arrayBuffer());
  const html = decodeDocument(rawBytes, response.headers.get("content-type") || "");
  const paragraphs = extractParagraphs(html);
  if (!paragraphs.length) {
    return {
      ...article,
      title: extractTitle(html) || article.title,
      authenticatedFetch: false,
      authenticatedFetchReason: "empty_paragraphs",
      authenticatedStatusCode: response.status
    };
  }

  return {
    ...article,
    title: extractTitle(html) || article.title,
    contentSnippet: paragraphs.join(" ").slice(0, 4000),
    notebooklmText: paragraphs.join("\n\n"),
    source: article.source || settings.label || domain,
    sourceType: `${article.sourceType || "trusted-domain-search"}+authenticated-fetch`,
    baseTrust: (article.baseTrust || 0) + settings.trustBonus,
    authenticatedFetch: true,
    authenticatedFetchReason: "success",
    authenticatedStatusCode: response.status
  };
}

export async function enrichAuthenticatedArticles(articles) {
  const targets = articles
    .filter((article) => findDomainSettings(article.link))
    .sort((a, b) => new Date(b.pubDate || 0) - new Date(a.pubDate || 0))
    .slice(0, AUTH_FETCH_LIMIT);

  const upgrades = await Promise.all(
    targets.map(async (article) => {
      try {
        return await fetchAuthenticatedArticle(article);
      } catch {
        return article;
      }
    })
  );

  const upgradedByLink = new Map(upgrades.map((article) => [article.link, article]));
  return articles.map((article) => upgradedByLink.get(article.link) || article);
}
