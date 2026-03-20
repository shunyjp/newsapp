import fs from "node:fs/promises";
import path from "node:path";

const dataDir = path.join(process.cwd(), "data");
const importFile = path.join(dataDir, "imported-articles.json");

async function ensureStore() {
  await fs.mkdir(dataDir, { recursive: true });
  try {
    await fs.access(importFile);
  } catch {
    await fs.writeFile(importFile, "[]", "utf8");
  }
}

async function writeImportedArticles(items) {
  await ensureStore();
  await fs.writeFile(importFile, JSON.stringify(items, null, 2), "utf8");
}

export async function listImportedArticles() {
  await ensureStore();
  const raw = await fs.readFile(importFile, "utf8");
  const items = JSON.parse(raw);
  return Array.isArray(items) ? items.sort((a, b) => new Date(b.importedAt || 0) - new Date(a.importedAt || 0)) : [];
}

export async function addImportedArticle(article) {
  const items = await listImportedArticles();
  const deduped = items.filter((item) => item.link !== article.link);
  deduped.unshift(article);
  await writeImportedArticles(deduped.slice(0, 200));
  return article;
}

export async function removeImportedArticle(link) {
  const items = await listImportedArticles();
  const filtered = items.filter((item) => item.link !== link);
  const removed = filtered.length !== items.length;
  if (removed) {
    await writeImportedArticles(filtered);
  }
  return removed;
}
