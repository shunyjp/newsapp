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

export async function saveNotebookLmBundle(newsData, topicIds) {
  await ensureOutputDir();

  const sections = newsData.articles.map((article, index) => {
    return [
      `# Article ${index + 1}`,
      `Topic: ${article.topicLabel}`,
      `Title: ${article.title}`,
      `Source: ${article.source || "unknown"}`,
      `Date: ${article.pubDate || "unknown"}`,
      `Link: ${article.link}`,
      `Source Type: ${article.sourceType || "unknown"}`,
      "",
      article.contentSnippet || "",
      ""
    ].join("\n");
  });

  const documentText = [
    "# NotebookLM Source Bundle",
    `Generated At: ${new Date(newsData.generatedAt).toISOString()}`,
    `Topics: ${newsData.topics.map((topic) => topic.label).join(" / ")}`,
    `Article Count: ${newsData.articles.length}`,
    "",
    ...sections
  ].join("\n");

  const fileName = `${createBaseName(topicIds)}-notebooklm.md`;
  const fullPath = path.join(outputDir, fileName);
  await fs.writeFile(fullPath, documentText, "utf8");
  return fullPath;
}

function createBaseName(topicIds) {
  const topicPart = topicIds.map(slugify).join("-");
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  return `news-digest-${topicPart}-${timestamp}`;
}
