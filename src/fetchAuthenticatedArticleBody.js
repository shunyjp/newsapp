import "dotenv/config";

import { enrichAuthenticatedArticles } from "./authenticatedFetchService.js";

async function main() {
  const url = process.argv[2] || "";
  if (!url) {
    process.stdout.write("{}");
    return;
  }

  const [result] = await enrichAuthenticatedArticles([
    {
      link: url,
      title: "",
      sourceType: "python-bridge"
    }
  ]);

  const payload = {
    text: result?.contentSnippet || "",
    title: result?.title || "",
    authenticatedFetch: Boolean(result?.authenticatedFetch),
    reason: result?.authenticatedFetchReason || "",
    statusCode: Number.isInteger(result?.authenticatedStatusCode) ? result.authenticatedStatusCode : null
  };
  process.stdout.write(JSON.stringify(payload));
}

main().catch(() => {
  process.stdout.write("{}");
});
