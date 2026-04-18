import test from "node:test";
import assert from "node:assert/strict";

import { selectAuthenticatedArticleTargets } from "../src/authenticatedFetchService.js";

test("selectAuthenticatedArticleTargets keeps Nikkei domains represented", () => {
  const articles = [
    {
      link: "https://xtech.nikkei.com/atcl/nxt/news/24/00005/",
      matchedTrustedDomain: "xtech.nikkei.com",
      pubDate: "2026-04-19T10:00:00Z"
    },
    {
      link: "https://xtech.nikkei.com/atcl/nxt/news/24/00004/",
      matchedTrustedDomain: "xtech.nikkei.com",
      pubDate: "2026-04-19T09:00:00Z"
    },
    {
      link: "https://xtech.nikkei.com/atcl/nxt/news/24/00003/",
      matchedTrustedDomain: "xtech.nikkei.com",
      pubDate: "2026-04-19T08:00:00Z"
    },
    {
      link: "https://www.nikkei.com/article/DGXZQOUC0000000000000000000001/",
      matchedTrustedDomain: "nikkei.com",
      pubDate: "2026-04-19T07:00:00Z"
    },
    {
      link: "https://www.nikkei.com/article/DGXZQOUC0000000000000000000002/",
      matchedTrustedDomain: "nikkei.com",
      pubDate: "2026-04-19T06:00:00Z"
    },
    {
      link: "https://financial.nikkei.com/article/DGXZQOUB0000000000000000000001/",
      matchedTrustedDomain: "financial.nikkei.com",
      pubDate: "2026-04-19T05:00:00Z"
    }
  ];

  const selected = selectAuthenticatedArticleTargets(articles, 4);

  assert.equal(selected.length, 4);
  assert.ok(selected.some((article) => article.link.includes("xtech.nikkei.com")));
  assert.ok(selected.some((article) => article.link.includes("www.nikkei.com/article/")));
  assert.ok(selected.some((article) => article.link.includes("financial.nikkei.com/article/")));
});
