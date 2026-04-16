import json
import shutil
import unittest
from pathlib import Path
from uuid import uuid4

from config import CONFIG_DIR, load_structured_config
from db.database import Database
from db.repository import CanonicalItem, ItemRepository, source_record_from_dict
from pipeline.cleanup import cleanup_explicit_noise_items
from pipeline.export import export_items
from pipeline.source_config import resolve_collect_max_items, resolve_source_ids
from sources.rss.nikkei_playwright_auth import fetch_authenticated_article_body
from sources.base import CollectRequest
from sources.rss.provider import NikkeiXTechCandidateProvider, RssCandidateSourceProvider


class ProductionMinSourceSetTests(unittest.TestCase):
    def test_production_min_source_set_loads(self) -> None:
        config = load_structured_config(CONFIG_DIR / "sources.yaml")

        source_ids = resolve_source_ids(config, source_set="production_min")

        self.assertEqual(
            source_ids,
            [
                "blog.openai",
                "blog.anthropic",
                "blog.google_gemini",
                "blog.google_deepmind",
                "blog.microsoft_ai",
                "blog.aws_ml",
                "blog.huggingface",
                "blog.langchain",
                "blog.pinecone",
                "blog.weaviate",
                "docs.python_insider",
                "nikkei.bizgate.genai",
                "nikkei.xtech.candidate",
            ],
        )
        configured_ids = {source["source_id"] for source in config["sources"]}
        self.assertTrue(set(source_ids).issubset(configured_ids))

    def test_nikkei_paid_ai_source_set_loads(self) -> None:
        config = load_structured_config(CONFIG_DIR / "sources.yaml")

        source_ids = resolve_source_ids(config, source_set="nikkei_paid_ai")

        self.assertEqual(
            source_ids,
            [
                "nikkei.bizgate.genai",
                "nikkei.financial.ai",
                "nikkei.xtech.candidate",
            ],
        )

    def test_source_set_default_max_items_is_applied_when_unset(self) -> None:
        config = load_structured_config(CONFIG_DIR / "sources.yaml")

        max_items = resolve_collect_max_items(
            config,
            source_set="nikkei_focus",
            explicit_max_items=None,
            fallback_default=5,
        )

        self.assertEqual(max_items, 15)

    def test_explicit_max_items_overrides_source_set_default(self) -> None:
        config = load_structured_config(CONFIG_DIR / "sources.yaml")

        max_items = resolve_collect_max_items(
            config,
            source_set="nikkei_focus",
            explicit_max_items=3,
            fallback_default=5,
        )

        self.assertEqual(max_items, 3)


class NikkeiCandidateProviderTests(unittest.TestCase):
    def test_nikkei_candidate_provider_generates_metadata_only_items(self) -> None:
        config = load_structured_config(CONFIG_DIR / "sources.yaml")
        source = next(
            source
            for source in config["sources"]
            if source["source_id"] == "nikkei.xtech.candidate"
        )

        class FakeResponse:
            text = """
                <rss><channel>
                  <item>
                    <title>AI chip feature</title>
                    <link>https://xtech.nikkei.com/atcl/nxt/news/24/00001/</link>
                    <pubDate>Fri, 21 Mar 2026 09:00:00 +0900</pubDate>
                    <category>semiconductor</category>
                  </item>
                </channel></rss>
            """

            def raise_for_status(self) -> None:
                return None

        provider = NikkeiXTechCandidateProvider(
            source,
            http_get=lambda _url: FakeResponse(),
        )

        records = provider.collect(
            CollectRequest(source_id="nikkei.xtech.candidate", max_items=5)
        )

        self.assertEqual(len(records), 1)
        item = records[0]["item"]
        self.assertEqual(item.source_id, "nikkei.xtech.candidate")
        self.assertEqual(item.body_kind, "metadata_only")
        self.assertEqual(item.content_status, "unavailable")
        self.assertEqual(item.title, "AI chip feature")
        self.assertEqual(item.url, "https://xtech.nikkei.com/atcl/nxt/news/24/00001/")
        self.assertEqual(item.published_at, "2026-03-21T09:00:00+09:00")
        self.assertEqual(item.retrieval_diagnostics["category"], "semiconductor")
        self.assertFalse(item.raw_text)
        self.assertFalse(item.cleaned_text)

    def test_nikkei_candidate_provider_falls_back_to_html_scrape(self) -> None:
        config = load_structured_config(CONFIG_DIR / "sources.yaml")
        source = next(
            source
            for source in config["sources"]
            if source["source_id"] == "nikkei.xtech.candidate"
        )

        class HtmlResponse:
            text = """
                <html><body>
                  <a href="/atcl/nxt/column/18/00001/00001/">Nikkei XTECH AI feature story</a>
                  <a href="/atcl/nxt/news/24/00001/">Another enterprise systems article</a>
                </body></html>
            """

            def raise_for_status(self) -> None:
                return None

        def fake_get(url: str) -> object:
            if url.endswith("/rss/index.rdf"):
                raise RuntimeError("rss unavailable")
            return HtmlResponse()

        provider = NikkeiXTechCandidateProvider(source, http_get=fake_get)

        records = provider.collect(
            CollectRequest(source_id="nikkei.xtech.candidate", max_items=5)
        )

        self.assertEqual(len(records), 2)
        self.assertEqual(
            records[0]["item"].retrieval_diagnostics["candidate_fetch_mode"],
            "html_fallback",
        )
        self.assertEqual(records[0]["item"].body_kind, "metadata_only")

    def test_nikkei_candidate_provider_uses_authenticated_fallback_when_public_body_missing(self) -> None:
        config = load_structured_config(CONFIG_DIR / "sources.yaml")
        source = next(
            source
            for source in config["sources"]
            if source["source_id"] == "nikkei.xtech.candidate"
        )

        class FeedResponse:
            text = """
                <rss><channel>
                  <item>
                    <title>Subscriber-only AI modernization case study</title>
                    <link>https://xtech.nikkei.com/atcl/nxt/column/18/00001/00001/</link>
                  </item>
                </channel></rss>
            """

            def raise_for_status(self) -> None:
                return None

        class ArticleResponse:
            text = """
                <html><head><title>Subscriber-only AI modernization case study</title></head>
                <body><div class="locked">Please log in</div></body></html>
            """

            def raise_for_status(self) -> None:
                return None

        def fake_get(url: str) -> object:
            if url.endswith("/rss/index.rdf"):
                return FeedResponse()
            return ArticleResponse()

        provider = NikkeiXTechCandidateProvider(
            source,
            http_get=fake_get,
            auth_fetch=lambda _url: {
                "text": (
                    "Nikkei xTECH reports that the project standardized internal AI tooling "
                    "across application teams and reduced approval lead time for deployments."
                )
            },
        )

        records = provider.collect(
            CollectRequest(source_id="nikkei.xtech.candidate", max_items=5)
        )

        self.assertEqual(len(records), 1)
        item = records[0]["item"]
        self.assertEqual(item.body_kind, "full_text")
        self.assertEqual(item.content_status, "available")
        self.assertEqual(item.retrieval_diagnostics["body_fetch_mode"], "authenticated_html")
        self.assertTrue(item.retrieval_diagnostics["authenticated_fetch"])
        self.assertIn("standardized internal AI tooling", item.cleaned_text)

    def test_nikkei_candidate_provider_records_authenticated_fetch_failure_diagnostics(self) -> None:
        config = load_structured_config(CONFIG_DIR / "sources.yaml")
        source = next(
            source
            for source in config["sources"]
            if source["source_id"] == "nikkei.financial.ai"
        )

        class SearchResponse:
            text = """
                <html><body>
                  <a href="/article/DGXZQOGN072XC0X00C26A3000000/">AI finance article</a>
                </body></html>
            """

            def raise_for_status(self) -> None:
                return None

        class HttpError(Exception):
            pass

        class ArticleResponse:
            text = ""

            def raise_for_status(self) -> None:
                raise HttpError("403")

        def fake_get(url: str) -> object:
            if "search?keyword=AI" in url:
                return SearchResponse()
            return ArticleResponse()

        provider = NikkeiXTechCandidateProvider(
            source,
            http_get=fake_get,
            auth_fetch=lambda _url: {
                "text": "",
                "title": "Authenticated title",
                "authenticatedFetch": False,
                "reason": "http_status:403",
                "statusCode": 403,
            },
        )

        records = provider.collect(
            CollectRequest(source_id="nikkei.financial.ai", max_items=5)
        )

        self.assertEqual(len(records), 1)
        item = records[0]["item"]
        self.assertEqual(item.body_kind, "metadata_only")
        self.assertEqual(item.content_status, "unavailable")
        self.assertTrue(item.retrieval_diagnostics["authenticated_fetch_attempted"])
        self.assertFalse(item.retrieval_diagnostics["authenticated_fetch"])
        self.assertEqual(
            item.retrieval_diagnostics["authenticated_fetch_reason"],
            "http_status:403",
        )
        self.assertEqual(
            item.retrieval_diagnostics["authenticated_fetch_status_code"],
            403,
        )


class NikkeiPlaywrightAuthTests(unittest.TestCase):
    def test_fetch_authenticated_article_body_rejects_unsupported_domains(self) -> None:
        payload = fetch_authenticated_article_body("https://example.com/article/1")

        self.assertEqual(payload["text"], "")
        self.assertFalse(payload["authenticatedFetch"])
        self.assertEqual(payload["reason"], "unsupported_domain")

    def test_fetch_authenticated_article_body_returns_missing_cookie_by_default(self) -> None:
        payload = fetch_authenticated_article_body(
            "https://financial.nikkei.com/article/DGXZQOGN072XC0X00C26A3000000/"
        )

        self.assertEqual(payload["text"], "")
        self.assertFalse(payload["authenticatedFetch"])
        self.assertEqual(payload["reason"], "missing_cookie")

    def test_bizgate_candidate_provider_collects_article_links(self) -> None:
        config = load_structured_config(CONFIG_DIR / "sources.yaml")
        source = next(
            source
            for source in config["sources"]
            if source["source_id"] == "nikkei.bizgate.genai"
        )

        class HtmlResponse:
            text = """
                <html><body>
                  <a href="/article/DGXZQOLM076MU007072023000000">生成AIのリスク、三部弁護士「法を守ることが武器に」</a>
                  <a href="/projects/genai">project landing</a>
                </body></html>
            """

            def raise_for_status(self) -> None:
                return None

        provider = NikkeiXTechCandidateProvider(
            source,
            http_get=lambda _url: HtmlResponse(),
            auth_fetch=lambda _url: {
                "text": (
                    "日本経済新聞社の生成AIコンソーシアムで、企業の活用と法制度の両立が論点として整理された。"
                )
            },
        )

        records = provider.collect(
            CollectRequest(source_id="nikkei.bizgate.genai", max_items=5)
        )

        self.assertEqual(len(records), 1)
        self.assertEqual(
            records[0]["item"].url,
            "https://bizgate.nikkei.com/article/DGXZQOLM076MU007072023000000",
        )
        self.assertEqual(records[0]["item"].content_status, "available")

    def test_bizgate_candidate_provider_filters_non_ai_titles(self) -> None:
        config = load_structured_config(CONFIG_DIR / "sources.yaml")
        source = next(
            source
            for source in config["sources"]
            if source["source_id"] == "nikkei.bizgate.genai"
        )

        class HtmlResponse:
            text = """
                <html><body>
                  <a href="/article/DGXZQOLM076MU007072023000000">生成AIのリスク、三部弁護士「法を守ることが武器に」</a>
                  <a href="/article/DGXZQOLM9999999999999999999999">ゴルフの「原因」を生涯追究 元国際金融マン、異色の挑戦</a>
                </body></html>
            """

            def raise_for_status(self) -> None:
                return None

        provider = NikkeiXTechCandidateProvider(
            source,
            http_get=lambda _url: HtmlResponse(),
            auth_fetch=lambda _url: {"text": "生成AIの法制度と活用の両立を議論した。"},
        )

        records = provider.collect(
            CollectRequest(source_id="nikkei.bizgate.genai", max_items=5)
        )

        self.assertEqual(len(records), 1)
        self.assertIn("生成AI", records[0]["item"].title)


    def test_financial_candidate_provider_filters_non_ai_titles(self) -> None:
        config = load_structured_config(CONFIG_DIR / "sources.yaml")
        source = next(
            source
            for source in config["sources"]
            if source["source_id"] == "nikkei.financial.ai"
        )

        class HtmlResponse:
            text = """
                <html><body>
                  <a href="/article/DGXZQOUB135SG0T10C26A1000000">面談記録、AIで要点抽出　対話の重要性変わらず</a>
                  <a href="/article/DGXZQOUB9999999999999999999999">金利見通しと日本株</a>
                </body></html>
            """

            def raise_for_status(self) -> None:
                return None

        provider = NikkeiXTechCandidateProvider(
            source,
            http_get=lambda _url: HtmlResponse(),
            auth_fetch=lambda _url: {
                "text": "金融庁がAI活用を促し、面談記録の要点抽出を支援した。"
            },
        )

        records = provider.collect(
            CollectRequest(source_id="nikkei.financial.ai", max_items=5)
        )

        self.assertEqual(len(records), 1)
        self.assertEqual(
            records[0]["item"].url,
            "https://financial.nikkei.com/article/DGXZQOUB135SG0T10C26A1000000",
        )

    def test_main_candidate_provider_filters_non_ai_titles(self) -> None:
        config = load_structured_config(CONFIG_DIR / "sources.yaml")
        source = next(
            source
            for source in config["sources"]
            if source["source_id"] == "nikkei.main.ai"
        )

        class HtmlResponse:
            text = """
                <html><body>
                  <a href="/article/DGXZQOUC1234567890123456789012/">生成AI、日本の進路探る</a>
                  <a href="/article/DGXZQOUC9999999999999999999999/">春闘賃上げ率が上昇</a>
                </body></html>
            """

            def raise_for_status(self) -> None:
                return None

        provider = NikkeiXTechCandidateProvider(
            source,
            http_get=lambda _url: HtmlResponse(),
            auth_fetch=lambda _url: {
                "text": "生成AIの投資とガバナンスが日本企業の焦点となっている。"
            },
        )

        records = provider.collect(
            CollectRequest(source_id="nikkei.main.ai", max_items=5)
        )

        self.assertEqual(len(records), 1)
        self.assertEqual(
            records[0]["item"].url,
            "https://www.nikkei.com/article/DGXZQOUC1234567890123456789012/",
        )

    def test_nikkei_candidate_provider_scans_past_non_matching_top_results(self) -> None:
        config = load_structured_config(CONFIG_DIR / "sources.yaml")
        source = next(
            source
            for source in config["sources"]
            if source["source_id"] == "nikkei.financial.ai"
        )

        class HtmlResponse:
            text = """
                <html><body>
                  <a href="/article/DGX0000000000000000000000001/">Market headline one</a>
                  <a href="/article/DGX0000000000000000000000002/">Economy headline two</a>
                  <a href="/article/DGX0000000000000000000000003/">AI market update</a>
                </body></html>
            """

            def raise_for_status(self) -> None:
                return None

        provider = NikkeiXTechCandidateProvider(
            source,
            http_get=lambda _url: HtmlResponse(),
            auth_fetch=lambda _url: {"text": "AI related article body for canonicalization."},
        )

        records = provider.collect(
            CollectRequest(source_id="nikkei.financial.ai", max_items=1)
        )

        self.assertEqual(len(records), 1)
        self.assertEqual(
            records[0]["item"].url,
            "https://financial.nikkei.com/article/DGX0000000000000000000000003/",
        )


class RssCandidateArticleBodyTests(unittest.TestCase):
    def test_html_candidate_collection_applies_entry_url_patterns(self) -> None:
        config = load_structured_config(CONFIG_DIR / "sources.yaml")
        source = next(
            source for source in config["sources"] if source["source_id"] == "blog.google_gemini"
        )

        class HtmlResponse:
            text = """
                <html><body>
                  <a href="/products-and-platforms/products/gemini/gemini-3/">Gemini 3 launch</a>
                  <a href="/products-and-platforms/products/search/search-update/">Search update</a>
                  <a href="/products-and-platforms/products/gemini/gemini-live/">Gemini Live</a>
                </body></html>
            """

            def raise_for_status(self) -> None:
                return None

        provider = RssCandidateSourceProvider(
            source,
            http_get=lambda _url: HtmlResponse(),
        )

        records = provider.collect(
            CollectRequest(source_id="blog.google_gemini", max_items=10)
        )

        self.assertEqual(len(records), 2)
        self.assertTrue(
            all(
                "/products-and-platforms/products/gemini/" in record["item"].url
                for record in records
            )
        )

    def test_html_candidate_collection_excludes_fragments_and_self_links(self) -> None:
        config = load_structured_config(CONFIG_DIR / "sources.yaml")
        source = next(
            source for source in config["sources"] if source["source_id"] == "blog.google_gemini"
        )

        class HtmlResponse:
            text = """
                <html><body>
                  <a href="#jump-content">Skip to main content</a>
                  <a href="/products-and-platforms/products/gemini/">Gemini App</a>
                  <a href="/products-and-platforms/products/gemini/gemini-3/">Gemini 3 launch</a>
                </body></html>
            """

            def raise_for_status(self) -> None:
                return None

        provider = RssCandidateSourceProvider(
            source,
            http_get=lambda _url: HtmlResponse(),
        )

        records = provider.collect(
            CollectRequest(source_id="blog.google_gemini", max_items=10)
        )

        self.assertEqual(len(records), 1)
        self.assertEqual(
            records[0]["item"].url,
            "https://blog.google/products-and-platforms/products/gemini/gemini-3/",
        )

    def test_html_candidate_collection_supports_source_specific_url_patterns(self) -> None:
        config = load_structured_config(CONFIG_DIR / "sources.yaml")
        source = next(
            source for source in config["sources"] if source["source_id"] == "blog.microsoft_ai"
        )

        class HtmlResponse:
            text = """
                <html><body>
                  <a href="/blog/2026/03/10/copilot-case-study/">Copilot case study</a>
                  <a href="/blog/tag/ai/">AI tag landing</a>
                  <a href="/blog/2026/03/10/security-case-study/">Security case study</a>
                </body></html>
            """

            def raise_for_status(self) -> None:
                return None

        provider = RssCandidateSourceProvider(
            source,
            http_get=lambda _url: HtmlResponse(),
        )

        records = provider.collect(
            CollectRequest(source_id="blog.microsoft_ai", max_items=10)
        )

        self.assertEqual(len(records), 2)
        self.assertTrue(
            all("/blog/2026/03/10/" in record["item"].url for record in records)
        )

    def test_html_candidate_collection_supports_excluded_url_patterns(self) -> None:
        config = load_structured_config(CONFIG_DIR / "sources.yaml")
        source = next(
            source for source in config["sources"] if source["source_id"] == "blog.aws_ml"
        )

        class HtmlResponse:
            text = """
                <html><body>
                  <a href="/blogs/machine-learning/run-nvidia-nemotron-3-super-on-amazon-bedrock/">Bedrock post</a>
                  <a href="/blogs/machine-learning/category/artificial-intelligence/amazon-machine-learning/amazon-bedrock/">Bedrock category</a>
                  <a href="/blogs/machine-learning/category/post-types/announcements/">Announcements</a>
                </body></html>
            """

            def raise_for_status(self) -> None:
                return None

        provider = RssCandidateSourceProvider(
            source,
            http_get=lambda _url: HtmlResponse(),
        )

        records = provider.collect(
            CollectRequest(source_id="blog.aws_ml", max_items=10)
        )

        self.assertEqual(len(records), 1)
        self.assertEqual(
            records[0]["item"].url,
            "https://aws.amazon.com/blogs/machine-learning/run-nvidia-nemotron-3-super-on-amazon-bedrock/",
        )

    def test_html_candidate_collection_supports_huggingface_blog_patterns(self) -> None:
        config = load_structured_config(CONFIG_DIR / "sources.yaml")
        source = next(
            source for source in config["sources"] if source["source_id"] == "blog.huggingface"
        )

        class HtmlResponse:
            text = """
                <html><body>
                  <a href="/blog">Blog home</a>
                  <a href="/blog/smolagents-v2">smolagents v2</a>
                  <a href="/blog/community">view all</a>
                  <a href="/blog/tag/agents">Agents tag</a>
                </body></html>
            """

            def raise_for_status(self) -> None:
                return None

        provider = RssCandidateSourceProvider(
            source,
            http_get=lambda _url: HtmlResponse(),
        )

        records = provider.collect(
            CollectRequest(source_id="blog.huggingface", max_items=10)
        )

        self.assertEqual(len(records), 1)
        self.assertEqual(
            records[0]["item"].url,
            "https://huggingface.co/blog/smolagents-v2",
        )

    def test_openai_candidate_provider_extracts_public_article_body(self) -> None:
        config = load_structured_config(CONFIG_DIR / "sources.yaml")
        source = next(
            source for source in config["sources"] if source["source_id"] == "blog.openai"
        )

        class FeedResponse:
            text = """
                <rss><channel>
                  <item>
                    <title>OpenAI launches new enterprise controls</title>
                    <link>https://openai.com/index/new-enterprise-controls/</link>
                    <pubDate>Fri, 21 Mar 2026 09:00:00 +0900</pubDate>
                  </item>
                </channel></rss>
            """

            def raise_for_status(self) -> None:
                return None

        class ArticleResponse:
            text = """
                <html>
                  <head>
                    <meta name="description" content="Fallback summary that should not win." />
                  </head>
                  <body>
                    <article>
                      <p>OpenAI introduced new enterprise controls that let teams manage model access,
                      project-level budgets, and audit logs from a shared administration surface.</p>
                      <p>The update also adds policy controls for tool use and improves review workflows
                      for production deployments across regulated environments.</p>
                    </article>
                  </body>
                </html>
            """

            def raise_for_status(self) -> None:
                return None

        def fake_get(url: str) -> object:
            if url.endswith("/rss.xml"):
                return FeedResponse()
            return ArticleResponse()

        provider = RssCandidateSourceProvider(source, http_get=fake_get)

        records = provider.collect(CollectRequest(source_id="blog.openai", max_items=5))

        self.assertEqual(len(records), 1)
        item = records[0]["item"]
        self.assertEqual(item.body_kind, "full_text")
        self.assertEqual(item.content_status, "available")
        self.assertEqual(item.retrieval_diagnostics["body_fetch_mode"], "public_html")
        self.assertNotIn("Fallback summary", item.raw_text)
        self.assertIn("shared administration surface", item.cleaned_text)

    def test_python_insider_candidate_provider_uses_meta_description_when_body_missing(self) -> None:
        config = load_structured_config(CONFIG_DIR / "sources.yaml")
        source = next(
            source
            for source in config["sources"]
            if source["source_id"] == "docs.python_insider"
        )

        class FeedResponse:
            text = """
                <rss><channel>
                  <item>
                    <title>Python 3.15.0 alpha 1 is available</title>
                    <link>https://blog.python.org/2026/03/python-3150-alpha-1-is-available.html</link>
                  </item>
                </channel></rss>
            """

            def raise_for_status(self) -> None:
                return None

        class ArticleResponse:
            text = """
                <html>
                  <head>
                    <meta name="description" content="Python 3.15.0 alpha 1 starts the release cycle and
                    includes parser, packaging, and runtime updates for early testing." />
                  </head>
                  <body><div class="hero">No article body in this fixture</div></body>
                </html>
            """

            def raise_for_status(self) -> None:
                return None

        def fake_get(url: str) -> object:
            if url.endswith("/rss.xml"):
                return FeedResponse()
            return ArticleResponse()

        provider = RssCandidateSourceProvider(source, http_get=fake_get)

        records = provider.collect(
            CollectRequest(source_id="docs.python_insider", max_items=5)
        )

        self.assertEqual(len(records), 1)
        item = records[0]["item"]
        self.assertEqual(item.body_kind, "description_only")
        self.assertEqual(item.content_status, "available")
        self.assertEqual(item.retrieval_diagnostics["body_fetch_mode"], "meta_description")
        self.assertIn("release cycle", item.cleaned_text)

    def test_candidate_provider_excludes_explicit_pr_titles(self) -> None:
        config = load_structured_config(CONFIG_DIR / "sources.yaml")
        source = next(
            source for source in config["sources"] if source["source_id"] == "blog.openai"
        )

        class FeedResponse:
            text = """
                <rss><channel>
                  <item>
                    <title>【PR】Sponsored AI launch roundup</title>
                    <link>https://openai.com/index/sponsored-ai-launch-roundup/</link>
                  </item>
                  <item>
                    <title>Model release notes</title>
                    <link>https://openai.com/index/model-release-notes/</link>
                  </item>
                </channel></rss>
            """

            def raise_for_status(self) -> None:
                return None

        class ArticleResponse:
            text = """
                <html><body><article>
                  <p>Model release notes describe availability, safeguards, and admin changes for users.</p>
                </article></body></html>
            """

            def raise_for_status(self) -> None:
                return None

        def fake_get(url: str) -> object:
            if url.endswith("/rss.xml"):
                return FeedResponse()
            return ArticleResponse()

        provider = RssCandidateSourceProvider(source, http_get=fake_get)

        records = provider.collect(CollectRequest(source_id="blog.openai", max_items=5))

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["item"].title, "Model release notes")


class ExportNotebookLMShapeCompatibilityTests(unittest.TestCase):
    def setUp(self) -> None:
        project_root = Path(__file__).resolve().parents[1]
        self.schema_path = project_root / "db" / "schema.sql"
        self.temp_dir = project_root / "tests" / ".tmp"
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.temp_dir / f"{uuid4().hex}.sqlite3"
        self.output_root = self.temp_dir / f"exports-{uuid4().hex}"
        self.output_root.mkdir(parents=True, exist_ok=True)
        self.db = Database(str(self.db_path), str(self.schema_path))
        self.repository = ItemRepository(self.db)
        config = load_structured_config(CONFIG_DIR / "sources.yaml")
        self.repository.sync_sources(
            [source_record_from_dict(source) for source in config["sources"]]
        )

    def tearDown(self) -> None:
        if self.output_root.exists():
            shutil.rmtree(self.output_root)

    def test_export_notebooklm_json_keeps_existing_document_shape(self) -> None:
        item = CanonicalItem(
            item_id="youtube.default:video-1",
            source_id="youtube.default",
            source_type="youtube_video",
            external_id="video-1",
            title="Video 1",
            author="Channel",
            published_at="2026-03-21T00:00:00Z",
            url="https://example.com/video-1",
            raw_text="Raw",
            cleaned_text="Cleaned text " * 20,
            body_kind="full_text",
            content_status="available",
            evidence_strength="medium",
            quality_tier="high",
            reader_eligibility="eligible",
            notebooklm_eligibility="eligible",
        )
        self.repository.upsert_item(item)
        self.repository.upsert_item_summary(item.item_id, "Short summary", "- Point A")
        chunk = self.repository.replace_chunks(item.item_id, ["Chunk body"])[0]
        self.repository.upsert_chunk_summary(
            chunk["chunk_id"],
            {
                "summary": "Chunk summary",
                "key_points": ["Point A"],
                "entities": ["Entity A"],
                "category": ["Category A"],
                "signal_score": 0.8,
            },
        )

        export_path = export_items(
            self.db,
            "notebooklm-json",
            self.output_root,
            source_ids={"youtube.default"},
        )
        payload = json.loads(export_path.read_text(encoding="utf-8"))

        self.assertEqual(payload["schema_version"], "notebooklm-pack.v1")
        self.assertEqual(payload["stats"]["video_count"], 1)
        self.assertIn("documents", payload)
        self.assertEqual(payload["documents"][0]["video"]["video_id"], "video-1")
        self.assertEqual(payload["documents"][0]["summary"]["short_summary"], "Short summary")
        self.assertEqual(payload["documents"][0]["evidence"]["chunk_count"], 1)

    def test_cleanup_explicit_noise_items_removes_existing_pr_rows(self) -> None:
        noise_item = CanonicalItem(
            item_id="nikkei.xtech.candidate:https://xtech.nikkei.com/noise",
            source_id="nikkei.xtech.candidate",
            source_type="article",
            external_id="xtech.nikkei.com/noise",
            title="【PR】Noise item",
            author="Nikkei xTECH Candidate",
            published_at="2026-03-21T00:00:00Z",
            url="https://xtech.nikkei.com/noise",
            raw_text="",
            cleaned_text="",
            body_kind="metadata_only",
            content_status="unavailable",
            evidence_strength="none",
            quality_tier="reject",
            reader_eligibility="eligible_with_warning",
            notebooklm_eligibility="ineligible",
        )
        keep_item = CanonicalItem(
            item_id="blog.openai:https://openai.com/keep",
            source_id="blog.openai",
            source_type="article",
            external_id="openai.com/keep",
            title="Keep item",
            author="OpenAI Blog",
            published_at="2026-03-21T00:00:00Z",
            url="https://openai.com/keep",
            raw_text="Useful article body " * 10,
            cleaned_text="Useful article body " * 10,
            body_kind="full_text",
            content_status="available",
            evidence_strength="medium",
            quality_tier="high",
            reader_eligibility="eligible",
            notebooklm_eligibility="eligible",
        )
        self.repository.upsert_item(noise_item)
        self.repository.upsert_item(keep_item)

        dry_run_report = cleanup_explicit_noise_items(
            self.db,
            source_ids={"blog.openai", "nikkei.xtech.candidate"},
            dry_run=True,
        )
        self.assertEqual(dry_run_report["matched_count"], 1)
        self.assertEqual(dry_run_report["deleted_count"], 0)

        cleanup_report = cleanup_explicit_noise_items(
            self.db,
            source_ids={"blog.openai", "nikkei.xtech.candidate"},
            dry_run=False,
        )
        self.assertEqual(cleanup_report["matched_count"], 1)
        self.assertEqual(cleanup_report["deleted_count"], 1)
        self.assertIsNone(self.repository.get_item(noise_item.item_id))
        self.assertIsNotNone(self.repository.get_item(keep_item.item_id))


if __name__ == "__main__":
    unittest.main()
