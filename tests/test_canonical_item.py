import unittest

from normalization.canonicalize import canonicalize_youtube_video


class CanonicalizeTests(unittest.TestCase):
    def test_canonicalize_marks_description_only_without_overstating_evidence(self) -> None:
        item = canonicalize_youtube_video(
            source_id="youtube.default",
            video={
                "video_id": "abc123",
                "title": "Example",
                "channel": "Channel A",
                "published_at": "2026-03-21T00:00:00Z",
                "url": "https://example.com/watch?v=abc123",
            },
            transcript_payload={
                "text": "Read more in the link below. Revenue grew 20 percent.",
                "source": "description",
                "diagnostics": {"description": "available"},
            },
        )

        self.assertEqual(item.source_type, "youtube_video")
        self.assertEqual(item.external_id, "abc123")
        self.assertEqual(item.body_kind, "description_only")
        self.assertEqual(item.content_status, "available")
        self.assertEqual(item.evidence_strength, "weak")


if __name__ == "__main__":
    unittest.main()
