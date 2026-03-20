import unittest

from processing.cleaner import clean_text


class CleanerTests(unittest.TestCase):
    def test_clean_text_removes_common_subtitle_noise(self) -> None:
        raw_text = """
        00:00 [Music]
        HOST: Welcome back to the show.
        00:05 - 00:09
        [Applause] The market is moving fast.
        Thanks for watching
        """

        self.assertEqual(
            clean_text(raw_text),
            "Welcome back to the show. The market is moving fast.",
        )

    def test_clean_text_keeps_content_but_drops_description_noise(self) -> None:
        raw_text = """
        #AINews #Tech
        @channelname
        Link in the description for the source report.
        CEO says revenue grew 30 percent year over year.
        """

        self.assertEqual(
            clean_text(raw_text),
            "CEO says revenue grew 30 percent year over year.",
        )

    def test_clean_text_deduplicates_repeated_lines_after_normalization(self) -> None:
        raw_text = """
        [Music]
        Speaker: Breaking news on chip exports.
        00:12 Speaker: Breaking news on chip exports.
        """

        self.assertEqual(clean_text(raw_text), "Breaking news on chip exports.")

    def test_clean_text_removes_webvtt_headers_and_cue_timestamps(self) -> None:
        raw_text = """
        WEBVTT
        Kind: captions
        Language: en

        00:00:01.000 --> 00:00:03.000 align:start position:0%
        AI demand is reshaping data center spending.
        """

        self.assertEqual(
            clean_text(raw_text),
            "AI demand is reshaping data center spending.",
        )

    def test_clean_text_removes_quote_speakers_and_musical_note_noise(self) -> None:
        raw_text = """
        \u266a Intro music \u266a
        >> HOST: Nvidia guided above expectations.
        - Guest: Supply remains constrained.
        """

        self.assertEqual(
            clean_text(raw_text),
            "Nvidia guided above expectations. Supply remains constrained.",
        )

    def test_clean_text_drops_social_cta_prefix_but_keeps_following_content(self) -> None:
        raw_text = """
        Smash that like button before we begin.
        Turn on notifications for more updates.
        Analysts expect inference demand to stay elevated through 2026.
        """

        self.assertEqual(
            clean_text(raw_text),
            "Analysts expect inference demand to stay elevated through 2026.",
        )

    def test_clean_text_removes_caption_credit_lines(self) -> None:
        raw_text = """
        Subtitles by Example Captioning
        Captions by Demo Team
        OpenAI announced a new reasoning workflow for agents.
        """

        self.assertEqual(
            clean_text(raw_text),
            "OpenAI announced a new reasoning workflow for agents.",
        )

    def test_clean_text_removes_descriptive_non_speech_and_censor_markers(self) -> None:
        raw_text = """
        [Upbeat music]
        [Audience applause]
        The company expects margin expansion next quarter.
        [__]
        """

        self.assertEqual(
            clean_text(raw_text),
            "The company expects margin expansion next quarter.",
        )

    def test_clean_text_removes_role_based_speaker_labels(self) -> None:
        raw_text = """
        Speaker 1: Model costs are falling.
        Narrator: Competition is increasing across providers.
        """

        self.assertEqual(
            clean_text(raw_text),
            "Model costs are falling. Competition is increasing across providers.",
        )

    def test_clean_text_removes_vtt_note_region_style_metadata(self) -> None:
        raw_text = """
        WEBVTT
        NOTE This is an auto-generated caption file.
        Region: id=scroll
        Style: .captionStyle { color: white; }
        Host: Revenue accelerated in the enterprise segment.
        """

        self.assertEqual(
            clean_text(raw_text),
            "Revenue accelerated in the enterprise segment.",
        )

    def test_clean_text_removes_srt_cue_ids_and_voiceover_variants(self) -> None:
        raw_text = """
        17
        00:00:07,120 --> 00:00:09,900
        Speaker 2 (voice-over): The launch window opens next month.
        18
        00:00:10,000 --> 00:00:12,400
        Reporter: Demand remains strong in Japan.
        """

        self.assertEqual(
            clean_text(raw_text),
            "The launch window opens next month. Demand remains strong in Japan.",
        )


if __name__ == "__main__":
    unittest.main()
