import unittest

from app import normalize_message_body


class MediaMessageParsingTests(unittest.TestCase):
    def test_image_message_with_caption_is_normalized(self) -> None:
        message = {
            "type": "image",
            "image": {"id": "media-id-123"},
            "caption": {"body": "hello from media"},
        }

        self.assertEqual(
            normalize_message_body(message),
            "media:image:media-id-123|hello from media",
        )

    def test_video_message_without_caption_is_normalized(self) -> None:
        message = {"type": "video", "video": {"url": "https://example.com/video.mp4"}}

        self.assertEqual(
            normalize_message_body(message),
            "media:video:https://example.com/video.mp4",
        )


if __name__ == "__main__":
    unittest.main()
