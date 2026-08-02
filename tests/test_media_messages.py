import importlib
import os
import unittest

os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"

from sqlalchemy import select

import app as app_module

app_module = importlib.reload(app_module)
Conversation = app_module.Conversation
SessionLocal = app_module.SessionLocal
init_db = app_module.init_db
normalize_message_body = app_module.normalize_message_body
save_message = app_module.save_message


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


class ConversationPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await init_db()

    async def test_save_message_increments_sent_count_and_stores_tags(self) -> None:
        async with SessionLocal() as session:
            phone = await save_message(
                session,
                direction="me",
                phone="+123456789",
                body="hello",
                tags="vip",
            )

        self.assertEqual(phone, "+123456789")

        async with SessionLocal() as session:
            conversation = await session.scalar(
                select(Conversation).where(Conversation.phone == "+123456789")
            )

        self.assertIsNotNone(conversation)
        self.assertEqual(conversation.sent_messages_count, 1)
        self.assertEqual(conversation.tags, "vip")


if __name__ == "__main__":
    unittest.main()
