import unittest

import httpx

from message_handler import handle_incoming_message


class IncomingMessageHandlerTests(unittest.IsolatedAsyncioTestCase):
    async def test_handler_uses_agent_response_and_sends_reply(self) -> None:
        class DummyAgentClient:
            def __init__(self) -> None:
                self.prompts: list[str] = []

            async def generate_agent(self, prompt: str) -> str:
                self.prompts.append(prompt)
                return "hi there"

        sent_messages: list[tuple[str, str]] = []

        async def fake_send_reply(to: str, body: str) -> None:
            sent_messages.append((to, body))

        agent_client = DummyAgentClient()
        await handle_incoming_message(
            phone="+593987654321",
            body="hello",
            agent_client=agent_client,
            send_reply=fake_send_reply,
        )

        self.assertEqual(sent_messages, [("+593987654321", "hi there")])
        self.assertEqual(len(agent_client.prompts), 1)
        self.assertIn("hello", agent_client.prompts[0])

    async def test_handler_sends_conversation_history_to_agent(self) -> None:
        class DummyAgentClient:
            def __init__(self) -> None:
                self.prompts: list[str] = []

            async def generate_agent(self, prompt: str) -> str:
                self.prompts.append(prompt)
                return "ok"

        agent_client = DummyAgentClient()
        await handle_incoming_message(
            phone="+593987654321",
            body="how much?",
            conversation_history="them:hi\nme:hello\nthem:how much?",
            agent_client=agent_client,
        )

        self.assertEqual(len(agent_client.prompts), 1)
        self.assertIn("Conversation messages:\nthem:hi\nme:hello\nthem:how much?", agent_client.prompts[0])
        self.assertIn("Latest user message: how much?", agent_client.prompts[0])

    async def test_handler_persists_outbound_reply_when_persist_callback_is_provided(self) -> None:
        class DummyAgentClient:
            async def generate_agent(self, prompt: str) -> str:
                return "thanks"

        persisted_messages: list[tuple[str, str]] = []

        async def fake_persist(phone: str, body: str) -> None:
            persisted_messages.append((phone, body))

        await handle_incoming_message(
            phone="+593987654321",
            body="hello",
            agent_client=DummyAgentClient(),
            send_reply=None,
            persist_reply=fake_persist,
        )

        self.assertEqual(persisted_messages, [("+593987654321", "thanks")])

    async def test_handler_sends_fallback_reply_when_agent_times_out(self) -> None:
        class TimeoutAgentClient:
            async def generate_agent(self, prompt: str) -> str:
                raise httpx.ReadTimeout("timed out")

        sent_messages: list[tuple[str, str]] = []

        async def fake_send_reply(to: str, body: str) -> None:
            sent_messages.append((to, body))

        with self.assertLogs("message_handler", level="ERROR") as logs:
            await handle_incoming_message(
                phone="+593987654321",
                body="hello",
                agent_client=TimeoutAgentClient(),
                send_reply=fake_send_reply,
            )

        self.assertEqual(
            sent_messages,
            [("+593987654321", "Thanks for your message. I'll get back to you shortly.")],
        )
        self.assertIn("Failed to generate LLM reply", logs.output[0])


if __name__ == "__main__":
    unittest.main()
