import unittest

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


if __name__ == "__main__":
    unittest.main()
