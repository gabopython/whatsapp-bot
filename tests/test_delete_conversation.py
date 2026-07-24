import unittest

from delete_conversation import DEFAULT_PHONES, _normalize_phone_list


class DeleteConversationTests(unittest.TestCase):
    def test_normalize_phone_list_supports_repeated_and_comma_values(self) -> None:
        phones = _normalize_phone_list(["593962052098,593998435259", "593996818841", "  "])

        self.assertEqual(phones, ["593962052098", "593998435259", "593996818841"])

    def test_default_phone_list_contains_the_requested_numbers(self) -> None:
        self.assertEqual(
            DEFAULT_PHONES,
            ("593962052098", "593998435259", "593996818841"),
        )


if __name__ == "__main__":
    unittest.main()
