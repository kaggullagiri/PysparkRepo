# Import necessary modules for testing
import unittest
from PysparkRepo/src/Assignment2/util import mask_card_number


class TestMaskCardNumber(unittest.TestCase):

    def test_mask_card_number(self):
        # Test cases
        test_cases = [
            ("1234567891234567", "************4567"),
            ("5678912345671234", "************1234"),
            ("9123456712345678", "************5678"),
            ("1234567812341122", "************1122"),
            ("1234567812341342", "************1342")
        ]

        # Check if the mask_card_number function produces the expected output
        for card_number, expected_result in test_cases:
            result = mask_card_number(card_number)
            self.assertEqual(result, expected_result)


