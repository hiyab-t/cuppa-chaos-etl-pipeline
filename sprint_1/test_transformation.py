import csv
import pytest
from unittest.mock import patch 
import uuid
from transformation import remove_sensitive_info, drop_duplicate_product_values, generate_uuid

@pytest.fixture
def raw_rows():
    # Load raw rows from the actual Chesterfield CSV file.
    with open("data/chesterfield.csv", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        return list(reader)

# Happy Test

def test_remove_pii_from_files_removes_extra_columns(raw_rows):
    # Verify that the remove_pii_from_files function:
    # - Returns a cleaned dataset with the expected header.
    # - Adds a new header row to the cleaned output.
    # - Removes personally identifiable information (PII) columns.
    # - Retains all required data columns in the cleaned rows.
    
    cleaned_rows = remove_sensitive_info(raw_rows)

    expected_header = ["datetime", "branch", "product", "total_price", "payment_method"]
    # Confirm the header matches exactly the expected column names
    assert cleaned_rows[0] == expected_header

    # The cleaned data should have one additional row due to the new header
    assert len(cleaned_rows) == len(raw_rows)

    # Spot check the first data row to ensure it contains the expected columns
    first_data_row = cleaned_rows[1]
    assert len(first_data_row) == len(expected_header)
    assert first_data_row[0], "Expected a datetime value"
    assert first_data_row[1], "Expected a branch name"
    assert first_data_row[2], "Expected a product string"
    assert first_data_row[3], "Expected a total price"
    assert first_data_row[4], "Expected a payment method"

# Unhappy Test

def test_remove_pii_from_files_handles_empty_and_malformed_input():
    # Unhappy path tests to confirm the function behaves safely with:
    # - An empty input list
    # - Malformed rows with missing columns
    
    # Empty input should return just the header row
    empty_input = []
    cleaned_empty = remove_sensitive_info(empty_input)
    assert cleaned_empty == [["datetime", "branch", "product", "total_price", "payment_method"]]

    # Malformed row with missing columns should be skipped or handled gracefully
    malformed_input = [
        ["datetime", "branch", "customer", "product", "total_price", "payment_method"],
        ["2023-01-01 10:00", "Branch1"],  # Malformed row, too short
        ["2023-01-01 11:00", "Branch2", "Customer2", "Product2", "10.00", "CARD"]
    ]

    cleaned_malformed = remove_sensitive_info(malformed_input)

    # Expect header plus only the well-formed row to be present
    assert cleaned_malformed[0] == ["datetime", "branch", "product", "total_price", "payment_method"]
    assert len(cleaned_malformed) == 2  # header + one valid row
    assert cleaned_malformed[1] == ["2023-01-01 11:00", "Branch2", "Product2", "10.00", "CARD"]

# Edge Case Test

def test_remove_pii_from_files_with_minimal_valid_input():
    # Edge case tests for minimal but valid inputs:
    # - Only header row
    # - Single data row
    
    # Input with only header row should return just the new header
    minimal_input = [["datetime", "branch", "customer", "product", "total_price", "payment_method"]]
    cleaned_minimal = remove_sensitive_info(minimal_input)
    assert cleaned_minimal == [["datetime", "branch", "product", "total_price", "payment_method"]]

    # Input with one data row
    single_row_input = [
        ["datetime", "branch", "customer", "product", "total_price", "payment_method"],
        ["2023-01-01 12:00", "BranchX", "CustomerX", "ProductX", "5.00", "CASH"]
    ]
    cleaned_single = remove_sensitive_info(single_row_input)
    assert len(cleaned_single) == 2
    assert cleaned_single[1] == ["2023-01-01 12:00", "BranchX", "ProductX", "5.00", "CASH"]

#happy case

class TestGenerateUUID(unittest.TestCase):

    # arrange by setting a decorator by mocking uuid  

    @patch('transformation.uuid.uuid4', return_value=uuid.UUID('11111111-1111-1111-1111-111111111111'))

    def test_generate_uuid_mocked_will_return_the_predetermined_uuid(self, mock_uuid):

        # act

        result = generate_uuid()

        # assert

        self.assertEqual(result, '11111111-1111-1111-1111-111111111111')

#happy case will drop rows that are duplicate

def test_will_drop_duplicate_keys():

    # arrange

    dummy_data = [{'size': 'Regular', 
                        'name': 'Flavoured Iced Latte',
                        'flavour': 'Hazelnut', 
                        'price': 2.75}, 

                        {'size': 'Large', 
                        'name': 'Latte', 
                        'flavour': None, 
                        'price': 2.45},

                        {'size': 'Regular', 
                        'name': 'Flavoured Iced Latte',
                        'flavour': 'Hazelnut', 
                        'price': 2.75}]

    # act

    expected = [{'size': 'Regular', 
                        'name': 'Flavoured Iced Latte',
                        'flavour': 'Hazelnut', 
                        'price': 2.75}, 

                        {'size': 'Large', 
                        'name': 'Latte', 
                        'flavour': None, 
                        'price': 2.45}]

    # assert

    result = drop_duplicate_product_values(dummy_data)

    assert result == expected, f'Expected {expected} but was {result}.'