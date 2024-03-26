import pytest

from src.utils import combine_parquet


class TestCombineParquet:
    """Object to collect the relevant tests for the function `combine_parquet` in
    the utils module."""

    # Add the test for the empty case and the right exception
    def test_combine_parquet_empty(self):
        """Test for the empty case and the right exception"""
        with pytest.raises(TypeError):
            combine_parquet()

    # Add the test for the empty input_files argument and the right exception
    def test_combine_parquet_empty_input_files_arg(self):
        """Test for the empty input_files argument and the right exception"""
        with pytest.raises(TypeError):
            combine_parquet(input_files=[], output_file="output_file")

    # Add the test for the empty output_file argument and the right exception
    def test_combine_parquet_empty_output_file_arg(self):
        """Test for the empty output_file argument and the right exception"""
        with pytest.raises(ValueError):
            combine_parquet(input_files=["file1", "file2"], output_file="")

    # Add the test for non-existing input files and the right exception
    def test_combine_parquet_non_existing_input_files(self):
        """Test for non-existing input files and the right exception"""
        with pytest.raises(FileNotFoundError):
            combine_parquet(input_files=["file1", "file2"], output_file="output_file")
