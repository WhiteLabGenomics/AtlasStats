import os
import pyarrow.parquet as pq


def combine_parquet(input_files: list, output_file: str) -> None:
    """Function that multiple parquet files into a single file.

    Parameters
    ----------
    input_files: list
        A list of file paths to combine.
    output_file: str
        A string indicating the output file path.

    Returns
    -------
    None
    """
    # Check if the input files are a list
    if not isinstance(input_files, list):
        raise TypeError("Input files should be a list of file paths.")
    # Check if the output file is not empty
    if len(output_file) < 1:
        raise ValueError("Output file path should not be empty.")
    # Check input are existing files
    for file in input_files:
        if not os.path.exists(file):
            raise FileNotFoundError(f"File {file} does not exist.")
    # Check if the output file already exists
    if os.path.exists(output_file):
        raise FileExistsError(f"Output file {output_file} already exists.")

    # Combine the parquet files without reading them

    # Initialize writer
    writer = None

    # Iterate through each input file
    for file in input_files:
        # Read the parquet file
        reader = pq.ParquetFile(source=file)
        # Get the ParquetSchema
        file_schema = reader.schema
        # Convert to ArrowSchema
        file_schema = file_schema.to_arrow_schema()

        # Initialize writer on the first iteration only
        if not writer:
            writer = pq.ParquetWriter(where=output_file, schema=file_schema)

        # Read data in batches using iter_batches()
        for batch in reader.iter_batches():
            # Write RecordBatch to the Parquet file
            writer.write_batch(batch)

    # Close the writer
    writer.close()

    # Return None
    return None
