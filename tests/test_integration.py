from unittest.mock import patch

from open_auto_loader.main import OpenAutoLoader


@patch("open_auto_loader.main.FileScanner")
@patch("open_auto_loader.main.PolarsEngine")
@patch("open_auto_loader.main.CheckPointManager")
@patch("open_auto_loader.main.SchemaManager")
def test_run_logic_flow(mock_schema, mock_cp, mock_engine, mock_scanner):
    # Setup mocks
    mock_scanner_inst = mock_scanner.return_value
    mock_scanner_inst.get_eligible_files.return_value = ["file1.csv"]

    mock_schema_inst = mock_schema.return_value
    mock_schema_inst.schema_exists.return_value = True
    mock_schema_inst.load_schema.return_value = {"id": "Int64"}

    loader = OpenAutoLoader(
        source="s3://bucket/",
        target="s3://target/",
        check_point="cp.db",
        schema_path="schemas/",
    )

    # Execute
    loader.run(batch_id="batch_001")

    # Assertions
    # 1. Did it scan for files?
    mock_scanner_inst.get_eligible_files.assert_called_once()

    # 2. Did it process the file found?
    mock_engine.return_value.process_single_file.assert_called_once()

    # 3. Did it mark the file as processed in the checkpoint?
    mock_cp.return_value.mark_processed.assert_called_with(
        "file1.csv", batch_id="batch_001"
    )
