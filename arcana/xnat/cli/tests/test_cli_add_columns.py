import os.path
from unittest.mock import patch
from arcana.core.cli.dataset import add_source, add_sink
from arcana.core.utils.testing import show_cli_trace


def test_add_source_xnat(mutable_dataset, cli_runner, work_dir):

    test_home_dir = work_dir / "test-arcana-home"

    with patch.dict(os.environ, {"ARCANA_HOME": str(test_home_dir)}):
        store_nickname = mutable_dataset.id + "_store"
        dataset_name = "testing123"
        mutable_dataset.store.save(store_nickname)
        dataset_locator = (
            store_nickname + "//" + mutable_dataset.id + "@" + dataset_name
        )
        mutable_dataset.save(dataset_name)

        result = cli_runner(
            add_source,
            [
                dataset_locator,
                "a_source",
                "fileformats.text:Plain",
                "--path",
                "file1",
                "--row-frequency",
                "session",
                "--quality",
                "questionable",
                "--order",
                "1",
                "--no-regex",
            ],
        )
        assert result.exit_code == 0, show_cli_trace(result)


def test_add_sink_xnat(mutable_dataset, work_dir, cli_runner):

    test_home_dir = work_dir / "test-arcana-home"

    with patch.dict(os.environ, {"ARCANA_HOME": str(test_home_dir)}):
        store_nickname = mutable_dataset.id + "_store"
        dataset_name = "testing123"
        mutable_dataset.store.save(store_nickname)
        dataset_locator = (
            store_nickname + "//" + mutable_dataset.id + "@" + dataset_name
        )
        mutable_dataset.save(dataset_name)

        result = cli_runner(
            add_sink,
            [
                dataset_locator,
                "a_sink",
                "fileformats.text:Plain",
                "--path",
                "deriv",
                "--row-frequency",
                "session",
                "--salience",
                "qa",
            ],
        )
        assert result.exit_code == 0, show_cli_trace(result)
