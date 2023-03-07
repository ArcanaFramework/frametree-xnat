from arcana.core.cli.dataset import add_source, add_sink
from arcana.core.utils.misc import show_cli_trace


def test_add_source_xnat(mutable_dataset, cli_runner, arcana_home, work_dir):

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


def test_add_sink_xnat(mutable_dataset, work_dir, arcana_home, cli_runner):

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
