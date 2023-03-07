from arcana.core.cli.store import add, ls, remove, rename
from arcana.core.utils.misc import show_cli_trace
from arcana.core.data.store import DataStore


def test_store_cli(xnat_repository, cli_runner, arcana_home, work_dir):
    # Add new XNAT configuration
    result = cli_runner(
        add,
        [
            "test-xnat",
            "xnat:Xnat",
            "--server",
            xnat_repository.server,
            "--user",
            xnat_repository.user,
            "--password",
            xnat_repository.password,
        ],
    )
    assert result.exit_code == 0, show_cli_trace(result)
    # List all saved and built-in stores
    result = cli_runner(ls, [])
    assert result.exit_code == 0, show_cli_trace(result)
    assert "test-xnat - arcana.xnat.data.api:Xnat" in result.output
    assert "    server: " + xnat_repository.server in result.output


def test_store_cli_remove(xnat_repository, cli_runner, arcana_home, work_dir):
    new_store_name = "test-xnat"
    # Create a new home directory so it doesn't conflict with user settings
    # Add new XNAT configuration
    cli_runner(
        add,
        [
            new_store_name,
            "xnat:Xnat",
            "--server",
            xnat_repository.server,
            "--user",
            xnat_repository.user,
            "--password",
            xnat_repository.password,
        ],
    )
    # Check store is saved
    result = cli_runner(ls, [])
    assert new_store_name in result.output

    cli_runner(remove, new_store_name)
    # Check store is gone
    result = cli_runner(ls, [])
    assert new_store_name not in result.output


def test_store_cli_rename(xnat_repository, cli_runner, arcana_home, work_dir):
    old_store_name = "i123"
    new_store_name = "y456"
    # Add new XNAT configuration
    cli_runner(
        add,
        [
            old_store_name,
            "xnat:Xnat",
            "--server",
            xnat_repository.server,
            "--user",
            xnat_repository.user,
            "--password",
            xnat_repository.password,
        ],
    )
    # Check store is saved
    result = cli_runner(ls, [])
    assert "i123 - arcana.xnat.data.api:Xnat" in result.output

    cli_runner(rename, [old_store_name, new_store_name])
    # Check store is renamed
    result = cli_runner(ls, [])
    assert "i123 - arcana.xnat.data.api:Xnat" not in result.output
    assert "y456 - arcana.xnat.data.api:Xnat" in result.output


def test_store_cli_encrypt_credentials(xnat_repository, cli_runner, arcana_home, work_dir):
    # Add new XNAT configuration
    result = cli_runner(
        add,
        [
            "test-xnat",
            "xnat:Xnat",
            "--server",
            xnat_repository.server,
            "--user",
            xnat_repository.user,
            "--password",
            xnat_repository.password,
        ],
    )
    assert result.exit_code == 0, show_cli_trace(result)
    # Check credentials have been encrypted
    loaded_xnat_repository = DataStore.load("test-xnat")
    assert loaded_xnat_repository.password != ""
    assert loaded_xnat_repository.password is not xnat_repository.password
    assert loaded_xnat_repository.user != ""
    assert loaded_xnat_repository.user is not xnat_repository.user
