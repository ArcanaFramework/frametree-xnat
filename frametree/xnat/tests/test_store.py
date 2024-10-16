import os
import sys
import os.path
import operator as op
import shutil
import logging
from pathlib import Path
from functools import reduce
import itertools
import pytest
from pydra.utils.hash import hash_object
from fileformats.generic import File
from fileformats.field import Text as TextField
from frametree.common import Clinical
from frametree.core.frameset import FrameSet
from frametree.xnat import XnatViaCS
from frametree.core.serialize import asdict

if sys.platform == "win32":

    def get_perms(f):
        return "WINDOWS-UNKNOWN"

else:
    from pwd import getpwuid
    from grp import getgrgid

    def get_perms(f):
        st = os.stat(f)
        return (
            getpwuid(st.st_uid).pw_name,
            getgrgid(st.st_gid).gr_name,
            oct(st.st_mode),
        )


# logger = logging.getLogger('frametree')
# logger.setLevel(logging.INFO)


def test_populate_tree(static_dataset: FrameSet):
    blueprint = static_dataset.__annotations__["blueprint"]
    for freq in Clinical:
        # For all non-zero bases in the row_frequency, multiply the dim lengths
        # together to get the combined number of rows expected for that
        # row_frequency
        num_rows = reduce(
            op.mul,
            (ln for ln, b in zip(blueprint.dim_lengths, freq) if b),
            1,
        )
        assert len(static_dataset.rows(freq)) == num_rows, (
            f"{freq} doesn't match {len(static_dataset.rows(freq))}" f" vs {num_rows}"
        )


def test_populate_row(static_dataset: FrameSet):
    blueprint = static_dataset.__annotations__["blueprint"]
    for row in static_dataset.rows("session"):
        expected_entries = sorted(
            itertools.chain(
                *(
                    [f"{scan_bp.name}/{res_bp.path}" for res_bp in scan_bp.resources]
                    for scan_bp in blueprint.scans
                )
            )
        )
        assert sorted(e.path for e in row.entries) == expected_entries


def test_get(static_dataset: FrameSet, caplog):
    blueprint = static_dataset.__annotations__["blueprint"]
    expected_files = {}
    for scan_bp in blueprint.scans:
        for resource_bp in scan_bp.resources:
            if resource_bp.datatype is not None:
                source_name = scan_bp.name + resource_bp.path
                static_dataset.add_source(
                    source_name, path=scan_bp.name, datatype=resource_bp.datatype
                )
                expected_files[source_name] = set(resource_bp.filenames)
    with caplog.at_level(logging.INFO, logger="frametree"):
        for row in static_dataset.rows(Clinical.session):
            for source_name, files in expected_files.items():
                try:
                    item = row[source_name]
                except PermissionError:
                    archive_dir = str(
                        Path.home()
                        / ".xnat4tests"
                        / "xnat_root"
                        / "archive"
                        / static_dataset.id
                    )
                    archive_perms = get_perms(archive_dir)
                    current_user = os.getlogin()
                    msg = (
                        f"Error accessing {item} as '{current_user}' when "
                        f"'{archive_dir}' has {archive_perms} permissions"
                    )
                    raise PermissionError(msg)
                item_files = sorted(
                    p.name for p in item.fspaths if not p.name.endswith("catalog.xml")
                )
                assert item_files == sorted(Path(f).name for f in files)
    method_str = "direct" if type(static_dataset.store) is XnatViaCS else "api"
    assert f"{method_str} access" in caplog.text.lower()


def test_post(dataset: FrameSet, source_data: Path, caplog):
    blueprint = dataset.__annotations__["blueprint"]
    all_checksums = {}
    is_direct = isinstance(dataset.store, XnatViaCS) and dataset.store.internal_upload
    for deriv_bp in blueprint.derivatives:
        dataset.add_sink(
            name=deriv_bp.path,
            datatype=deriv_bp.datatype,
            row_frequency=deriv_bp.row_frequency,
        )
        # Create test files, calculate checksums and recorded expected paths
        # for inserted files
        item = deriv_bp.make_item(
            source_data=source_data,
            source_fallback=True,
        )
        # if len(fspaths) == 1 and fspaths[0].is_dir():
        #     relative_to = fspaths[0]
        # else:
        #     relative_to = deriv_tmp_dir
        all_checksums[deriv_bp.path] = item.hash_files()
        # Insert into first row of that row_frequency in dataset
        row = next(iter(dataset.rows(deriv_bp.row_frequency)))
        with caplog.at_level(logging.INFO, logger="frametree"):
            row[deriv_bp.path] = item
        method_str = "direct" if is_direct else "api"
        assert f"{method_str} access" in caplog.text.lower()

    access_method = "cs" if is_direct else "api"

    def check_inserted():
        for deriv_bp in blueprint.derivatives:
            row = next(iter(dataset.rows(deriv_bp.row_frequency)))
            cell = row.cell(deriv_bp.path, allow_empty=False)
            item = cell.item
            assert isinstance(item, deriv_bp.datatype)
            assert item.hash_files() == all_checksums[deriv_bp.path]

    if access_method == "api":
        check_inserted()  # Check cache
        # Check downloaded by deleting the cache dir
        shutil.rmtree(dataset.store.cache_dir / "projects" / dataset.id)
        check_inserted()


def test_frameset_roundtrip(simple_dataset: FrameSet):
    definition = asdict(simple_dataset, omit=["store", "name"])
    definition["store-version"] = "1.0.0"

    data_store = simple_dataset.store

    with data_store.connection:
        data_store.save_frameset_definition(
            dataset_id=simple_dataset.id, definition=definition, name="test_dataset"
        )
        reloaded_definition = data_store.load_frameset_definition(
            dataset_id=simple_dataset.id, name="test_dataset"
        )
    assert definition == reloaded_definition


# We use __file__ here as we just need any old file and can guarantee it exists
@pytest.mark.parametrize("datatype,value", [(File, __file__), (TextField, "value")])
def test_provenance_roundtrip(datatype: type, value: str, simple_dataset: FrameSet):
    provenance = {"a": 1, "b": [1, 2, 3], "c": {"x": True, "y": "foo", "z": "bar"}}
    data_store = simple_dataset.store

    with data_store.connection:
        entry = data_store.create_entry("provtest@", datatype, simple_dataset.root)
        data_store.put(datatype(value), entry)  # Create the entry first
        data_store.put_provenance(provenance, entry)  # Save the provenance
        reloaded_provenance = data_store.get_provenance(entry)  # reload the provenance
        assert provenance == reloaded_provenance


def test_dataset_bytes_hash(static_dataset):

    hsh = hash_object(static_dataset)
    # Check hashing is stable
    assert hash_object(static_dataset) == hsh
