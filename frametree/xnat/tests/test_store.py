import itertools
import logging
import operator as op
import os
import random
import shutil
import sys
from copy import deepcopy
from datetime import datetime
from functools import reduce
from pathlib import Path
from typing import Any

import pytest
from fileformats.field import Text as TextField
from fileformats.generic import File
from fileformats.text import Plain as PlainText
from fileformats.text import TextFile
from frametree.axes.medimage import MedImage
from frametree.core.frameset import FrameSet
from frametree.core.serialize import asdict
from frametree.testing.blueprint import FileSetEntryBlueprint as FileBP
from pydra.utils.hash import hash_object

from conftest import access_dataset
from frametree.xnat import Xnat, XnatViaCS
from frametree.xnat.testing import ScanBlueprint as ScanBP
from frametree.xnat.testing import TestXnatDatasetBlueprint

if sys.platform == "win32":

    def get_perms(f: str) -> str:
        return "WINDOWS-UNKNOWN"

else:
    from grp import getgrgid
    from pwd import getpwuid

    def get_perms(f: str) -> tuple[str, str, str]:
        st = os.stat(f)
        return (
            getpwuid(st.st_uid).pw_name,
            getgrgid(st.st_gid).gr_name,
            oct(st.st_mode),
        )


# # logger = logging.getLogger('frametree')
# # logger.setLevel(logging.INFO)


def test_populate_tree(static_dataset: FrameSet) -> None:
    blueprint = static_dataset.__annotations__["blueprint"]
    for freq in MedImage:
        # For all non-zero bases in the row_frequency, multiply the dim lengths
        # together to get the combined number of rows expected for that
        # row_frequency
        num_rows = reduce(
            op.mul,
            (ln for ln, b in zip(blueprint.dim_lengths, freq) if b),
            1,
        )
        assert len(static_dataset.rows(str(freq))) == num_rows, (
            f"{freq} doesn't match {len(static_dataset.rows(str(freq)))}"
            f" vs {num_rows}"
        )


def test_populate_tree_restrict_sessions(static_dataset: FrameSet) -> None:
    dataset = deepcopy(static_dataset)
    blueprint: TestXnatDatasetBlueprint = dataset.__annotations__["blueprint"]
    session_ids = [i[1] for i in blueprint.all_ids]
    dataset.include = {"session": session_ids[1::2]}  # Include every second session
    num_sessions = reduce(op.mul, blueprint.dim_lengths, 1) // 2
    with dataset.tree:
        assert len(dataset.tree.root.children[MedImage.session]) == num_sessions


def test_populate_tree_restrict_subjects(static_dataset: FrameSet) -> None:
    dataset = deepcopy(static_dataset)
    blueprint: TestXnatDatasetBlueprint = dataset.__annotations__["blueprint"]
    subject_ids = sorted(set(i[0] for i in blueprint.all_ids))
    restricted_subject_ids = subject_ids[::2]
    dataset.include = {
        "subject": restricted_subject_ids
    }  # Include every second session
    num_sessions = int(
        reduce(op.mul, blueprint.dim_lengths, 1)
        * len(restricted_subject_ids)
        / len(subject_ids)
    )
    with dataset.tree:
        assert len(dataset.tree.root.children[MedImage.session]) == num_sessions


def test_populate_row(static_dataset: FrameSet) -> None:
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


def test_get(static_dataset: FrameSet, caplog: Any) -> None:
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
        for row in static_dataset.rows(str(MedImage.session)):
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
                    p.name for p in item.fspaths if not p.name.endswith("catalog.xml")  # type: ignore[attr-defined]
                )
                assert item_files == sorted(Path(f).name for f in files)
    method_str = "direct" if type(static_dataset.store) is XnatViaCS else "api"
    assert f"{method_str} access" in caplog.text.lower()


def test_post(dataset: FrameSet, source_data: Path, caplog: Any) -> None:
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
        all_checksums[deriv_bp.path] = item.hash_files()  # type: ignore[attr-defined]
        # Insert into first row of that row_frequency in dataset
        row = next(iter(dataset.rows(deriv_bp.row_frequency)))
        with caplog.at_level(logging.INFO, logger="frametree"):
            row[deriv_bp.path] = item
        method_str = "direct" if is_direct else "api"
        assert f"{method_str} access" in caplog.text.lower()

    access_method = "cs" if is_direct else "api"

    def check_inserted() -> None:
        for deriv_bp in blueprint.derivatives:
            row = next(iter(dataset.rows(deriv_bp.row_frequency)))
            cell = row.cell(deriv_bp.path, allow_empty=False)
            item = cell.item
            assert isinstance(item, deriv_bp.datatype)
            assert item.hash_files() == all_checksums[deriv_bp.path]  # type: ignore[attr-defined]

    if access_method == "api":
        check_inserted()  # Check cache
        # Check downloaded by deleting the cache dir
        shutil.rmtree(dataset.store.cache_dir / "projects" / dataset.id)
        check_inserted()


def test_frameset_roundtrip(simple_dataset: FrameSet) -> None:
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
def test_provenance_roundtrip(
    datatype: type[Any], value: str, simple_dataset: FrameSet
) -> None:
    provenance = {"a": 1, "b": [1, 2, 3], "c": {"x": True, "y": "foo", "z": "bar"}}
    data_store = simple_dataset.store

    with data_store.connection:
        entry = data_store.create_entry("provtest@", datatype, simple_dataset.root)
        data_store.put(
            datatype(value), entry
        )  # Create the entry first  # type: ignore[misc]
        data_store.put_provenance(provenance, entry)  # Save the provenance
        reloaded_provenance = data_store.get_provenance(entry)  # reload the provenance
        assert provenance == reloaded_provenance


def test_dataset_bytes_hash(static_dataset: FrameSet) -> None:

    hsh = hash_object(static_dataset)
    # Check hashing is stable
    assert hash_object(static_dataset) == hsh


def test_session_datetime_sorting(
    xnat_repository: Xnat,
    xnat_archive_dir: Path,
    source_data: Path,
    run_prefix: str,
) -> None:
    """Creates a dataset that with session date"""
    blueprint = TestXnatDatasetBlueprint(  # type: ignore[call-arg]
        dim_lengths=[2, 1, 1],  # number of visits, groups and members respectively
        scans=[
            ScanBP(
                name="scan1",  # scan type (ID is index)
                resources=[
                    FileBP(
                        path="Text",
                        datatype=TextFile,
                        filenames=["file.txt"],  # resource name  # Data datatype
                    )
                ],
            ),
        ],
    )
    project_id = run_prefix + "datecompare" + str(hex(random.getrandbits(16)))[2:]
    blueprint.make_dataset(
        dataset_id=project_id,
        store=xnat_repository,
        source_data=source_data,
        name="",
    )
    with xnat_repository.connection:
        xproject = xnat_repository.connection.projects[project_id]
        xsubject = next(iter(xproject.subjects.values()))
        xsession = xsubject.experiments["visit0group0member0"]
        xsession.date = datetime.today()
        xsession.time = datetime.now().time()

    dataset = access_dataset(
        project_id, "api", xnat_repository, xnat_archive_dir, run_prefix
    )
    assert list(dataset.row_ids()) == ["visit1group0member0", "visit0group0member0"]


def test_duplicate_entry_access(
    xnat_repository: Xnat, tmp_path: Path, run_prefix: str
) -> None:

    blueprint = TestXnatDatasetBlueprint(  # type:  ignore[call-arg]
        dim_lengths=[1, 1, 1],
        scans=[
            ScanBP(
                name="a_scan",  # scan type (ID is index)
                resources=[
                    FileBP(
                        path="Text",
                        datatype=PlainText,
                        filenames=["1.txt"],  # resource name  # Data datatype
                    )
                ],
                id="1",
            ),
            ScanBP(
                name="a_scan",  # scan type (ID is index)
                resources=[
                    FileBP(
                        path="Text",
                        datatype=PlainText,
                        filenames=["2.txt"],  # resource name  # Data datatype
                    )
                ],
                id="2",
            ),
        ],
    )

    project_id = run_prefix + "duplicatescans"
    dataset: FrameSet = blueprint.make_dataset(xnat_repository, project_id)
    row = dataset.row(id="visit0group0member0", frequency="session")
    assert PlainText(row.entry("a_scan/Text", order=0).item).contents == "1.txt"
    assert PlainText(row.entry("a_scan/Text", order=1).item).contents == "2.txt"
    assert PlainText(row.entry("a_scan/Text", order=-1).item).contents == "2.txt"
    assert PlainText(row.entry("a_scan/Text", order=-2).item).contents == "1.txt"
    assert PlainText(row.entry("a_scan/Text", key="1").item).contents == "1.txt"
    assert PlainText(row.entry("a_scan/Text", key="2").item).contents == "2.txt"
    with pytest.raises(ValueError):
        row.entry("a_scan", order=0, key="0")


def test_single_load(
    xnat_repository: Xnat,
    run_prefix: str,
    caplog: pytest.LogCaptureFixture,
) -> None:

    blueprint = TestXnatDatasetBlueprint(  # type:  ignore[call-arg]
        dim_lengths=[1, 1, 2],
        scans=[
            ScanBP(
                name="a_scan",  # scan type (ID is index)
                resources=[
                    FileBP(
                        path="Text",
                        datatype=PlainText,
                        filenames=["1.txt"],  # resource name  # Data datatype
                    )
                ],
                id="1",
            ),
        ],
    )

    project_id = run_prefix + "singleload"
    frameset = blueprint.make_dataset(xnat_repository, project_id)

    assert len(list(frameset.rows("session"))) == 2

    logging.getLogger("frametree").setLevel(logging.DEBUG)

    frameset = xnat_repository.load_frameset(
        id=project_id,
        name="",
        include={"session": ["visit0group0member0"]},
    )

    assert len(list(frameset.rows("session"))) == 1
    assert caplog.text.count("Adding leaf to data tree at path") == 1
