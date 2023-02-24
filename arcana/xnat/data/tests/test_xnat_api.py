import os
import sys
import os.path
import operator as op
import shutil
import logging
from pathlib import Path
from tempfile import mkdtemp
from functools import reduce
from fileformats.core import FileSet
from arcana.core.data.store import DataStore
from arcana.core.data.space import Clinical
from arcana.core.data.set import Dataset
from arcana.xnat.data import XnatViaCS

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


# logger = logging.getLogger('arcana')
# logger.setLevel(logging.INFO)


def test_find_rows(xnat_dataset):
    blueprint = xnat_dataset.__annotations__["blueprint"]
    for freq in Clinical:
        # For all non-zero bases in the row_frequency, multiply the dim lengths
        # together to get the combined number of rows expected for that
        # row_frequency
        num_rows = reduce(
            op.mul,
            (ln for ln, b in zip(blueprint.dim_lengths, freq) if b),
            1,
        )
        assert len(xnat_dataset.rows(freq)) == num_rows, (
            f"{freq} doesn't match {len(xnat_dataset.rows(freq))}" f" vs {num_rows}"
        )


def test_get_items(xnat_dataset, caplog):
    blueprint = xnat_dataset.__annotations__["blueprint"]
    expected_files = {}
    for scan in blueprint.scans:
        for resource in scan.resources:
            if resource.datatype is not None:
                source_name = scan.name + resource.name
                xnat_dataset.add_source(
                    source_name, path=scan.name, datatype=resource.datatype
                )
                expected_files[source_name] = set(resource.filenames)
    with caplog.at_level(logging.INFO, logger="arcana"):
        for row in xnat_dataset.rows(Clinical.session):
            for source_name, files in expected_files.items():
                try:
                    item = row[source_name]
                except PermissionError:
                    archive_dir = str(
                        Path.home()
                        / ".xnat4tests"
                        / "xnat_root"
                        / "archive"
                        / xnat_dataset.id
                    )
                    archive_perms = get_perms(archive_dir)
                    current_user = os.getlogin()
                    msg = (
                        f"Error accessing {item} as '{current_user}' when "
                        f"'{archive_dir}' has {archive_perms} permissions"
                    )
                    raise PermissionError(msg)
                if item.is_dir:
                    fspaths = item.fspath.iterdir()
                else:
                    fspaths = item.fspaths
                item_files = sorted(
                    p.name for p in fspaths if not p.name.endswith("catalog.xml")
                )
                assert item_files == sorted(Path(f).name for f in files)
    method_str = "direct" if type(xnat_dataset.store) is XnatViaCS else "api"
    assert f"{method_str} access" in caplog.text.lower()


def test_put_items(mutable_dataset: Dataset, source_data: Path, caplog):
    blueprint = mutable_dataset.__annotations__["blueprint"]
    all_checksums = {}
    tmp_dir = Path(mkdtemp())
    for deriv in blueprint.derivatives:
        mutable_dataset.add_sink(
            name=deriv.name, datatype=deriv.datatype, row_frequency=deriv.row_frequency
        )
        deriv_tmp_dir = tmp_dir / deriv.name
        # Create test files, calculate checksums and recorded expected paths
        # for inserted files
        fspaths = []
        for fname in deriv.filenames:
            fspaths.append(
                DataStore.create_test_fsobject(
                    fname=fname,
                    dpath=deriv_tmp_dir,
                    source_data=source_data,
                    source_fallback=True,
                )
            )

        # if len(fspaths) == 1 and fspaths[0].is_dir():
        #     relative_to = fspaths[0]
        # else:
        #     relative_to = deriv_tmp_dir
        all_checksums[deriv.name] = deriv.datatype(fspaths).hash_files()
        # Insert into first row of that row_frequency in xnat_dataset
        row = next(iter(mutable_dataset.rows(deriv.row_frequency)))
        with caplog.at_level(logging.INFO, logger="arcana"):
            row[deriv.name] = fspaths
        method_str = "direct" if type(mutable_dataset.store) is XnatViaCS else "api"
        assert f"{method_str} access" in caplog.text.lower()

    access_method = "cs" if type(mutable_dataset.store) is XnatViaCS else "api"

    def check_inserted():
        for deriv in blueprint.derivatives:
            row = next(iter(mutable_dataset.rows(deriv.row_frequency)))
            cell = row.cell(deriv.name, allow_empty=False)
            item = cell.item
            assert isinstance(item, deriv.datatype)
            assert item.hash_files() == all_checksums[deriv.name]

    if access_method == "api":
        check_inserted()  # Check cache
        # Check downloaded by deleting the cache dir
        shutil.rmtree(mutable_dataset.store.cache_dir / "projects" / mutable_dataset.id)
        check_inserted()
