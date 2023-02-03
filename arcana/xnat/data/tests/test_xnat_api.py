import os
import sys
import os.path
import operator as op
import shutil
import logging
from pathlib import Path
import hashlib
from tempfile import mkdtemp
from functools import reduce
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
                item = row[source_name]
                try:
                    item.get()
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
                    item_files = set(os.listdir(item.fspath))
                else:
                    item_files = set(p.name for p in item.fspaths)
                assert item_files == files
    method_str = "direct" if type(xnat_dataset.store) is XnatViaCS else "api"
    assert f"{method_str} access" in caplog.text.lower()


def test_put_items(mutable_dataset: Dataset, caplog):
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
        all_checksums[deriv.name] = checksums = {}
        fspaths = []
        for fname in deriv.filenames:
            test_file = DataStore.create_test_data_item(fname, deriv_tmp_dir)
            fhash = hashlib.md5()
            with open(deriv_tmp_dir / test_file, "rb") as f:
                fhash.update(f.read())
            try:
                rel_path = str(test_file.relative_to(Path(deriv.filenames[0])))
            except ValueError:
                rel_path = ".".join(test_file.suffixes)[1:]
            checksums[rel_path] = fhash.hexdigest()
            fspaths.append(deriv_tmp_dir / test_file.parts[0])
        # Insert into first row of that row_frequency in xnat_dataset
        row = next(iter(mutable_dataset.rows(deriv.row_frequency)))
        item = row[deriv.name]
        with caplog.at_level(logging.INFO, logger="arcana"):
            item.put(*fspaths)
        method_str = "direct" if type(mutable_dataset.store) is XnatViaCS else "api"
        assert f"{method_str} access" in caplog.text.lower()

    access_method = "cs" if type(mutable_dataset.store) is XnatViaCS else "api"

    def check_inserted():
        for deriv in blueprint.derivatives:
            row = next(iter(mutable_dataset.rows(deriv.row_frequency)))
            item = row[deriv.name]
            item.get_checksums(force_calculate=(access_method == "cs"))
            assert isinstance(item, deriv.datatype)
            assert item.checksums == all_checksums[deriv.name]
            item.get()
            assert all(p.exists() for p in item.fspaths)

    if access_method == "api":
        check_inserted()
        # Check read from cached files
        mutable_dataset.refresh()
        # Note that we can't check the direct access put by this method since
        # it isn't registered with the XNAT database and therefore isn't
        # found by `find_cells`. In real life this is handled by the output
        # handlers of the container service
        check_inserted()
        # Check downloaded by deleting the cache dir
        shutil.rmtree(
            mutable_dataset.store.cache_dir / "projects" / mutable_dataset.id
        )
        mutable_dataset.refresh()
        check_inserted()
