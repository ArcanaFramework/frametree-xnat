import typing as ty
from pathlib import Path
from types import SimpleNamespace

import pytest
from fileformats.core import FileSet
from fileformats.core.exceptions import FormatMismatchError
from fileformats.generic import Directory
from fileformats.image import Png
from frametree.axes.medimage import MedImage

from frametree.xnat import XnatViaCS

BUNDLE_CONTENTS = ["DWI", "Response", "execution_log.txt"]


def make_bundle(dpath: Path) -> Path:
    """Makes a directory with several entries, like the bundled output directory of a
    pipeline"""
    (dpath / "DWI").mkdir(parents=True)
    (dpath / "DWI" / "dwi.mif").write_text("dwi")
    (dpath / "Response").mkdir()
    (dpath / "Response" / "wm.txt").write_text("wm")
    (dpath / "execution_log.txt").write_text("log")
    return dpath


def direct_mount_store(tmp_path: Path) -> XnatViaCS:
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    return XnatViaCS(
        server="http://xnat.example.com",
        user="user",
        password="password",
        cache_dir=cache_dir,
        row_frequency=MedImage.session,
        input_mount=tmp_path / "input",
        output_mount=tmp_path / "output",
    )


def session_resource_entry(resource_name: str) -> ty.Any:
    """A stand-in for an entry of a derivative resource of a session, which is enough to
    access it from the input mount"""
    return SimpleNamespace(
        path=resource_name + "@",
        is_derivative=True,
        uri=(
            "/data/projects/PROJ/subjects/SUBJ01/experiments/SUBJ01_MR01/resources/"
            + resource_name
        ),
        row=SimpleNamespace(frequency=MedImage.session, id="SUBJ01_MR01"),
    )


@pytest.mark.parametrize(
    "datatype", [Directory, ty.Optional[Directory], Directory | None]
)
def test_get_directory_contents_at_resource_root(
    tmp_path: Path, datatype: type
) -> None:
    """Checks that a directory can be loaded via the input mount from a resource that
    holds the contents of the directory at its root, e.g. as created by uploading a
    directory with `xnat`'s `upload_dir(<dir>)`"""
    store = direct_mount_store(tmp_path)
    resource_dir = make_bundle(tmp_path / "input" / "RESOURCES" / "dwi_preprocess")
    fspaths = store.get_fileset(session_resource_entry("dwi_preprocess"), datatype)
    directory = Directory(fspaths)
    # NB: compared with samefile as the input mount is case-insensitive on macOS, so
    # it can be found at "resources" as well as "RESOURCES"
    assert directory.fspath.samefile(resource_dir)
    assert sorted(p.name for p in directory.fspath.iterdir()) == BUNDLE_CONTENTS


def test_get_nested_directory(tmp_path: Path) -> None:
    """Checks that a directory nested within a resource, as stored by frametree's sink
    columns, is still loaded from the nested directory"""
    store = direct_mount_store(tmp_path)
    resource_dir = tmp_path / "input" / "RESOURCES" / "dwi_preprocess"
    make_bundle(resource_dir / "bundle")
    fspaths = store.get_fileset(session_resource_entry("dwi_preprocess"), Directory)
    assert Directory(fspaths).fspath.samefile(resource_dir / "bundle")


def test_get_fileset_resource_contents(tmp_path: Path) -> None:
    """Checks that non-directory datatypes are still passed the resource's contents"""
    store = direct_mount_store(tmp_path)
    resource_dir = make_bundle(tmp_path / "input" / "RESOURCES" / "dwi_preprocess")
    fspaths = store.get_fileset(session_resource_entry("dwi_preprocess"), FileSet)
    assert sorted(p.name for p in fspaths) == BUNDLE_CONTENTS
    assert all(p.parent.samefile(resource_dir) for p in fspaths)


def test_get_optional_mismatch(tmp_path: Path) -> None:
    """Checks that a resource that doesn't match an optional datatype raises a
    FormatMismatchError (rather than trying to pass the paths to NoneType)"""
    store = direct_mount_store(tmp_path)
    make_bundle(tmp_path / "input" / "RESOURCES" / "dwi_preprocess")
    with pytest.raises(FormatMismatchError):
        store.get_fileset(session_resource_entry("dwi_preprocess"), ty.Optional[Png])
