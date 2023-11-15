from __future__ import annotations
from pathlib import Path
import docker
from warnings import warn
import tempfile
import requests
from docker.errors import ContainerError
import pytest
from fileformats.medimage import DicomSeries
import medimages4tests.dummy.dicom.mri.t1w.siemens.skyra.syngo_d13c
import medimages4tests.dummy.dicom.mri.fmap.siemens.skyra.syngo_d13c
from arcana.core.cli.dataset import export
from arcana.core.utils.misc import show_cli_trace
from arcana.xnat import Xnat
from arcana.bids import Bids
from arcana.core.utils.misc import add_exc_note
from conftest import (
    TestXnatDatasetBlueprint,
    ScanBP,
    FileBP,
)


@pytest.fixture(scope="session")
def source_dicom_data():
    source_data = Path(tempfile.mkdtemp())
    # Create DICOM data
    dicom_dir = source_data / "dicom"
    dicom_dir.mkdir()
    medimages4tests.dummy.dicom.mri.t1w.siemens.skyra.syngo_d13c.get_image(
        out_dir=dicom_dir / "t1w"
    )
    medimages4tests.dummy.dicom.mri.fmap.siemens.skyra.syngo_d13c.get_image(
        out_dir=dicom_dir / "fmap"
    )
    return source_data


def test_bids_export(
    xnat_repository: Xnat,
    cli_runner,
    work_dir: Path,
    arcana_home: str,
    run_prefix: str,
    nifti_sample_dir: Path,
    bids_validator_docker: str,
    bids_success_str: str,
    source_dicom_data: Path,
):

    blueprint = TestXnatDatasetBlueprint(
        dim_lengths=[2, 2, 2],
        scans=[
            ScanBP(
                "mprage",
                [
                    FileBP(
                        path="DICOM",
                        datatype=DicomSeries,
                        filenames=["dicom/t1w/*"],
                    )
                ],
            ),
        ],
        id_patterns={
            # "timepoint": "session:order",
            "group": r"subject::group(\d+).*",
            "member": r"subject::group\d+member(\d+)",
        }
    )
    project_id = run_prefix + "bids_export"
    original = blueprint.make_dataset(
        store=xnat_repository,
        dataset_id=project_id,
        source_data=source_dicom_data,
        metadata={'authors': ["some.one@an.org", "another.person@another.org"]}
    )
    original.add_source(
        name="anat/T1w",
        datatype=DicomSeries,
        path="mprage",
    )
    original.save()
    bids_dataset_path = str(work_dir / "exported-bids")
    xnat_repository.save("myxnat")
    # Add source column to saved dataset
    result = cli_runner(
        export,
        [
            original.locator,
            "bids",
            bids_dataset_path,
            "--hierarchy",
            "group,subject,timepoint"
        ],
    )
    assert result.exit_code == 0, show_cli_trace(result)
    bids_dataset = Bids().load_dataset(bids_dataset_path)
    assert sorted(bids_dataset.columns) == ["anat/T1w"]

    # Full dataset validation using dockerized validator
    dc = docker.from_env()
    try:
        dc.images.pull(bids_validator_docker)
    except requests.exceptions.HTTPError:
        warn("No internet connection, so couldn't download latest BIDS validator")
    try:
        result = dc.containers.run(
            bids_validator_docker,
            "/data",
            volumes=[f"{bids_dataset_path}:/data:ro"],
            remove=True,
            stderr=True,
        ).decode("utf-8")
    except ContainerError as e:
        add_exc_note(e, f"attempting to run:\n\ndocker run --rm -v {bids_dataset_path}:/data:ro {bids_validator_docker} /data")
    assert bids_success_str in result
