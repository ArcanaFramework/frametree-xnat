from __future__ import annotations
from pathlib import Path
import docker
from warnings import warn
import requests
from fileformats.medimage import NiftiGzX, NiftiGzXBvec
from arcana.core.cli.dataset import export
from arcana.core.utils.misc import show_cli_trace
from arcana.xnat import Xnat
from arcana.bids import Bids
from conftest import (
    TestXnatDatasetBlueprint,
    ScanBP,
    FileBP,
)


def test_bids_export(
    xnat_repository: Xnat,
    cli_runner,
    work_dir: Path,
    arcana_home: str,
    run_prefix: str,
    nifti_sample_dir: Path,
    bids_validator_docker: str,
    bids_success_str: str,
):

    blueprint = TestXnatDatasetBlueprint(
        dim_lengths=[2, 2, 2],
        scans=[
            ScanBP(
                "mprage",
                [
                    FileBP(
                        path="NiftiGzX",
                        datatype=NiftiGzX,
                        filenames=["anat/T1w.nii.gz", "anat/T1w.json"],
                    )
                ],
            ),
            ScanBP(
                "flair",
                [
                    FileBP(
                        path="NiftiGzX",
                        datatype=NiftiGzX,
                        filenames=["anat/T2w.nii.gz", "anat/T2w.json"],
                    )
                ],
            ),
            ScanBP(
                "diffusion",
                [
                    FileBP(
                        path="NiftiGzXBvec",
                        datatype=NiftiGzXBvec,
                        filenames=[
                            "dwi/dwi.nii.gz",
                            "dwi/dwi.json",
                            "dwi/dwi.bvec",
                            "dwi/dwi.bval",
                        ],
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
        source_data=nifti_sample_dir,
    )
    original.add_source(
        name="anat/T1w",
        datatype=NiftiGzX,
        path="mprage",
    )
    original.add_source(
        name="anat/T2w",
        datatype=NiftiGzX,
        path="flair",
    )
    original.add_source(
        name="dwi/dwi",
        datatype=NiftiGzXBvec,
        path="diffusion",
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
    assert sorted(bids_dataset.columns) == ["anat/T1w", "anatT2w", "dwi/dwi"]

    # Full dataset validation using dockerized validator
    dc = docker.from_env()
    try:
        dc.images.pull(bids_validator_docker)
    except requests.exceptions.HTTPError:
        warn("No internet connection, so couldn't download latest BIDS validator")
    result = dc.containers.run(
        bids_validator_docker,
        "/data",
        volumes=[f"{bids_dataset_path}:/data:ro"],
        remove=True,
        stderr=True,
    ).decode("utf-8")
    assert bids_success_str in result
