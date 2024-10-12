# flake8: noqa: E501
import os
import logging
import sys
from tempfile import mkdtemp
from unittest.mock import patch
import json
import tempfile
from datetime import datetime
from pathlib import Path
from warnings import warn
import pytest
import requests
import numpy
import docker
import random
import nibabel
from click.testing import CliRunner
from imageio.core.fetching import get_remote_file
import xnat4tests
import medimages4tests.dummy.nifti
import medimages4tests.dummy.dicom.mri.fmap.siemens.skyra.syngo_d13c
from pipeline2app.core.image.base import BaseImage
from frametree.common import Clinical
from frametree.core.frameset import FrameSet
from fileformats.medimage import NiftiGzX, NiftiGz, DicomSeries, NiftiX
from fileformats.text import Plain as Text
from fileformats.image import Png
from fileformats.application import Json
from fileformats.generic import Directory
from frametree.xnat.api import Xnat
from frametree.xnat.testing import (
    TestXnatDatasetBlueprint,
    FileSetEntryBlueprint as FileBP,
    ScanBlueprint as ScanBP,
)
from frametree.xnat.cs import XnatViaCS

try:
    from pydra import set_input_validator
except ImportError:
    pass
else:
    set_input_validator(True)

# For debugging in IDE's don't catch raised exceptions and let the IDE
# break at it
if os.getenv("_PYTEST_RAISE", "0") != "0":

    @pytest.hookimpl(tryfirst=True)
    def pytest_exception_interact(call):
        raise call.excinfo.value

    @pytest.hookimpl(tryfirst=True)
    def pytest_internalerror(excinfo):
        raise excinfo.value

    CATCH_CLI_EXCEPTIONS = False
else:
    CATCH_CLI_EXCEPTIONS = True


@pytest.fixture
def catch_cli_exceptions():
    return CATCH_CLI_EXCEPTIONS


PKG_DIR = Path(__file__).parent


log_level = logging.WARNING

logger = logging.getLogger("frametree")
logger.setLevel(log_level)

sch = logging.StreamHandler()
sch.setLevel(log_level)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sch.setFormatter(formatter)
logger.addHandler(sch)

logger = logging.getLogger("frametree")
logger.setLevel(log_level)

sch = logging.StreamHandler()
sch.setLevel(log_level)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sch.setFormatter(formatter)
logger.addHandler(sch)


@pytest.fixture(scope="session")
def run_prefix():
    "A datetime string used to avoid stale data left over from previous tests"
    return datetime.strftime(datetime.now(), "%Y%m%d%H%M%S")


@pytest.fixture
def cli_runner(catch_cli_exceptions):
    def invoke(*args, catch_exceptions=catch_cli_exceptions, **kwargs):
        runner = CliRunner()
        result = runner.invoke(*args, catch_exceptions=catch_exceptions, **kwargs)
        return result

    return invoke


@pytest.fixture
def work_dir() -> Path:
    work_dir = tempfile.mkdtemp()
    return Path(work_dir)


@pytest.fixture(scope="session")
def build_cache_dir():
    return Path(mkdtemp())


@pytest.fixture(scope="session")
def pkg_dir():
    return PKG_DIR


@pytest.fixture
def frametree_home(work_dir):
    frametree_home = work_dir / "frametree-home"
    with patch.dict(os.environ, {"FRAMETREE_HOME": str(frametree_home)}):
        yield frametree_home


# -----------------------
# Test dataset structures
# -----------------------


TEST_XNAT_DATASET_BLUEPRINTS = {
    "basic": TestXnatDatasetBlueprint(  # dataset name
        dim_lengths=[1, 1, 3],  # number of visits, groups and members respectively
        scans=[
            ScanBP(
                name="scan1",  # scan type (ID is index)
                resources=[
                    FileBP(
                        path="Text",
                        datatype=Text,
                        filenames=["file.txt"],  # resource name  # Data datatype
                    )
                ],
            ),  # name files to place within resource
            ScanBP(
                name="scan2",
                resources=[
                    FileBP(
                        path="NiftiGzX",
                        datatype=NiftiGzX,
                        filenames=["nifti/anat/T1w.nii.gz", "nifti/anat/T1w.json"],
                    )
                ],
            ),
            ScanBP(
                name="scan3",
                resources=[
                    FileBP(
                        path="Directory",
                        datatype=Directory,
                        filenames=["doubledir"],
                    )
                ],
            ),
            ScanBP(
                name="scan4",
                resources=[
                    FileBP(
                        path="DICOM",
                        datatype=DicomSeries,
                        filenames=[
                            "dicom/fmap/1.dcm",
                            "dicom/fmap/2.dcm",
                            "dicom/fmap/3.dcm",
                        ],
                    ),
                    FileBP(
                        path="NIFTI",
                        datatype=NiftiGz,
                        filenames=["nifti/anat/T2w.nii.gz"],
                    ),
                    FileBP(
                        path="BIDS", datatype=Json, filenames=["nifti/anat/T2w.json"]
                    ),
                    FileBP(
                        path="SNAPSHOT", datatype=Png, filenames=["images/chelsea.png"]
                    ),
                ],
            ),
        ],
        derivatives=[
            FileBP(
                path="deriv1",
                row_frequency=Clinical.visit,
                datatype=Text,
                filenames=["file.txt"],
            ),
            FileBP(
                path="deriv2",
                row_frequency=Clinical.subject,
                datatype=NiftiGzX,
                filenames=["nifti/anat/T1w.nii.gz", "nifti/anat/T1w.json"],
            ),
            FileBP(
                path="deriv3",
                row_frequency=Clinical.groupedvisit,
                datatype=Directory,
                filenames=["dir"],
            ),
            FileBP(
                path="deriv4",
                row_frequency=Clinical.constant,
                datatype=Text,
                filenames=["file.txt"],
            ),
        ],
    ),
    "multi": TestXnatDatasetBlueprint(  # dataset name
        dim_lengths=[2, 2, 2],  # number of visits, groups and members respectively
        scans=[
            ScanBP(
                name="scan1",
                resources=[FileBP(path="Text", datatype=Text, filenames=["file.txt"])],
            )
        ],
        id_patterns={
            "group": r"subject::group(\d+)member\d+",
            "member": r"subject::group\d+member(\d+)",
            "visit": r"session::visit(\d+).*",
        },
        derivatives=[
            FileBP(
                path="deriv1",
                row_frequency=Clinical.session,
                datatype=Text,
                filenames=["file.txt"],
            ),
            FileBP(
                path="deriv2",
                row_frequency=Clinical.subject,
                datatype=NiftiGzX,
                filenames=["nifti/anat/T2w.nii.gz", "nifti/anat/T2w.json"],
            ),
            FileBP(
                path="deriv3",
                row_frequency=Clinical.visit,
                datatype=Directory,
                filenames=["doubledir"],
            ),
            FileBP(
                path="deriv4",
                row_frequency=Clinical.member,
                datatype=Text,
                filenames=["file.txt"],
            ),
            FileBP(
                path="deriv5",
                row_frequency=Clinical.constant,
                datatype=Text,
                filenames=["file.txt"],
            ),
            FileBP(
                path="deriv6",
                row_frequency=Clinical.groupedvisit,
                datatype=Text,
                filenames=["file.txt"],
            ),
            FileBP(
                path="deriv7",
                row_frequency=Clinical.matchedvisit,
                datatype=Text,
                filenames=["file.txt"],
            ),
            FileBP(
                path="deriv8",
                row_frequency=Clinical.group,
                datatype=Text,
                filenames=["file.txt"],
            ),
        ],
    ),
    "concatenate_test": TestXnatDatasetBlueprint(
        dim_lengths=[1, 1, 2],
        scans=[
            ScanBP(
                name="scan1",
                resources=[FileBP(path="Text", datatype=Text, filenames=["file1.txt"])],
            ),
            ScanBP(
                name="scan2",
                resources=[FileBP(path="Text", datatype=Text, filenames=["file2.txt"])],
            ),
        ],
        derivatives=[
            FileBP(
                path="concatenated_file",
                row_frequency=Clinical.session,
                datatype=Text,
                filenames=["concatenated.txt"],
            )
        ],
    ),
}

GOOD_DATASETS = ["basic.api", "multi.api", "basic.cs", "multi.cs"]
MUTABLE_DATASETS = ["basic.api", "multi.api", "basic.cs", "multi.cs"]

# ------------------------------------
# Pytest fixtures and helper functions
# ------------------------------------


@pytest.fixture(params=GOOD_DATASETS, scope="session")
def static_dataset(
    xnat_repository: Xnat,
    xnat_archive_dir: Path,
    source_data: Path,
    run_prefix: str,
    request,
):
    """Creates a static dataset to can be reused between unittests and save setup
    times"""
    dataset_id, access_method = request.param.split(".")
    blueprint = TEST_XNAT_DATASET_BLUEPRINTS[dataset_id]
    project_id = run_prefix + dataset_id + str(hex(random.getrandbits(16)))[2:]
    logger.debug("Making dataset at %s", project_id)
    blueprint.make_dataset(
        dataset_id=project_id,
        store=xnat_repository,
        source_data=source_data,
        name="",
    )
    logger.debug("accessing dataset at %s", project_id)
    return access_dataset(
        project_id, access_method, xnat_repository, xnat_archive_dir, run_prefix
    )


@pytest.fixture(params=MUTABLE_DATASETS, scope="function")
def dataset(
    xnat_repository: Xnat,
    xnat_archive_dir: Path,
    source_data: Path,
    run_prefix: str,
    request,
):
    """Creates a dataset that can be mutated (as its name is unique to the function)"""
    dataset_id, access_method = request.param.split(".")
    blueprint = TEST_XNAT_DATASET_BLUEPRINTS[dataset_id]
    project_id = (
        run_prefix
        + dataset_id
        + "mutable"
        + access_method
        + str(hex(random.getrandbits(16)))[2:]
    )
    blueprint.make_dataset(
        dataset_id=project_id,
        store=xnat_repository,
        source_data=source_data,
        name="",
    )
    return access_dataset(
        project_id, access_method, xnat_repository, xnat_archive_dir, run_prefix
    )


@pytest.fixture
def simple_dataset(xnat_repository, work_dir, run_prefix):
    blueprint = TestXnatDatasetBlueprint(
        dim_lengths=[1, 1, 1],
        scans=[
            ScanBP(
                name="scan1",
                resources=[FileBP(path="TEXT", datatype=Text, filenames=["file.txt"])],
            )
        ],
    )
    project_id = run_prefix + "simple" + str(hex(random.getrandbits(16)))[2:]
    return blueprint.make_dataset(xnat_repository, project_id, name="")


def access_dataset(
    project_id: str,
    access_method: str,
    xnat_repository: Xnat,
    xnat_archive_dir: Path,
    run_prefix: str,
) -> FrameSet:
    if access_method == "cs":
        proj_dir = xnat_archive_dir / project_id / "arc001"
        store = XnatViaCS(
            server=xnat_repository.server,
            user=xnat_repository.user,
            password=xnat_repository.password,
            cache_dir=xnat_repository.cache_dir,
            row_frequency=Clinical.constant,
            input_mount=proj_dir,
            output_mount=Path(mkdtemp()),
        )
        store.save(name=f"testxnatcs{run_prefix}")
    elif access_method == "api":
        store = xnat_repository
    else:
        assert False, f"unrecognised access method {access_method}"
    return store.load_frameset(project_id, name="")


@pytest.fixture(scope="session")
def xnat4tests_config() -> xnat4tests.Config:

    return xnat4tests.Config()


@pytest.fixture(scope="session")
def xnat_root_dir(xnat4tests_config) -> Path:
    return xnat4tests_config.xnat_root_dir


@pytest.fixture(scope="session")
def xnat_archive_dir(xnat_root_dir):
    return xnat_root_dir / "archive"


@pytest.fixture(scope="session")
def xnat_repository(run_prefix, xnat4tests_config, frametree_home):

    xnat4tests.start_xnat()

    repository = Xnat(
        server=xnat4tests_config.xnat_uri,
        user=xnat4tests_config.xnat_user,
        password=xnat4tests_config.xnat_password,
        cache_dir=mkdtemp(),
    )

    repository.save(name="testxnat")

    # Stash a project prefix in the repository object
    repository.__annotations__["run_prefix"] = run_prefix

    yield repository


@pytest.fixture(scope="session")
def xnat_via_cs_repository(run_prefix, xnat4tests_config):

    xnat4tests.start_xnat()

    repository = Xnat(
        server=xnat4tests_config.xnat_uri,
        user=xnat4tests_config.xnat_user,
        password=xnat4tests_config.xnat_password,
        cache_dir=mkdtemp(),
    )

    # Stash a project prefix in the repository object
    repository.__annotations__["run_prefix"] = run_prefix

    yield repository


@pytest.fixture(scope="session")
def xnat_respository_uri(xnat_repository):
    return xnat_repository.server


@pytest.fixture(scope="session")
def docker_registry_for_xnat():
    return xnat4tests.start_registry()


@pytest.fixture(scope="session")
def docker_registry_for_xnat_uri(docker_registry_for_xnat):
    if sys.platform == "linux":
        uri = "172.17.0.1"  # Linux + GH Actions
    else:
        uri = "host.docker.internal"  # Mac/Windows local debug
    return uri


@pytest.fixture
def dummy_niftix(work_dir):

    nifti_path = work_dir / "t1w.nii"
    json_path = work_dir / "t1w.json"

    # Create a random Nifti file to satisfy BIDS parsers
    hdr = nibabel.Nifti1Header()
    hdr.set_data_shape((10, 10, 10))
    hdr.set_zooms((1.0, 1.0, 1.0))  # set voxel size
    hdr.set_xyzt_units(2)  # millimeters
    hdr.set_qform(numpy.diag([1, 2, 3, 1]))
    nibabel.save(
        nibabel.Nifti1Image(
            numpy.random.randint(0, 1, size=[10, 10, 10]),
            hdr.get_best_affine(),
            header=hdr,
        ),
        nifti_path,
    )

    with open(json_path, "w") as f:
        json.dump({"test": "json-file"}, f)

    return NiftiX.from_fspaths(nifti_path, json_path)


@pytest.fixture(scope="session")
def command_spec():
    return {
        "task": "frametree.testing.tasks:concatenate",
        "inputs": {
            "first_file": {
                "datatype": "text/text-file",
                "field": "in_file1",
                "column_defaults": {
                    "row_frequency": "session",
                },
                "help": "the first file to pass as an input",
            },
            "second_file": {
                "datatype": "text/text-file",
                "field": "in_file2",
                "column_defaults": {
                    "row_frequency": "session",
                },
                "help": "the second file to pass as an input",
            },
        },
        "outputs": {
            "concatenated_file": {
                "datatype": "text/text-file",
                "field": "out_file",
                "help": "an output file",
            }
        },
        "parameters": {
            "duplicates": {
                "field": "duplicates",
                "default": 2,
                "datatype": "int",
                "required": True,
                "help": "a parameter",
            }
        },
        "row_frequency": "session",
    }


BIDS_VALIDATOR_DOCKER = "bids/validator:latest"
SUCCESS_STR = "This dataset appears to be BIDS compatible"
MOCK_BIDS_APP_IMAGE = "frametree-mock-bids-app"
BIDS_VALIDATOR_APP_IMAGE = "frametree-bids-validator-app"


@pytest.fixture(scope="session")
def bids_command_spec(mock_bids_app_executable):
    inputs = {
        "T1w": {
            "configuration": {
                "path": "anat/T1w",
            },
            "datatype": "medimage/nifti-gz-x",
            "help": "T1-weighted image",
        },
        "T2w": {
            "configuration": {
                "path": "anat/T2w",
            },
            "datatype": "medimage/nifti-gz-x",
            "help": "T2-weighted image",
        },
        "DWI": {
            "configuration": {
                "path": "dwi/dwi",
            },
            "datatype": "medimage/nifti-gz-x-bvec",
            "help": "DWI-weighted image",
        },
    }

    outputs = {
        "file1": {
            "configuration": {
                "path": "file1",
            },
            "datatype": "text/text-file",
            "help": "an output file",
        },
        "file2": {
            "configuration": {
                "path": "file2",
            },
            "datatype": "text/text-file",
            "help": "another output file",
        },
    }

    return {
        "task": "frametree.bids.tasks:bids_app",
        "inputs": inputs,
        "outputs": outputs,
        "row_frequency": "session",
        "configuration": {
            "inputs": inputs,
            "outputs": outputs,
            "executable": str(mock_bids_app_executable),
        },
    }


@pytest.fixture(scope="session")
def bids_success_str():
    return SUCCESS_STR


@pytest.fixture(scope="session")
def bids_validator_app_script():
    return f"""#!/bin/sh
# Echo inputs to get rid of any quotes
BIDS_DATASET=$(echo $1)
OUTPUTS_DIR=$(echo $2)
SUBJ_ID=$5
# Run BIDS validator to check whether BIDS dataset is created properly
output=$(/usr/local/bin/bids-validator "$BIDS_DATASET")
if [[ "$output" != *"{SUCCESS_STR}"* ]]; then
    echo "BIDS validation was not successful, exiting:\n "
    echo $output
    exit 1;
fi
# Write mock output files to 'derivatives' Directory
mkdir -p $OUTPUTS_DIR
echo 'file1' > $OUTPUTS_DIR/sub-${{SUBJ_ID}}_file1.txt
echo 'file2' > $OUTPUTS_DIR/sub-${{SUBJ_ID}}_file2.txt
"""


# FIXME: should be converted to python script to be Windows compatible
@pytest.fixture(scope="session")
def mock_bids_app_script():
    file_tests = ""
    for inpt_path, datatype in [
        ("anat/T1w", NiftiGzX),
        ("anat/T2w", NiftiGzX),
        ("dwi/dwi", NiftiGzX),
    ]:
        subdir, suffix = inpt_path.split("/")
        file_tests += f"""
        if [ ! -f "$BIDS_DATASET/sub-${{SUBJ_ID}}/{subdir}/sub-${{SUBJ_ID}}_{suffix}{datatype.ext}" ]; then
            echo "Did not find {suffix} file at $BIDS_DATASET/sub-${{SUBJ_ID}}/{subdir}/sub-${{SUBJ_ID}}_{suffix}{datatype.ext}"
            exit 1;
        fi
        """

    return f"""#!/bin/sh
BIDS_DATASET=$1
OUTPUTS_DIR=$2
SUBJ_ID=$5
{file_tests}
# Write mock output files to 'derivatives' Directory
mkdir -p $OUTPUTS_DIR
echo 'file1' > $OUTPUTS_DIR/sub-${{SUBJ_ID}}_file1.txt
echo 'file2' > $OUTPUTS_DIR/sub-${{SUBJ_ID}}_file2.txt
"""


@pytest.fixture(scope="session")
def mock_bids_app_executable(build_cache_dir, mock_bids_app_script):
    # Create executable that runs validator then produces some mock output
    # files
    script_path = build_cache_dir / "mock-bids-app-executable.sh"
    with open(script_path, "w") as f:
        f.write(mock_bids_app_script)
    os.chmod(script_path, 0o777)
    return script_path


@pytest.fixture(scope="session")
def mock_bids_app_image(mock_bids_app_script, build_cache_dir):
    return build_app_image(
        MOCK_BIDS_APP_IMAGE,
        mock_bids_app_script,
        build_cache_dir,
        base_image=BaseImage().reference,
    )


@pytest.fixture(scope="session")
def bids_validator_docker():
    dc = docker.from_env()
    try:
        dc.images.pull(BIDS_VALIDATOR_DOCKER)
    except requests.exceptions.HTTPError:
        warn("No internet connection, so couldn't download latest BIDS validator")
    return BIDS_VALIDATOR_DOCKER


def build_app_image(tag_name, script, build_cache_dir, base_image):
    dc = docker.from_env()

    # Create executable that runs validator then produces some mock output
    # files
    build_dir = build_cache_dir / tag_name.replace(":", "__i__")
    build_dir.mkdir()
    launch_sh = build_dir / "launch.sh"
    with open(launch_sh, "w") as f:
        f.write(script)

    # Build mock BIDS app image
    with open(build_dir / "Dockerfile", "w") as f:
        f.write(
            f"""FROM {base_image}
ADD ./launch.sh /launch.sh
RUN chmod +x /launch.sh
ENTRYPOINT ["/launch.sh"]"""
        )

    dc.images.build(path=str(build_dir), tag=tag_name)

    return tag_name


@pytest.fixture(scope="session")
def source_data():
    source_data = Path(tempfile.mkdtemp())
    # Create NIFTI data
    nifti_dir = source_data / "nifti"
    nifti_dir.mkdir()
    for fname, fdata in NIFTI_DATA_SPEC.items():
        fpath = nifti_dir.joinpath(*fname.split("/"))
        fpath.parent.mkdir(exist_ok=True, parents=True)
        if fname.endswith(".nii.gz") or fname.endswith(".nii"):
            medimages4tests.dummy.nifti.get_image(out_file=fpath)
        elif fname.endswith(".json"):
            with open(fpath, "w") as f:
                json.dump(fdata, f)
        else:
            with open(fpath, "w") as f:
                f.write(fdata)
    # Create DICOM data
    dicom_dir = source_data / "dicom"
    dicom_dir.mkdir()
    medimages4tests.dummy.dicom.mri.fmap.siemens.skyra.syngo_d13c.get_image(
        out_dir=dicom_dir / "fmap"
    )
    # Create png data
    get_remote_file("images/chelsea.png", directory=source_data)
    return source_data


@pytest.fixture(scope="session")
def nifti_sample_dir(source_data):
    return source_data / "nifti"


NIFTI_DATA_SPEC = {
    "anat/T1w.nii.gz": None,
    "anat/T1w.json": {
        "Modality": "MR",
        "MagneticFieldStrength": 3,
        "ImagingFrequency": 123.252,
        "Manufacturer": "Siemens",
        "ManufacturersModelName": "Skyra",
        "InstitutionName": "Monash Biomedical Imaging",
        "InstitutionalDepartmentName": "Department",
        "InstitutionAddress": "Blackburn Road 770,Clayton,Victoria,AU,3800",
        "DeviceSerialNumber": "45193",
        "StationName": "AWP45193",
        "BodyPartExamined": "BRAIN",
        "PatientPosition": "HFS",
        "ProcedureStepDescription": "MR Scan",
        "SoftwareVersions": "syngo MR D13C",
        "MRAcquisitionType": "3D",
        "SeriesDescription": "t1_mprage_sag_p2_iso_1_ADNI",
        "ProtocolName": "t1_mprage_sag_p2_iso_1_ADNI",
        "ScanningSequence": "GR\\IR",
        "SequenceVariant": "SK\\SP\\MP",
        "ScanOptions": "IR",
        "SequenceName": "*tfl3d1_16ns",
        "ImageType": ["ORIGINAL", "PRIMARY", "M", "ND", "NORM"],
        "SeriesNumber": 4,
        "AcquisitionTime": "15:19:35.435000",
        "AcquisitionNumber": 1,
        "SliceThickness": 1,
        "SAR": 0.0956743,
        "EchoTime": 0.00207,
        "RepetitionTime": 2.3,
        "InversionTime": 0.9,
        "FlipAngle": 9,
        "PartialFourier": 1,
        "BaseResolution": 256,
        "ShimSetting": [-4103, 15460, -15533, -6, 142, -137, -58, 61],
        "TxRefAmp": 374.478,
        "PhaseResolution": 1,
        "ReceiveCoilName": "Head_32",
        "ReceiveCoilActiveElements": "HEA;HEP",
        "PulseSequenceDetails": "%SiemensSeq%\\tfl",
        "RefLinesPE": 32,
        "ConsistencyInfo": "N4_VD13C_LATEST_20121124",
        "PercentPhaseFOV": 93.75,
        "PercentSampling": 100,
        "PhaseEncodingSteps": 239,
        "AcquisitionMatrixPE": 240,
        "ReconMatrixPE": 240,
        "ParallelReductionFactorInPlane": 2,
        "PixelBandwidth": 230,
        "DwellTime": 8.5e-06,
        "ImageOrientationPatientDICOM": [
            -0.012217,
            0.999925,
            4.08204e-08,
            -0.029664,
            -0.00036239,
            -0.99956,
        ],
        "InPlanePhaseEncodingDirectionDICOM": "ROW",
        "ConversionSoftware": "dcm2niix",
        "ConversionSoftwareVersion": "v1.0.20201102",
    },
    "anat/T2w.nii.gz": None,
    "anat/T2w.json": {
        "Modality": "MR",
        "MagneticFieldStrength": 3,
        "ImagingFrequency": 123.252,
        "Manufacturer": "Siemens",
        "ManufacturersModelName": "Skyra",
        "InstitutionName": "Monash Biomedical Imaging",
        "InstitutionalDepartmentName": "Department",
        "InstitutionAddress": "Blackburn Road 770,Clayton,Victoria,AU,3800",
        "DeviceSerialNumber": "45193",
        "StationName": "AWP45193",
        "BodyPartExamined": "BRAIN",
        "PatientPosition": "HFS",
        "ProcedureStepDescription": "MR Scan",
        "SoftwareVersions": "syngo MR D13C",
        "MRAcquisitionType": "3D",
        "SeriesDescription": "t2_spc_da-fl_sag_p2_iso_1.0",
        "ProtocolName": "t2_spc_da-fl_sag_p2_iso_1.0",
        "ScanningSequence": "SE\\IR",
        "SequenceVariant": "SK\\SP\\MP",
        "ScanOptions": "IR\\PFP",
        "SequenceName": "*spcir_284ns",
        "ImageType": ["ORIGINAL", "PRIMARY", "M", "ND", "NORM"],
        "SeriesNumber": 5,
        "AcquisitionTime": "15:25:1.425000",
        "AcquisitionNumber": 1,
        "SliceThickness": 1,
        "SAR": 0.184773,
        "EchoTime": 0.397,
        "RepetitionTime": 5,
        "InversionTime": 1.8,
        "FlipAngle": 120,
        "PartialFourier": 1,
        "BaseResolution": 256,
        "ShimSetting": [-4122, 15404, -15151, 308, 209, -107, 427, 150],
        "TxRefAmp": 374.478,
        "PhaseResolution": 1,
        "ReceiveCoilName": "Head_32",
        "ReceiveCoilActiveElements": "HEA;HEP",
        "PulseSequenceDetails": "%SiemensSeq%\\tse_vfl",
        "RefLinesPE": 32,
        "ConsistencyInfo": "N4_VD13C_LATEST_20121124",
        "PercentPhaseFOV": 93.75,
        "PercentSampling": 100,
        "EchoTrainLength": 258,
        "PhaseEncodingSteps": 199,
        "AcquisitionMatrixPE": 240,
        "ReconMatrixPE": 240,
        "ParallelReductionFactorInPlane": 2,
        "PixelBandwidth": 780,
        "DwellTime": 2.5e-06,
        "PhaseEncodingDirection": "i",
        "ImageOrientationPatientDICOM": [
            -0.012217,
            0.999925,
            4.08204e-08,
            -0.029664,
            -0.00036239,
            -0.99956,
        ],
        "InPlanePhaseEncodingDirectionDICOM": "ROW",
        "ConversionSoftware": "dcm2niix",
        "ConversionSoftwareVersion": "v1.0.20201102",
    },
    "anat/T2w_mask.nii.gz": None,
    "dwi/dwi.nii.gz": None,
    "dwi/dwi.json": {
        "Modality": "MR",
        "MagneticFieldStrength": 3,
        "ImagingFrequency": 123.252,
        "Manufacturer": "Siemens",
        "ManufacturersModelName": "Skyra",
        "InstitutionName": "Monash Biomedical Imaging",
        "InstitutionalDepartmentName": "Department",
        "InstitutionAddress": "Blackburn Road 770,Clayton,Victoria,AU,3800",
        "DeviceSerialNumber": "45193",
        "StationName": "AWP45193",
        "BodyPartExamined": "BRAIN",
        "PatientPosition": "HFS",
        "ProcedureStepDescription": "MR Scan",
        "SoftwareVersions": "syngo MR D13C",
        "MRAcquisitionType": "2D",
        "SeriesDescription": "R-L MRtrix 60 directions interleaved B0 ep2d_diff_p2",
        "ProtocolName": "R-L MRtrix 60 directions interleaved B0 ep2d_diff_p2",
        "ScanningSequence": "EP",
        "SequenceVariant": "SK\\SP",
        "ScanOptions": "PFP\\FS",
        "SequenceName": "*ep_b0",
        "ImageType": ["ORIGINAL", "PRIMARY", "DIFFUSION", "NONE", "ND", "NORM"],
        "SeriesNumber": 13,
        "AcquisitionTime": "15:40:9.337500",
        "AcquisitionNumber": 1,
        "SliceThickness": 2.5,
        "SpacingBetweenSlices": 2.5,
        "SAR": 0.321094,
        "EchoTime": 0.11,
        "RepetitionTime": 8.8,
        "FlipAngle": 90,
        "PartialFourier": 0.75,
        "BaseResolution": 96,
        "ShimSetting": [-4121, 15424, -15122, 361, 212, 42, 373, 160],
        "DiffusionScheme": "Bipolar",
        "TxRefAmp": 374.478,
        "PhaseResolution": 1,
        "ReceiveCoilName": "Head_32",
        "ReceiveCoilActiveElements": "HEA;HEP",
        "PulseSequenceDetails": "%SiemensSeq%\\ep2d_diff",
        "RefLinesPE": 24,
        "ConsistencyInfo": "N4_VD13C_LATEST_20121124",
        "PercentPhaseFOV": 100,
        "PercentSampling": 100,
        "EchoTrainLength": 36,
        "PhaseEncodingSteps": 72,
        "AcquisitionMatrixPE": 96,
        "ReconMatrixPE": 96,
        "BandwidthPerPixelPhaseEncode": 34.153,
        "ParallelReductionFactorInPlane": 2,
        "EffectiveEchoSpacing": 0.000305,
        "DerivedVendorReportedEchoSpacing": 0.00061,
        "TotalReadoutTime": 0.028975,
        "PixelBandwidth": 2365,
        "DwellTime": 2.2e-06,
        "PhaseEncodingDirection": "i",
        "SliceTiming": [
            4.39,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
            -0.001,
        ],
        "ImageOrientationPatientDICOM": [1, 0, 0, 0, 0.995884, -0.0906326],
        "InPlanePhaseEncodingDirectionDICOM": "ROW",
        "ConversionSoftware": "dcm2niix",
        "ConversionSoftwareVersion": "v1.0.20201102",
    },
    "dwi/dwi.bvec": """0 0.00201544 -0.999916 0.940802 -0.267573 -0.341263 -0.304651 -0.798158 0.911671 -0.332582 0.129953 0 -0.0184011 -0.949467 0.478181 -0.680372 0.0631794 0.905646 0.538287 -0.94236 -0.622045 0.418813 0 0.283531 0.707108 -0.532643 0.574192 0.118616 0.561832 0.167948 -0.795972 0.786687 -0.208195 0 0.600265 -0.837063 -0.16334 -0.0164679 -0.516504 0.318434 0.552268 -0.388239 0.740417 -0.342968 0 0.735306 -0.59768 -0.878689 -0.79987 0.48572 -0.375167 0.323404 0.0328518 -0.221148 -0.0564251 0 0.282136 -0.0542923 0.654414 0.355357 0.800053 0.27937 -0.948506 0.572595 0.715205 -0.819106 0
0 -0.0906324 0.00150768 0.103725 0.566424 -0.932798 0.263583 -0.359376 0.383683 -0.0678173 -0.963974 0 -0.997849 -0.139095 0.789375 -0.642742 0.546254 -0.224242 -0.806156 0.331059 -0.247675 -0.512537 0 -0.823189 -0.706887 -0.556777 -0.156167 -0.667398 0.535909 -0.927327 0.292595 0.308536 -0.955598 0 0.176478 0.505578 -0.903742 -0.415211 -0.84375 -0.947182 -0.402461 -0.80222 0.671615 -0.389569 0 0.581373 0.080015 -0.445495 0.529249 -0.844261 0.75936 0.0700211 0.241606 -0.666627 -0.865575 0 -0.260901 0.780566 -0.589582 0.390048 -0.0200536 0.712645 0.193044 -0.657209 -0.372654 -0.0372004 0
0 -0.995882 0.0128958 -0.322697 -0.779467 0.115876 -0.915266 -0.483521 -0.147117 -0.940633 -0.232091 0 0.0629249 -0.281363 -0.385005 -0.352104 -0.835233 -0.35989 0.245682 0.0485524 -0.742777 -0.749601 0 -0.491905 -0.0175686 -0.63741 -0.803688 -0.735194 -0.630195 0.334452 -0.529921 -0.534723 -0.208535 0 -0.780088 -0.209086 0.395689 -0.909576 -0.145981 0.0380189 0.730086 -0.453557 -0.0267631 -0.854757 0 -0.348326 -0.797732 -0.171583 0.283026 -0.226493 -0.531623 -0.943667 -0.969818 -0.71183 -0.49759 0 -0.923217 -0.622711 0.473429 -0.849461 -0.599594 -0.643498 -0.251139 -0.490114 -0.59128 -0.572434 0""",
    "dwi/dwi.bval": "0 3000 3000 3000 3000 3000 3000 3000 3000 3000 3000 0 3000 3000 3000 3000 3000 3000 3000 3000 3000 3000 0 3000 3000 3000 3000 3000 3000 3000 3000 3000 3000 0 3000 3000 3000 3000 3000 3000 3000 3000 3000 3000 0 3000 3000 3000 3000 3000 3000 3000 3000 3000 3000 0 3000 3000 3000 3000 3000 3000 3000 3000 3000 3000 0",
    "func/bold.nii.gz": None,
    "func/bold.json": {
        "Modality": "MR",
        "MagneticFieldStrength": 3,
        "ImagingFrequency": 123.252,
        "Manufacturer": "Siemens",
        "ManufacturersModelName": "Skyra",
        "InstitutionName": "Monash Biomedical Imaging",
        "InstitutionAddress": "Blackburn Road 770,Clayton,Victoria,AU,3800",
        "DeviceSerialNumber": "45193",
        "StationName": "AWP45193",
        "BodyPartExamined": "BRAIN",
        "PatientPosition": "HFS",
        "ProcedureStepDescription": "MR Scan",
        "SoftwareVersions": "syngo MR D13C",
        "MRAcquisitionType": "2D",
        "SeriesDescription": "REST_cmrr_mbep2d_bold_mat64_32Ch",
        "ProtocolName": "REST_cmrr_mbep2d_bold_mat64_32Ch",
        "ScanningSequence": "EP",
        "SequenceVariant": "SK\\SS",
        "ScanOptions": "FS",
        "ImageType": ["ORIGINAL", "PRIMARY", "M", "MB", "ND", "NORM", "MOSAIC"],
        "SeriesNumber": 3,
        "AcquisitionTime": "15:13:52.795000",
        "AcquisitionNumber": 1,
        "SliceThickness": 3,
        "SpacingBetweenSlices": 3,
        "EchoTime": 0.021,
        "RepetitionTime": 0.801,
        "FlipAngle": 50,
        "ReceiveCoilActiveElements": "HEA;HEP",
        "CoilString": "Head_32",
        "PartialFourier": 0.96875,
        "PercentPhaseFOV": 100,
        "PercentSampling": 100,
        "EchoTrainLength": 31,
        "AcquisitionMatrixPE": 64,
        "ReconMatrixPE": 64,
        "BandwidthPerPixelPhaseEncode": 41.118,
        "ParallelReductionFactorInPlane": 2,
        "EffectiveEchoSpacing": 0.000380004,
        "DerivedVendorReportedEchoSpacing": 0.000760008,
        "TotalReadoutTime": 0.0239402,
        "PixelBandwidth": 1475,
        "PhaseEncodingDirection": "i",
        "SliceTiming": [
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
            10.4125,
        ],
        "ImageOrientationPatientDICOM": [
            1,
            -2.03527e-10,
            2.5383e-11,
            2.05103e-10,
            0.992315,
            -0.12374,
        ],
        "InPlanePhaseEncodingDirectionDICOM": "ROW",
        "ConversionSoftware": "dcm2niix",
        "ConversionSoftwareVersion": "v1.0.20201102",
    },
    "func/bold_ref.nii.gz": None,
}
