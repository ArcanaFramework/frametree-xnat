import typing as ty
import time
from pathlib import Path
import tempfile
import attrs
import xnat
from arcana.core.data.space import Clinical
from arcana.core.data.space import DataSpace
from arcana.core.data.row import DataRow
from arcana.testing.data.blueprint import TestDatasetBlueprint, FileSetEntryBlueprint
from arcana.core.exceptions import ArcanaError


@attrs.define
class ScanBlueprint:

    name: str
    resources: ty.List[FileSetEntryBlueprint]


@attrs.define(slots=False, kw_only=True)
class TestXnatDatasetBlueprint(TestDatasetBlueprint):

    scans: ty.List[ScanBlueprint]

    # Overwrite attributes in core blueprint class
    space: type = Clinical
    hierarchy: list[DataSpace] = ["subject", "session"]
    filesets: list[str] = None

    def make_entries(self, row: DataRow, source_data: Path = None):
        xrow = row.dataset.store.get_xrow(row)
        xclasses = xrow.xnat_session.classes
        for scan_id, scan_bp in enumerate(self.scans, start=1):
            xscan = xclasses.MrScanData(
                id=scan_id, type=scan_bp.name, parent=xrow
            )
            for resource_bp in scan_bp.resources:
                tmp_dir = Path(tempfile.mkdtemp())
                # Create the resource
                xresource = xscan.create_resource(resource_bp.path)
                # Create the dummy files
                item = resource_bp.make_item(
                    source_data=source_data,
                    source_fallback=True,
                    escape_source_name=False,
                )
                item.copy_to(tmp_dir)
                xresource.upload_dir(tmp_dir)


def install_and_launch_xnat_cs_command(
    command_json: dict,
    project_id: str,
    session_id: str,
    inputs: ty.Dict[str, str],
    xlogin: xnat.XNATSession,
    timeout: int = 1000,  # seconds
    poll_interval: int = 10,  # seconds
):
    """Installs a new command for the XNAT container service and lanches it on
    the specified session.

    Parameters
    ----------
    cmd_name : str
        The name to install the command as
    command_json : dict[str, Any]
        JSON that defines the XNAT command in the container service (see `generate_xnat_command`)
    project_id : str
        ID of the project to enable the command for
    session_id : str
        ID of the session to launch the command on
    inputs : dict[str, str]
        Inputs passed to the pipeline at launch (i.e. typically through text fields in the CS launch UI)
    xlogin : xnat.XNATSession
        XnatPy connection to the XNAT server
    timeout : int
        the time to wait for the pipeline to complete (seconds)
    poll_interval : int
        the time interval between status polls (seconds)

    Returns
    -------
    workflow_id : int
        the auto-generated ID for the launched workflow
    status : str
        the status of the completed workflow
    out_str : str
        stdout and stderr from the workflow run
    """

    cmd_name = command_json["name"]
    wrapper_name = command_json["xnat"][0]["name"]
    cmd_id = xlogin.post("/xapi/commands", json=command_json).json()

    # Enable the command globally and in the project
    xlogin.put(f"/xapi/commands/{cmd_id}/wrappers/{wrapper_name}/enabled")
    xlogin.put(
        f"/xapi/projects/{project_id}/commands/{cmd_id}/wrappers/{wrapper_name}/enabled"
    )

    launch_json = {"SESSION": f"/archive/experiments/{session_id}"}

    launch_json.update(inputs)

    launch_result = xlogin.post(
        f"/xapi/projects/{project_id}/wrappers/{cmd_id}/root/SESSION/launch",
        json=launch_json,
    ).json()

    if launch_result["status"] != "success":
        raise ArcanaError(
            f"{cmd_name} workflow wasn't launched successfully ({launch_result['status']})"
        )
    workflow_id = launch_result["workflow-id"]
    assert workflow_id != "To be assigned"

    num_attempts = (timeout // poll_interval) + 1
    max_runtime = num_attempts * poll_interval

    for i in range(num_attempts):
        wf_result = xlogin.get(f"/xapi/workflows/{workflow_id}").json()
        if wf_result["status"] not in INCOMPLETE_CS_STATES:
            break
        time.sleep(poll_interval)

    launch_status = wf_result["status"]
    if launch_status == "success":
        raise ValueError(
            f"Launching {cmd_name} in the XNAT CS failed with status {launch_status} "
            f"for inputs: \n{launch_json}"
        )

    container_id = wf_result["comments"]

    # Get workflow stdout/stderr for error messages if required
    out_str = ""
    stdout_result = xlogin.get(
        f"/xapi/containers/{container_id}/logs/stdout", accepted_status=[200, 204]
    )
    if stdout_result.status_code == 200:
        out_str = f"stdout:\n{stdout_result.content.decode('utf-8')}\n"

    stderr_result = xlogin.get(
        f"/xapi/containers/{container_id}/logs/stderr", accepted_status=[200, 204]
    )
    if stderr_result.status_code == 200:
        out_str += f"\nstderr:\n{stderr_result.content.decode('utf-8')}"

    if i == num_attempts - 1:
        status = f"NotCompletedAfter{max_runtime}Seconds"
    else:
        status = wf_result["status"]

    return workflow_id, status, out_str


# List of intermediatary states can pass through
# before completing successfully
INCOMPLETE_CS_STATES = (
    "Pending",
    "Running",
    "_Queued",
    "Staging",
    "Finalizing",
    "Created",
    "_die",
    "die",
)
