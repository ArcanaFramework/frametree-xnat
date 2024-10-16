"""
Helper functions for generating XNAT Container Service compatible Docker
containers
"""

import os
import re
import logging
from pathlib import Path
import attrs
from fileformats.core import FileSet
from frametree.core.utils import path2label
from frametree.common import Clinical
from frametree.core.axes import Axes
from frametree.core.row import DataRow
from frametree.core.entry import DataEntry
from frametree.core.exceptions import FrameTreeNoDirectXnatMountException
from .api import Xnat

logger = logging.getLogger("frametree")


@attrs.define
class XnatViaCS(Xnat):
    """
    Access class for XNAT repositories via the XNAT container service plugin.
    The container service allows the exposure of the underlying file system
    where imaging data can be accessed directly (for performance), and outputs

    Parameters
    ----------
    server : str (URI)
        URI of XNAT server to connect to
    project_id : str
        The ID of the project in the XNAT repository
    cache_dir : str (name_path)
        Path to local directory to cache remote data in
    user : str
        Username with which to connect to XNAT with
    password : str
        Password to connect to the XNAT repository with
    check_md5 : bool
        Whether to check the MD5 digest of cached files before using. This
        checks for updates on the server since the file was cached
    race_cond_delay : int
        The amount of time to wait before checking that the required
        fileset has been downloaded to cache by another process has
        completed if they are attempting to download the same fileset
    row_frequency: Axes
        the frequency of the row the pipeline is executed against
    row_id : str
        the ID of the row
    input_mount : Path
        the file-system path the inputs are mounted at
    output_mount : Path
        the file-system mount the outputs are to be stored in
    server : str
        the URI of the server
    user : str
        the username of the user
    password : str
        the password of the user
    cache_dir: Path
        the path to the cache dir to download any files that aren't on the input mount
    internal_upload : bool, optional
        whether to use XNAT CS's built-in output uploader or use the more flexible API
    """

    INPUT_MOUNT = Path("/input")
    OUTPUT_MOUNT = Path("/output")
    WORK_MOUNT = Path("/work")
    CACHE_DIR = Path("/cache")

    row_frequency: Axes = attrs.field(default=Clinical.session)
    row_id: str = attrs.field(default=None)
    input_mount: Path = attrs.field(default=INPUT_MOUNT, converter=Path)
    output_mount: Path = attrs.field(default=OUTPUT_MOUNT, converter=Path)
    server: str = attrs.field()
    user: str = attrs.field()
    password: str = attrs.field()
    cache_dir: Path = attrs.field(default=CACHE_DIR, converter=Path)
    internal_upload: bool = attrs.field(default=False)

    alias = "xnat_via_cs"

    @server.default
    def server_default(self) -> str:
        server = os.environ["XNAT_HOST"]
        logger.debug("XNAT (via CS) server found %s", server)
        return server

    @user.default
    def user_default(self) -> str:
        return os.environ["XNAT_USER"]

    @password.default
    def password_default(self) -> str:
        return os.environ["XNAT_PASS"]

    def get_fileset(self, entry: DataEntry, datatype: type) -> FileSet:
        """Attempt to get fileset directly from the input mount, falling back to API
        access if that fails"""
        try:
            input_mount = self.get_input_mount(entry.row)
        except FrameTreeNoDirectXnatMountException:
            # Fallback to API access
            return super().get_fileset(entry, datatype)  # type: ignore[no-any-return]
        logger.info(
            "Getting %s from %s:%s row via direct access to archive directory",
            entry.path,
            entry.row.frequency,
            entry.row.id,
        )
        if entry.is_derivative and self.internal_upload:
            # entry is in input mount
            resource_path = self.output_mount_fspath(entry)
            fspaths = [
                p
                for p in resource_path.parent.iterdir()
                if re.match("^" + resource_path.name + r"\b", p.name)
            ]
        else:
            match = re.match(
                r"/data/(?:archive/)?projects/[a-zA-Z0-9\-_]+/"
                r"(?:subjects/[a-zA-Z0-9\-_]+/)?"
                r"(?:experiments/[a-zA-Z0-9\-_]+/)?(?P<path>.*)$",
                entry.uri,
            )
            if match is None:
                raise ValueError(f"Invalid URI in {self}: {entry.uri}")
            path = match.group("path")
            if "scans" in path:
                path = path.replace("scans", "SCANS").replace("resources/", "")
            resource_path = input_mount / path
            if not resource_path.exists():
                resource_path = input_mount / path.replace("resources", "RESOURCES")
            assert (
                resource_path.exists()
            ), f"Resource path {path} not found in {input_mount}: {list(input_mount.iterdir())}"  # noqa
            fspaths = [
                p
                for p in resource_path.iterdir()
                if not p.name.endswith("_catalog.xml")
            ]
        return datatype(fspaths)  # type: ignore[no-any-return]

    def put_fileset(self, fileset: FileSet, entry: DataEntry) -> FileSet:
        if not (self.internal_upload and entry.is_derivative):
            return super().put_fileset(fileset, entry)  # type: ignore[no-any-return]
        cached = fileset.copy(
            dest_dir=self.output_mount,
            make_dirs=True,
            new_stem=entry.path.split("/")[-1].split("@")[0],
            trim=False,
            overwrite=True,
        )
        logger.info(
            "Put %s into %s:%s row via direct access to archive directory",
            entry.path,
            entry.row.frequency,
            entry.row.id,
        )
        return cached

    def post_fileset(
        self, fileset: FileSet, path: str, datatype: type, row: DataRow
    ) -> DataEntry:
        uri = self._make_uri(row) + "/RESOURCES/" + path
        entry = row.add_entry(path=path, datatype=datatype, uri=uri)
        self.put_fileset(fileset, entry)
        return entry

    def output_mount_fspath(self, entry: DataEntry) -> Path:
        """Determine the paths that derivatives will be saved at"""
        assert entry.is_derivative
        path_parts = entry.path.split("/")
        # Escape resource name
        path_parts[-1] = path2label(path_parts[-1])
        return self.output_mount.joinpath(*path_parts)

    def get_input_mount(self, row: DataRow) -> Path:
        if self.row_frequency == row.frequency:
            return self.input_mount
        elif (
            self.row_frequency == Clinical.constant
            and row.frequency == Clinical.session
        ):
            arc_dirs = [
                d
                for d in self.input_mount.iterdir()
                if d.is_dir() and d.name.startswith("arc")
            ]
            for arc_dir in arc_dirs:
                session_dir: Path = arc_dir / row.id
                if session_dir.exists():
                    return session_dir
            raise FrameTreeNoDirectXnatMountException(
                f"No direct mount found for {row.frequency} {row.id} found arc dirs {arc_dirs}"
            )
        else:
            raise FrameTreeNoDirectXnatMountException

    def _make_uri(self, row: DataRow) -> str:
        uri: str = "/data/archive/projects/" + row.frameset.id
        if row.frequency == Clinical.session:
            uri += "/experiments/" + row.id
        elif row.frequency == Clinical.subject:
            uri += "/subjects/" + row.id
        elif row.frequency != Clinical.constant:
            uri += "/subjects/" + self.make_row_name(row)
        return uri


# def get_existing_docker_tags(docker_registry, docker_org, image_name):
#     result = requests.get(
#         f'https://{docker_registry}/v2/repositories/{docker_org}/{image_name}/tags')
#     return [r['name'] for r in result.json()]
