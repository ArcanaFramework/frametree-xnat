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
from arcana.common import Clinical
from arcana.core.data.space import DataSpace
from arcana.core.data.row import DataRow
from arcana.core.data.entry import DataEntry
from arcana.core.exceptions import ArcanaNoDirectXnatMountException
from .api import Xnat, path2label

logger = logging.getLogger("arcana")


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
    """

    INPUT_MOUNT = Path("/input")
    OUTPUT_MOUNT = Path("/output")
    WORK_MOUNT = Path("/work")
    CACHE_DIR = Path("/cache")

    row_frequency: DataSpace = attrs.field(default=Clinical.session)
    row_id: str = attrs.field(default=None)
    input_mount: Path = attrs.field(default=INPUT_MOUNT, converter=Path)
    output_mount: Path = attrs.field(default=OUTPUT_MOUNT, converter=Path)
    server: str = attrs.field()
    user: str = attrs.field()
    password: str = attrs.field()
    cache_dir: str = attrs.field(default=CACHE_DIR, converter=Path)

    alias = "xnat_via_cs"

    @server.default
    def server_default(self):
        server = os.environ["XNAT_HOST"]
        logger.debug("XNAT (via CS) server found %s", server)
        return server

    @user.default
    def user_default(self):
        return os.environ["XNAT_USER"]

    @password.default
    def password_default(self):
        return os.environ["XNAT_PASS"]

    def get_fileset(self, entry: DataEntry, datatype: type) -> FileSet:
        """Attempt to get fileset directly from the input mount, falling back to API
        access if that fails"""
        try:
            input_mount = self.get_input_mount(entry.row)
        except ArcanaNoDirectXnatMountException:
            # Fallback to API access
            return super().get_fileset(entry, datatype)
        logger.info(
            "Getting %s from %s:%s row via direct access to archive directory",
            entry.path,
            entry.row.frequency,
            entry.row.id,
        )
        if entry.is_derivative:
            # entry is in input mount
            resource_path = self.entry_fspath(entry)
        else:
            path = re.match(
                r"/data/(?:archive/)?projects/[a-zA-Z0-9\-_]+/"
                r"(?:subjects/[a-zA-Z0-9\-_]+/)?"
                r"(?:experiments/[a-zA-Z0-9\-_]+/)?(?P<path>.*)$",
                entry.uri,
            ).group("path")
            if "scans" in path:
                path = path.replace("scans", "SCANS").replace("resources/", "")
            path = path.replace("resources", "RESOURCES")
            resource_path = input_mount / path
        fspaths = list(p for p in resource_path.iterdir() if not p.name.endswith("_catalog.xml"))
        return datatype(fspaths)

    def put_fileset(self, fileset: FileSet, entry: DataEntry) -> FileSet:
        if not entry.is_derivative:
            super().put_fileset(fileset, entry)  # Fallback to API access
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
        self, fileset, path: str, datatype: type, row: DataRow
    ) -> DataEntry:
        uri = self._make_uri(row) + "/RESOURCES/" + path
        entry = row.add_entry(path=path, datatype=datatype, uri=uri)
        self.put_fileset(fileset, entry)
        return entry

    def entry_fspath(self, entry: DataEntry) -> Path:
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
            self.row_frequency == Clinical.dataset and row.frequency == Clinical.session
        ):
            return self.input_mount / row.id
        else:
            raise ArcanaNoDirectXnatMountException

    def _make_uri(self, row: DataRow):
        uri = "/data/archive/projects/" + row.dataset.id
        if row.frequency == Clinical.session:
            uri += "/experiments/" + row.id
        elif row.frequency == Clinical.subject:
            uri += "/subjects/" + row.id
        elif row.frequency != Clinical.dataset:
            uri += "/subjects/" + self.make_row_name(row)
        return uri


# def get_existing_docker_tags(docker_registry, docker_org, image_name):
#     result = requests.get(
#         f'https://{docker_registry}/v2/repositories/{docker_org}/{image_name}/tags')
#     return [r['name'] for r in result.json()]
