"""
Helper functions for generating XNAT Container Service compatible Docker
containers
"""
import os
import re
import logging
import typing as ty
from pathlib import Path
import shutil
import attrs
from arcana.core.data import Clinical
from arcana.core.data.space import DataSpace
from fileformats.core.base import FileSet
from arcana.core.exceptions import ArcanaNoDirectXnatMountException
from .api import Xnat

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

    def get_fileset_paths(self, fileset: FileSet) -> ty.List[Path]:
        try:
            input_mount = self.get_input_mount(fileset)
        except ArcanaNoDirectXnatMountException:
            # Fallback to API access
            return super().get_fileset_paths(fileset)
        logger.info(
            "Getting %s from %s:%s row via direct access to archive directory",
            fileset.path,
            fileset.row.frequency,
            fileset.row.id,
        )
        if fileset.uri:
            path = re.match(
                r"/data/(?:archive/)?projects/[a-zA-Z0-9\-_]+/"
                r"(?:subjects/[a-zA-Z0-9\-_]+/)?"
                r"(?:experiments/[a-zA-Z0-9\-_]+/)?(?P<path>.*)$",
                fileset.uri,
            ).group("path")
            if "scans" in path:
                path = path.replace("scans", "SCANS").replace("resources/", "")
            path = path.replace("resources", "RESOURCES")
            resource_path = input_mount / path
            if fileset.is_dir:
                # Link files from resource dir into temp dir to avoid catalog XML
                dir_path = self.cache_path(fileset)
                try:
                    shutil.rmtree(dir_path)
                except FileNotFoundError:
                    pass
                os.makedirs(dir_path, exist_ok=True)
                for item in resource_path.iterdir():
                    if not item.name.endswith("_catalog.xml"):
                        os.symlink(item, dir_path / item.name)
                fspaths = [dir_path]
            else:
                fspaths = list(resource_path.iterdir())
        else:
            logger.debug(
                "No URI set for fileset %s, assuming it is a newly created "
                "derivative on the output mount",
                fileset,
            )
            stem_path = self.fileset_stem_path(fileset)
            if fileset.is_dir:
                fspaths = [stem_path]
            else:
                fspaths = list(stem_path.iterdir())
        return fspaths

    def put_fileset_paths(
        self, fileset: FileSet, fspaths: ty.List[Path]
    ) -> ty.List[Path]:
        stem_path = self.fileset_stem_path(fileset)
        os.makedirs(stem_path.parent, exist_ok=True)
        cache_paths = []
        for fspath in fspaths:
            if fileset.is_dir:
                target_path = stem_path
                shutil.copytree(fspath, target_path)
            else:
                target_path = fileset.copy_ext(fspath, stem_path)
                # Upload primary file and add to cache
                shutil.copyfile(fspath, target_path)
            cache_paths.append(target_path)
        # Update file-set with new values for local paths and XNAT URI
        fileset.uri = (
            self._make_uri(fileset.row) + "/RESOURCES/" + fileset.path
        )
        logger.info(
            "Put %s into %s:%s row via direct access to archive directory",
            fileset.path,
            fileset.row.frequency,
            fileset.row.id,
        )
        return cache_paths

    def fileset_stem_path(self, fileset):
        """Determine the paths that derivatives will be saved at"""
        return self.output_mount.joinpath(*fileset.path.split("/"))

    def get_input_mount(self, fileset):
        row = fileset.row
        if self.row_frequency == row.frequency:
            return self.input_mount
        elif (
            self.row_frequency == Clinical.dataset and row.frequency == Clinical.session
        ):
            return self.input_mount / row.id
        else:
            raise ArcanaNoDirectXnatMountException


# def get_existing_docker_tags(docker_registry, docker_org, image_name):
#     result = requests.get(
#         f'https://{docker_registry}/v2/repositories/{docker_org}/{image_name}/tags')
#     return [r['name'] for r in result.json()]
