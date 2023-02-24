import os
import os.path as op
import hashlib
from pathlib import Path
import typing as ty
from glob import glob
import time
import tempfile
import logging
import errno
from itertools import product
import json
import re
from zipfile import ZipFile, BadZipfile
import shutil
import attrs
import xnat.session
from fileformats.core import FileSet, Field
from fileformats.generic import BaseDirectory
from fileformats.medimage import Dicom
from fileformats.core.exceptions import FormatRecognitionError
from arcana.core.utils.misc import (
    dir_modtime,
    JSON_ENCODING,
    path2varname,
    varname2path,
)
from arcana.core.data.store import (
    DataStore,
)
from arcana.core.data.testing import TestDatasetBlueprint
from arcana.core.data.row import DataRow
from arcana.core.exceptions import (
    ArcanaError,
    ArcanaUsageError,
    ArcanaWrongRepositoryError,
    DatatypeUnsupportedByStoreError,
)
from fileformats.core import DataType
from arcana.core.utils.serialize import asdict
from arcana.core.utils.misc import dict_diff
from arcana.core.data.set import Dataset
from arcana.core.data.tree import DataTree
from arcana.core.data.entry import DataEntry
from arcana.core.data import Clinical
from .testing import ScanBlueprint


logger = logging.getLogger("arcana")

special_char_re = re.compile(r"[^a-zA-Z_0-9]")
tag_parse_re = re.compile(r"\((\d+),(\d+)\)")

RELEVANT_DICOM_TAG_TYPES = set(("UI", "CS", "DA", "TM", "SH", "LO", "PN", "ST", "AS"))

# COMMAND_INPUT_TYPES = {bool: "bool", str: "string", int: "number", float: "number"}


@attrs.define
class Xnat(DataStore):
    """
    Access class for XNAT data repositories

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
    race_condition_delay : int
        The amount of time to wait before checking that the required
        fileset has been downloaded to cache by another process has
        completed if they are attempting to download the same fileset
    """

    server: str = attrs.field()
    cache_dir: str = attrs.field(converter=Path)
    name: str = None
    user: str = attrs.field(default=None, metadata={"asdict": False})
    password: str = attrs.field(default=None, metadata={"asdict": False})
    race_condition_delay: int = attrs.field(default=30)
    _cached_datasets: ty.Dict[str, Dataset] = attrs.field(factory=dict, init=False)
    _login: xnat.session.XNATSession = attrs.field(default=None, init=False)

    alias = "xnat"
    CHECKSUM_SUFFIX = ".md5.json"
    PROV_SUFFIX = ".__prov__.json"
    FIELD_PROV_RESOURCE = "__provenance__"
    depth = 2
    DEFAULT_SPACE = Clinical
    DEFAULT_HIERARCHY = ["subject", "timepoint"]
    METADATA_RESOURCE = "__arcana__"
    LICENSE_RESOURCE = "LICENSES"

    @cache_dir.validator
    def cache_dir_validator(self, _, cache_dir):
        if not cache_dir.exists():
            raise ValueError(f"Cache dir, '{cache_dir}' does not exist")

    def get(self, entry: DataEntry, datatype: type) -> DataType:
        if entry.datatype.is_fileset:
            item = self.get_fileset(entry, datatype)
        elif entry.datatype.is_field:
            item = self.get_field(entry, datatype)
        else:
            raise DatatypeUnsupportedByStoreError(entry.datatype, self)
        assert isinstance(item, datatype)
        return item

    def put(self, item: DataType, entry: DataEntry):
        if entry.datatype.is_fileset:
            item = self.put_fileset(item, entry)
        elif entry.datatype.is_field:
            item = self.put_field(item, entry)
        else:
            raise DatatypeUnsupportedByStoreError(entry.datatype, self)
        return item

    def post(self, item: DataType, path: str, datatype: type, row: DataRow):
        if datatype.is_fileset:
            entry = self.post_fileset(item, path=path, datatype=datatype, row=row)
        elif datatype.is_field:
            entry = self.post_field(item, path=path, datatype=datatype, row=row)
        else:
            raise DatatypeUnsupportedByStoreError(datatype, self)
        return entry

    def populate_tree(self, tree: DataTree):
        """
        Find all filesets, fields and provenance provenances within an XNAT
        project and create data tree within dataset

        Parameters
        ----------
        dataset : Dataset
            The dataset to construct
        """
        with self.connection:
            # Get all "leaf" nodes, i.e. XNAT imaging session objects
            for exp in self.connection.projects[tree.dataset_id].experiments.values():
                tree.add_leaf([exp.subject.label, exp.label])

    def populate_row(self, row: DataRow):
        """Find all resource objects at scan and imaging session/subject/project level
        and create corresponding file-set entries, and list all fields"""
        with self.connection:
            xrow = self.get_xrow(row)
            # Add scans, fields and resources to data row
            try:
                xscans = xrow.scans
            except AttributeError:
                pass  # A subject or project row
            else:
                for xscan in xscans.values():
                    for xresource in xscan.resources.values():
                        row.add_entry(
                            path=f"{xscan.type}/{xresource.label}",
                            datatype=FileSet,
                            order=xscan.id,
                            quality=xscan.quality,
                            uri=self._get_resource_uri(xresource),
                        )
            for field_id in xrow.fields:
                row.add_entry(path=varname2path(field_id), datatype=Field, uri=None)
            for xresource in xrow.resources.values():
                uri = self._get_resource_uri(xresource)
                try:
                    datatype = FileSet.from_mime(xresource.format)
                except FormatRecognitionError:
                    datatype = FileSet
                if xresource.label in ("DICOM", "secondary"):
                    if datatype is FileSet:
                        datatype = Dicom
                    item_metadata = self.get_dicom_header(uri)
                else:
                    item_metadata = {}
                row.add_entry(
                    path="@" + varname2path(xresource.label),
                    datatype=datatype,
                    uri=uri,
                    item_metadata=item_metadata,
                    checksums=self.get_checksums(uri),
                )

    def get_fileset(self, entry: DataEntry, datatype: type) -> FileSet:
        """
        Caches a fileset to the local file system and returns the path to
        the cached files

        Parameters
        ----------
        entry: DataEntry
            The entry to retrieve the file-set for
        datatype: type
            the datatype to return the item as

        Returns
        -------
        FileSet
            the cached file-set
        """
        logger.info(
            "Getting %s from %s:%s row via API access",
            entry.path,
            entry.row.frequency,
            entry.row.id,
        )
        cache_path = self.cache_path(entry.uri)
        need_to_download = True
        if op.exists(cache_path):
            md5_path = append_suffix(cache_path, self.CHECKSUM_SUFFIX)
            if md5_path.exists():
                with open(md5_path, "r") as f:
                    cached_checksums = json.load(f)
            if cached_checksums == entry.checksums:
                need_to_download = False
        if need_to_download:
            with self.connection:
                tmp_download_dir = append_suffix(cache_path, ".download")
                try:
                    os.makedirs(tmp_download_dir)
                except OSError as e:
                    if e.errno == errno.EEXIST:
                        # Attempt to make tmp download directory. This will
                        # fail if another process (or previous attempt) has
                        # already created it. In that case this process will
                        # wait 'race_cond_delay' seconds to see if it has been
                        # updated (i.e. is being downloaded by the other process)
                        # and otherwise assume that it was interrupted and redownload.
                        self._delayed_download(
                            entry,
                            tmp_download_dir,
                            cache_path,
                            delay=self._race_cond_delay,
                        )
                    else:
                        raise
                else:
                    self.download_fileset(entry, tmp_download_dir, cache_path)
                    shutil.rmtree(tmp_download_dir)
                # Save checksums for future reference, so we can check to see if cache
                # is stale
                checksums = self.get_checksums(entry.uri)
                with open(
                    str(cache_path) + self.CHECKSUM_SUFFIX, "w", **JSON_ENCODING
                ) as f:
                    json.dump(checksums, f, indent=2)
        if datatype.is_subtype_of(BaseDirectory):
            # Directory file-sets are stored as separate files directly under the
            # resource, so in this case we take the whole cache path to be the fspath
            cache_paths = [cache_path]
        else:
            cache_paths = list(cache_path.iterdir())
        return datatype(cache_paths)

    def put_fileset(self, fileset: FileSet, entry: DataEntry) -> FileSet:
        """
        Stores files for a file set into the XNAT repository

        Parameters
        ----------
        fileset : FileSet
            The file-set to put the paths for
        fspaths: list[Path or str  ]
            The paths of files/directories to put into the XNAT repository

        Returns
        -------
        list[Path]
            The locations of the locally cached paths
        """
        # Open XNAT session
        with self.connection:
            # Get existing session
            xresource = self.connection.classes.Resource(
                uri=entry.uri, xnat_session=self.connection.session
            )
            # Create cache path
            cache_path = self.cache_path(entry.uri)
            if cache_path.exists():
                shutil.rmtree(cache_path)
            # Copy to cache
            if fileset.is_dir:
                # Directories are mapped to the cache directory, not uploaded within it
                # to match how DICOMs are stored on XNAT (not sure this is a good idea)
                cached = fileset.copy_to(
                    cache_path.parent, stem=cache_path.name, make_dirs=True
                )
                xresource.upload_dir(str(cached), overwrite=True)
            else:
                cached = fileset.copy_to(cache_path, make_dirs=True)
                xresource.upload_dir(cache_path, overwrite=True)
            checksums = self.get_checksums(entry.uri)
            calculated_checksums = cached.hash_files(
                crypto=hashlib.md5, relative_to=cache_path
            )
            if checksums != calculated_checksums:
                raise ArcanaError(
                    f"Checksums for uploaded file-set at {entry} don't match that of the original files:\n\n"
                    + dict_diff(
                        calculated_checksums,
                        checksums,
                        label1="original",
                        label2="remote",
                    )
                )
        # Save checksums, to avoid having to redownload if they haven't been altered
        # on XNAT
        with open(
            append_suffix(cache_path, self.CHECKSUM_SUFFIX), "w", **JSON_ENCODING
        ) as f:
            json.dump(checksums, f, indent=2)
        logger.info(
            "Put %s into %s:%s row via API access",
            entry.path,
            entry.row.frequency,
            entry.row.id,
        )
        return cached

    def post_fileset(
        self, fileset: DataType, path: str, datatype: type, row: DataRow
    ) -> DataEntry:
        """
        Creates a new resource entry to store the fileset in then puts it in it

        Parameters
        ----------
        fileset : FileSet
            The file-set to put the paths for
        fspaths: list[Path or str  ]
            The paths of files/directories to put into the XNAT repository

        Returns
        -------
        list[Path]
            The locations of the locally cached paths
        """
        if path.startswith("@"):
            path = path[1:]
        else:
            raise NotImplementedError(
                f"Posting fileset to non-derivative path '{path}' is not currently "
                "supported"
            )
        # Open XNAT connection session
        with self.connection:
            # Create the new resource for the fileset entry
            xresource = self.connection.classes.ResourceCatalog(
                parent=self.get_xrow(row),
                label=path2varname(path),
                format=datatype.mime_like,
            )
            # Add corresponding entry to row
            entry = row.add_entry(
                path=path,
                datatype=datatype,
                uri=self._get_resource_uri(xresource),
            )
            # Put the fileset data into the entry
            self.put_fileset(fileset, entry)
        return entry

    def get_field(self, entry: DataEntry, datatype: type) -> Field:
        """
        Retrieves a fields value

        Parameters
        ----------
        field : Field
            The field to retrieve

        Returns
        -------
        value : ty.Union[float, int, str, ty.List[float], ty.List[int], ty.List[str]]
            The value of the field
        """
        with self.connection:
            xrow = self.get_xrow(entry.row)
            val = xrow.fields[path2varname(entry.path)]
            val = val.replace("&quot;", '"')
        return datatype(val)

    def put_field(self, field: Field, entry: DataEntry):
        """Store the value for a field in the XNAT repository

        Parameters
        ----------
        field : Field
            the field to store the value for
        value : str or float or int or bool
            the value to store
        """
        field = entry.datatype(field)
        with self.connection:
            xrow = self.get_xrow(entry.row)
            xrow.fields[path2varname(entry.path)] = str(field)

    def post_field(
        self, field: Field, path: str, datatype: type, row: DataRow
    ) -> DataEntry:
        entry = row.add_entry(path, datatype, uri=None)
        self.put_field(field, entry)
        return entry

    def get_checksums(self, uri: str):
        """
        Downloads the MD5 digests associated with the files in the file-set.
        These are saved with the downloaded files in the cache and used to
        check if the files have been updated on the server

        Parameters
        ----------
        fileset: FileSet
            the fileset to get the checksums for. Used to
            determine the primary file within the resource and change the
            corresponding key in the checksums dictionary to '.' to match
            the way it is generated locally by Arcana.
        """
        if uri is None:
            raise ArcanaUsageError(
                "Can't retrieve checksums as URI has not been set for {}".format(uri)
            )
        with self.connection:
            checksums = {
                r["URI"]: r["digest"]
                for r in self.connection.get_json(uri + "/files")["ResultSet"]["Result"]
            }
        # strip base URI to get relative paths of files within the resource
        checksums = {
            re.match(r".*/resources/\w+/files/(.*)$", u).group(1): c
            for u, c in sorted(checksums.items())
        }
        # if not self.is_dir:
        #     # Replace the paths of the primary file with primary file with '.'
        #     checksums['.'] = checksums.pop(primary)
        #     for path in set(checksums.keys()) - set(['.']):
        #         ext = '.'.join(Path(path).suffixes)
        #         if ext in checksums:
        #             logger.warning(
        #                 f"Multiple side-cars found in {fileset} XNAT "
        #                 f"resource with the same extension (this isn't "
        #                 f"allowed) and therefore cannot convert {path} to "
        #                 "{ext} in checksums")
        #         else:
        #             checksums[ext] = checksums.pop(path)
        return checksums

    def save_dataset_definition(
        self, dataset_id: str, definition: ty.Dict[str, ty.Any], name: str
    ):
        with self.connection:
            xproject = self.connection.projects[dataset_id]
            try:
                xresource = xproject.resources[self.METADATA_RESOURCE]
            except KeyError:
                # Create the new resource for the fileset
                xresource = self.connection.classes.ResourceCatalog(
                    parent=xproject, label=self.METADATA_RESOURCE, format="json"
                )
            definition_file = Path(tempfile.mkdtemp()) / str(name + ".json")
            with open(definition_file, "w") as f:
                json.dump(definition, f, indent="    ")
            xresource.upload(str(definition_file), name + ".json", overwrite=True)

    def load_dataset_definition(self, dataset_id: str, name: str) -> dict[str, ty.Any]:
        with self.connection:
            xproject = self.connection.projects[dataset_id]
            try:
                xresource = xproject.resources[self.METADATA_RESOURCE]
            except KeyError:
                definition = None
            else:
                download_dir = Path(tempfile.mkdtemp())
                xresource.download_dir(download_dir)
                fpath = (
                    download_dir
                    / dataset_id
                    / "resources"
                    / "__arcana__"
                    / "files"
                    / (name + ".json")
                )
                print(fpath)
                if fpath.exists():
                    with open(fpath) as f:
                        definition = json.load(f)
                else:
                    definition = None
        return definition

    def connect(self) -> xnat.XNATSession:
        """
        Parameters
        ----------
        prev_login : xnat.XNATSession
            An XNAT login that has been opened in the code that calls
            the method that calls login. It is wrapped in a
            NoExitWrapper so the returned connection can be used
            in a "with" statement in the method.
        """
        sess_kwargs = {}
        if self.user is not None:
            sess_kwargs["user"] = self.user
        if self.password is not None:
            sess_kwargs["password"] = self.password
        return xnat.connect(server=self.server, **sess_kwargs)

    def disconnect(self, session: xnat.XNATSession):
        session.disconnect()

    def put_provenance(self, item, provenance: ty.Dict[str, ty.Any]):
        xresource, _, cache_path = self._provenance_location(item, create_resource=True)
        with open(cache_path, "w") as f:
            json.dump(provenance, f, indent="  ")
        xresource.upload(cache_path, cache_path.name)

    def get_provenance(self, item) -> ty.Dict[str, ty.Any]:
        try:
            xresource, uri, cache_path = self._provenance_location(item)
        except KeyError:
            return {}  # Provenance doesn't exist on server
        with open(cache_path, "w") as f:
            xresource.xnat_session.download_stream(uri, f)
            provenance = json.load(f)
        return provenance

    def download_fileset(
        self, entry: DataEntry, tmp_download_dir: Path, target_path: Path
    ):
        with self.connection:
            # Download resource to zip file
            zip_path = op.join(tmp_download_dir, "download.zip")
            with open(zip_path, "wb") as f:
                self.connection.download_stream(
                    entry.uri + "/files", f, format="zip", verbose=True
                )
            # Extract downloaded zip file
            expanded_dir = op.join(tmp_download_dir, "expanded")
            try:
                with ZipFile(zip_path) as zip_file:
                    zip_file.extractall(expanded_dir)
            except BadZipfile as e:
                raise ArcanaError(
                    "Could not unzip file '{}' ({})".format(zip_path, e)
                ) from e
            data_path = glob(expanded_dir + "/**/files", recursive=True)[0]
            # Remove existing cache if present
            try:
                shutil.rmtree(target_path)
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise e
            shutil.move(data_path, target_path)

    def _delayed_download(
        self, entry: DataEntry, tmp_download_dir: Path, target_path: Path, delay: int
    ):
        logger.info(
            "Waiting %s seconds for incomplete download of '%s' "
            "initiated another process to finish",
            delay,
            target_path,
        )
        initial_mod_time = dir_modtime(tmp_download_dir)
        time.sleep(delay)
        if op.exists(target_path):
            logger.info(
                "The download of '%s' has completed "
                "successfully in the other process, continuing",
                target_path,
            )
            return
        elif initial_mod_time != dir_modtime(tmp_download_dir):
            logger.info(
                "The download of '%s' hasn't completed yet, but it has"
                " been updated.  Waiting another %s seconds before "
                "checking again.",
                target_path,
                delay,
            )
            self._delayed_download(entry, tmp_download_dir, target_path, delay)
        else:
            logger.warning(
                "The download of '%s' hasn't updated in %s "
                "seconds, assuming that it was interrupted and "
                "restarting download",
                target_path,
                delay,
            )
            shutil.rmtree(tmp_download_dir)
            os.mkdir(tmp_download_dir)
            self.download_fileset(entry, tmp_download_dir, target_path)

    def get_xrow(self, row: DataRow):
        """
        Returns the XNAT session and cache dir corresponding to the provided
        row

        Parameters
        ----------
        row : DataRow
            The row to get the corresponding XNAT row for
        """
        with self.connection:
            xproject = self.connection.projects[row.dataset.id]
            if row.frequency == Clinical.dataset:
                xrow = xproject
            elif row.frequency == Clinical.subject:
                xrow = xproject.subjects[row.ids[Clinical.subject]]
            elif row.frequency == Clinical.session:
                xrow = xproject.experiments[row.ids[Clinical.session]]
            else:
                xrow = self.connection.classes.SubjectData(
                    label=self.make_row_name(row), parent=xproject
                )
            return xrow

    def get_dicom_header(self, uri: str):
        def convert(val, code):
            if code == "TM":
                try:
                    val = float(val)
                except ValueError:
                    pass
            elif code == "CS":
                val = val.split("\\")
            return val

        with self.connection:
            scan_uri = "/" + "/".join(uri.split("/")[2:-2])
            response = self.connection.get(
                "/REST/services/dicomdump?src=" + scan_uri
            ).json()["ResultSet"]["Result"]
        hdr = {
            tag_parse_re.match(t["tag1"]).groups(): convert(t["value"], t["vr"])
            for t in response
            if (tag_parse_re.match(t["tag1"]) and t["vr"] in RELEVANT_DICOM_TAG_TYPES)
        }
        return hdr

    def make_row_name(self, row):
        # Create a "subject" to hold the non-standard row (i.e. not
        # a project, subject or session row)
        if row.id is None:
            id_str = ""
        elif isinstance(row.id, tuple):
            id_str = "_" + "_".join(row.id)
        else:
            id_str = "_" + str(row.id)
        return f"__{row.frequency}{id_str}__"

    def cache_path(self, uri: str):
        """Path to the directory where the item is/should be cached. Note that
        the URI of the item needs to be set beforehand

        Parameters
        ----------
        uri :  `str`
            the uri of the entry to be cached

        Returns
        -------
        cache_path : Path
            the path to the directory where the entry will be cached
        """
        return self.cache_dir.joinpath(*uri.split("/")[3:])

    def _check_store(self, entry):
        if entry.row.dataset.store is not self:
            raise ArcanaWrongRepositoryError(
                f"{entry} is from {entry.dataset.store} instead of {self}"
            )

    def site_licenses_dataset(self):
        """Return a dataset that holds site-wide licenses

        Returns
        -------
        Dataset or None
            the dataset that holds site-wide licenses
        """
        try:
            user = os.environ[self.SITE_LICENSES_USER_ENV]
        except KeyError:
            store = self
        else:
            # Reconnect to store with site-license user/password
            store = type(self)(
                server=self,
                cache_dir=self.cache_dir,
                user=user,
                password=os.environ[self.SITE_LICENSES_PASS_ENV],
            )
        try:
            dataset_name = os.environ[self.SITE_LICENSES_DATASET_ENV]
        except KeyError:
            return None
        return store.load_dataset(dataset_name)

    # @classmethod
    # def human_readable_uri(cls, xresource):
    #     """Convert the given URI to one that uses the labels instead of the internal
    #     XNAT IDs to address the resource

    #     >>> from arcana.xnat.data import Xnat
    #     >>> store = Xnat.load('my-xnat')
    #     >>> xrow = store.login.experiments['MRH017_100_MR01']
    #     >>> store.standard_uri(xrow)

    #     '/data/archive/projects/MRH017/subjects/MRH017_100/experiments/MRH017_100_MR01'

    #     Parameters
    #     ----------
    #     xrow : xnat.ImageSession | xnat.Subject | xnat.Project
    #         A row of the XNAT data tree
    #     """
    #     uri = self._get_resource_uri(xresource)
    #     # Replace resource ID with resource label
    #     uri = re.sub(r"(?<=/resources/)[^/]+", xresource.label, uri)
    #     xrow = xresource.parent_obj
    #     if xrow.__xsi_type__ == "xnat:mrScanData":

    #     if "experiments" in uri:
    #         # Replace ImageSession ID with label in URI.
    #         uri = re.sub(r"(?<=/experiments/)[^/]+", xrow.label, uri)
    #     if "subjects" in uri:
    #         try:
    #             # If xrow is a ImageSession
    #             subject_id = xrow.subject.label
    #         except AttributeError:
    #             # If xrow is a Subject
    #             subject_id = xrow.label
    #         except KeyError:
    #             # There is a bug where the subject does't appeared to be cached
    #             # so we use this as a workaround
    #             subject_json = xrow.xnat_session.get_json(
    #                 xrow.uri.split("/experiments")[0]
    #             )
    #             subject_id = subject_json["items"][0]["data_fields"]["label"]
    #         # Replace subject ID with subject label in URI
    #         uri = re.sub(r"(?<=/subjects/)[^/]+", subject_id, uri)
    #     return uri

    @classmethod
    def standard_uri(cls, xrow):
        """Get the URI of the XNAT row (ImageSession | Subject | Project)
        using labels rather than IDs for subject and sessions, e.g

        >>> from arcana.xnat.data import Xnat
        >>> store = Xnat.load('my-xnat')
        >>> xrow = store.login.experiments['MRH017_100_MR01']
        >>> store.standard_uri(xrow)

        '/data/archive/projects/MRH017/subjects/MRH017_100/experiments/MRH017_100_MR01'

        Parameters
        ----------
        xrow : xnat.ImageSession | xnat.Subject | xnat.Project
            A row of the XNAT data tree
        """
        uri = xrow.uri
        if "experiments" in uri:
            # Replace ImageSession ID with label in URI.
            uri = re.sub(r"(?<=/experiments/)[^/]+", xrow.label, uri)
        if "subjects" in uri:
            try:
                # If xrow is a ImageSession
                subject_id = xrow.subject.label
            except AttributeError:
                # If xrow is a Subject
                subject_id = xrow.label
            except KeyError:
                # There is a bug where the subject does't appeared to be cached
                # so we use this as a workaround
                subject_json = xrow.xnat_session.get_json(
                    xrow.uri.split("/experiments")[0]
                )
                subject_id = subject_json["items"][0]["data_fields"]["label"]
            # Replace subject ID with subject label in URI
            uri = re.sub(r"(?<=/subjects/)[^/]+", subject_id, uri)
        return uri

    def _provenance_location(self, item, create_resource=False):
        xrow = self.get_xrow(item.row)
        if item.is_field:
            fname = self.FIELD_PROV_PREFIX + path2varname(item)
        else:
            fname = path2varname(item) + ".json"
        uri = f"{self.standard_uri(xrow)}/resources/{self.PROV_RESOURCE}/files/{fname}"
        cache_path = self.cache_path(uri)
        cache_path.parent.mkdir(parent=True, exist_ok=True)
        try:
            xresource = xrow.resources[self.PROV_RESOURCE]
        except KeyError:
            if create_resource:
                xresource = self.connection.classes.ResourceCatalog(
                    parent=xrow, label=self.PROV_RESOURCE, datatype="PROVENANCE"
                )
            else:
                raise
        return xresource, uri, cache_path

    def _encrypt_credentials(self, serialised):
        with self.connection:
            (
                serialised["user"],
                serialised["password"],
            ) = self.connection.services.issue_token()

    def asdict(self, **kwargs):
        # Call asdict utility method with 'ignore_instance_method' to avoid
        # infinite recursion
        dct = asdict(self, **kwargs)
        self._encrypt_credentials(dct)
        return dct

    SITE_LICENSES_DATASET_ENV = "ARCANA_SITE_LICENSE_DATASET"
    SITE_LICENSES_USER_ENV = "ARCANA_SITE_LICENSE_USER"
    SITE_LICENSES_PASS_ENV = "ARCANA_SITE_LICENSE_PASS"

    def create_empty_dataset(
        self,
        id: str,
        hierarchy: list[str],
        row_ids: list[list[str]],
        space: type = Clinical,
        name: str = None,
        **kwargs,
    ):
        raise NotImplementedError

    def create_test_dataset_data(
        self, blueprint: TestDatasetBlueprint, dataset_id: str, source_data: Path = None
    ):
        """
        Creates dataset for each entry in dataset_structures
        """

        with self.connection:
            self.connection.put(f"/data/archive/projects/{dataset_id}")

        with self.connection:
            xproject = self.connection.projects[dataset_id]
            xclasses = self.connection.classes
            for id_tple in product(*(list(range(d)) for d in blueprint.dim_lengths)):
                ids = dict(zip(Clinical.axes(), id_tple))
                # Create subject
                subject_label = "".join(f"{b}{ids[b]}" for b in Clinical.subject.span())
                xsubject = xclasses.SubjectData(label=subject_label, parent=xproject)
                # Create session
                session_label = "".join(f"{b}{ids[b]}" for b in Clinical.session.span())
                xsession = xclasses.MrSessionData(label=session_label, parent=xsubject)

                for i, scan in enumerate(blueprint.scans, start=1):
                    # Create scan
                    self.create_test_fsobject(
                        scan_id=i,
                        blueprint=scan,
                        parent=xsession,
                        source_data=source_data,
                    )

    def create_test_fsobject(
        self, scan_id: int, blueprint: ScanBlueprint, parent, source_data: Path = None
    ):
        xclasses = parent.xnat_session.classes
        xscan = xclasses.MrScanData(id=scan_id, type=blueprint.name, parent=parent)
        for resource in blueprint.resources:
            tmp_dir = Path(tempfile.mkdtemp())
            # Create the resource
            xresource = xscan.create_resource(resource.name)
            # Create the dummy files
            for fname in resource.filenames:
                fpath = super().create_test_fsobject(
                    fname,
                    tmp_dir,
                    source_data=source_data,
                    source_fallback=True,
                    escape_source_name=False,
                )
                xresource.upload(str(fpath), fpath.name)

    @classmethod
    def _get_resource_uri(cls, xresource):
        """Replaces the resource ID with the resource label"""
        return re.match(r"(.*/)[^/]+", xresource.uri).group(1) + xresource.label


def append_suffix(path, suffix):
    "Appends a string suffix to a Path object"
    return Path(str(path) + suffix)
