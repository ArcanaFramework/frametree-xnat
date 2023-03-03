import os.path as op
from pathlib import Path
import typing as ty
from glob import glob
import tempfile
import logging
import hashlib
import errno
from itertools import product
import json
import re
from zipfile import ZipFile, BadZipfile
import shutil
import attrs
import xnat.session
from fileformats.core import FileSet, Field
from fileformats.medimage import DicomSet
from fileformats.core.exceptions import FormatRecognitionError
from arcana.core.utils.misc import (
    path2varname,
    varname2path,
)
from arcana.core.data.store.remote import (
    RemoteStore,
)
from arcana.xnat.data.testing import TestXnatDatasetBlueprint
from arcana.core.data.row import DataRow
from arcana.core.exceptions import (
    ArcanaError,
    ArcanaUsageError,
)
from arcana.core.utils.serialize import asdict
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
class Xnat(RemoteStore):
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

    depth = 2
    DEFAULT_SPACE = Clinical
    DEFAULT_HIERARCHY = ["subject", "timepoint"]

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
                        datatype = DicomSet
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

    def download_files(
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
            expanded_dir = tmp_download_dir / "expanded"
            try:
                with ZipFile(zip_path) as zip_file:
                    zip_file.extractall(expanded_dir)
            except BadZipfile as e:
                raise ArcanaError(f"Could not unzip file '{zip_path}' ({e})") from e
            data_path = glob(str(expanded_dir) + "/**/files", recursive=True)[0]
            # Remove existing cache if present
            try:
                shutil.rmtree(target_path)
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise e
            shutil.move(data_path, target_path)

    def upload_files(self, cache_path: Path, entry: DataEntry):
        # Copy to cache
        xresource = self.connection.classes.Resource(
            uri=entry.uri, xnat_session=self.connection.session
        )
        xresource.upload_dir(cache_path, overwrite=True)

    def create_fileset_entry(
        self, path: str, datatype: type, row: DataRow
    ) -> DataEntry:
        """
        Creates a new resource entry to store a fileset

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
        return checksums

    def calculate_checksums(self, fileset: FileSet) -> dict[str, str]:
        """
        Downloads the checksum digests associated with the files in the file-set.
        These are saved with the downloaded files in the cache and used to
        check if the files have been updated on the server

        Parameters
        ----------
        uri: str
            uri of the data item to download the checksums for
        """
        return fileset.hash_files(crypto=hashlib.md5, relative_to=fileset.fspath.parent)    

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

    ####################
    # Helper Functions #
    ####################

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

    def _provenance_location(self, item, create_resource=False):
        xrow = self.get_xrow(item.row)
        if item.is_field:
            fname = self.FIELD_PROV_PREFIX + path2varname(item)
        else:
            fname = path2varname(item) + ".json"
        uri = f"{xrow.uri}/resources/{self.PROV_RESOURCE}/files/{fname}"
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

    def create_test_dataset_data(
        self, blueprint: TestXnatDatasetBlueprint, dataset_id: str, source_data: Path = None
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
                super().create_test_fsobject(
                    fname,
                    tmp_dir,
                    source_data=source_data,
                    source_fallback=True,
                    escape_source_name=False,
                )
            xresource.upload_dir(tmp_dir)

    @classmethod
    def _get_resource_uri(cls, xresource):
        """Replaces the resource ID with the resource label"""
        return re.match(r"(.*/)[^/]+", xresource.uri).group(1) + xresource.label
