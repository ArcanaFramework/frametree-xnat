import os.path as op
from pathlib import Path
import typing as ty
from glob import glob
import tempfile
import logging
import hashlib
from itertools import product
import json
import re
from zipfile import ZipFile, BadZipfile
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
from arcana.core.data.row import DataRow
from arcana.core.exceptions import (
    ArcanaError,
    ArcanaUsageError,
)
from arcana.core.utils.serialize import asdict
from arcana.core.data.tree import DataTree
from arcana.core.data.set import Dataset
from arcana.core.data.entry import DataEntry
from arcana.core.data import Clinical


logger = logging.getLogger("arcana")

tag_parse_re = re.compile(r"\((\d+),(\d+)\)")

RELEVANT_DICOM_TAG_TYPES = set(("UI", "CS", "DA", "TM", "SH", "LO", "PN", "ST", "AS"))


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

    #############################
    # DataStore implementations #
    #############################

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
                        uri = self._get_resource_uri(xresource)
                        if xresource.label in ("DICOM", "secondary"):
                            datatype = DicomSet
                            item_metadata = self.get_dicom_header(uri)
                        else:
                            datatype = FileSet
                            item_metadata = {}
                        row.add_entry(
                            path=f"{xscan.type}/{xresource.label}",
                            datatype=datatype,
                            order=xscan.id,
                            quality=xscan.quality,
                            item_metadata=item_metadata,
                            uri=uri,
                        )
            for field_id in xrow.fields:
                row.add_entry(path=label2path(field_id), datatype=Field, uri=None)
            for xresource in xrow.resources.values():
                uri = self._get_resource_uri(xresource)
                try:
                    datatype = FileSet.from_mime(xresource.format)
                except FormatRecognitionError:
                    datatype = FileSet
                # "Derivative" entry paths are of the form "@dataset_name/column_name"
                # escaped by `path2label`. So we reverse the escape here
                path = label2path(xresource.label)
                if "@" not in path:
                    path += "@"
                row.add_entry(
                    path=path,
                    datatype=datatype,
                    uri=uri,
                    checksums=self.get_checksums(uri),
                )

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

    def create_data_tree(
        self,
        id: str,
        leaves: list[tuple[str, ...]],
        **kwargs
    ):
        with self.connection:
            self.connection.put(f"/data/archive/projects/{id}")
            xproject = self.connection.projects[id]
            xclasses = self.connection.classes
            for ids_tuple in leaves:
                subject_id, session_id = ids_tuple
                # Create subject
                xsubject = xclasses.SubjectData(label=subject_id, parent=xproject)
                # Create session
                xclasses.MrSessionData(label=session_id, parent=xsubject)

    ################################
    # RemoteStore-specific methods #
    ################################

    def download_files(
        self, entry: DataEntry, download_dir: Path
    ) -> Path:
        with self.connection:
            # Download resource to zip file
            zip_path = op.join(download_dir, "download.zip")
            with open(zip_path, "wb") as f:
                self.connection.download_stream(
                    entry.uri + "/files", f, format="zip", verbose=True
                )
            # Extract downloaded zip file
            expanded_dir = download_dir / "expanded"
            try:
                with ZipFile(zip_path) as zip_file:
                    zip_file.extractall(expanded_dir)
            except BadZipfile as e:
                raise ArcanaError(f"Could not unzip file '{zip_path}' ({e})") from e
            data_path = glob(str(expanded_dir) + "/**/files", recursive=True)[0]
        return data_path

    def upload_files(self, cache_path: Path, entry: DataEntry):
        # Copy to cache
        xresource = self.connection.classes.Resource(
            uri=entry.uri, xnat_session=self.connection.session
        )
        # FIXME: work out which exception upload_dir raises when it can't overwrite
        # and catch it here and add more descriptive error message
        xresource.upload_dir(cache_path, overwrite=entry.is_derivative)

    def download_value(self, entry: DataEntry):
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
            val = xrow.fields[path2label(entry.path)]
            val = val.replace("&quot;", '"')  # Not sure this is necessary
        return val

    def upload_value(self, value, entry: DataEntry):
        """Store the value for a field in the XNAT repository

        Parameters
        ----------
        field : Field
            the field to store the value for
        value : str or float or int or bool
            the value to store
        """
        with self.connection:
            xrow = self.get_xrow(entry.row)
            field_name = path2label(entry.path)
            if not entry.is_derivative and field_name in xrow.fields:
                field_name
                raise ArcanaUsageError(
                    f"Refusing to overwrite non-derivative field {entry.path} in {xrow}"
                )
            xrow.fields[field_name] = str(value)

    def create_fileset_entry(
        self, path: str, datatype: type, row: DataRow
    ) -> DataEntry:
        """
        Creates a new resource entry to store a fileset

        Parameters
        ----------
        path: str
            the path to the entry relative to the row
        datatype : type
            the datatype of the entry
        row : DataRow
            the row of the data entry
        """
        # Open XNAT connection session
        with self.connection:
            xrow = self.get_xrow(row)
            if not DataEntry.path_is_derivative(path):
                if row.frequency != Clinical.session:
                    raise ArcanaUsageError(
                        f"Cannot create file-set entry for '{path}': non-derivative "
                        "file-sets (specified by entry paths that don't contain a "
                        "'@' separator) are only allowed in MRSession nodes"
                    )
                scan_id, resource_label = path.split("/")
                parent = self.connection.classes.MrScanData(
                    id=scan_id,
                    parent=xrow,
                )
                xformat = None
            else:
                parent = xrow
                xformat = datatype.mime_like
                resource_label = path2label(path)
            xresource = self.connection.classes.ResourceCatalog(
                parent=parent,
                label=resource_label,
                format=xformat,
            )
            # Add corresponding entry to row
            entry = row.add_entry(
                path=path,
                datatype=datatype,
                uri=self._get_resource_uri(xresource),
            )
        return entry

    def create_field_entry(self, path: str, datatype: type, row: DataRow):
        """
        Creates a new resource entry to store a field

        Parameters
        ----------
        path: str
            the path to the entry relative to the row
        datatype : type
            the datatype of the entry
        row : DataRow
            the row of the data entry
        """
        return row.add_entry(path, datatype, uri=None)

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

    ##################
    # Helper methods #
    ##################

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
                xrow = xproject.subjects[row.frequency_id("subject")]
            elif row.frequency == Clinical.session:
                xrow = xproject.experiments[row.frequency_id("session")]
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

    def _provenance_location(self, item, create_resource=False):
        xrow = self.get_xrow(item.row)
        if item.is_field:
            fname = self.FIELD_PROV_PREFIX + path2label(item)
        else:
            fname = path2label(item) + ".json"
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

    @classmethod
    def _get_resource_uri(cls, xresource):
        """Replaces the resource ID with the resource label"""
        return re.match(r"(.*/)[^/]+", xresource.uri).group(1) + xresource.label


def path2label(path: str):
    if path.endswith("@"):
        path += Dataset.EMPTY_NAME
    return path2varname(path)


def label2path(label: str):
    path = varname2path(label)
    if path.endswith(f"@{Dataset.EMPTY_NAME}"):
        path = path.rstrip(Dataset.EMPTY_NAME)
    return path
