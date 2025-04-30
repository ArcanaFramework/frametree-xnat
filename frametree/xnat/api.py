import os.path as op
from pathlib import Path
import typing as ty
import tempfile
import logging
import hashlib
import json
import re
from operator import attrgetter
from zipfile import ZipFile, BadZipfile
import attrs
import xnat.session
from fileformats.core import FileSet, Field
from fileformats.medimage import DicomSeries
from fileformats.core.exceptions import FormatRecognitionError
from frametree.core.utils import (
    path2label,
    label2path,
)
from frametree.core.store.remote import (
    RemoteStore,
)
from frametree.core.row import DataRow
from frametree.core.exceptions import (
    FrameTreeError,
    FrameTreeUsageError,
)
from frametree.core.serialize import asdict
from frametree.core.axes import Axes
from frametree.core.tree import DataTree
from frametree.core.entry import DataEntry
from frametree.common import Clinical


logger = logging.getLogger("frametree")

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

    verify_ssl: bool = True

    depth = 2
    DEFAULT_AXES = Clinical
    DEFAULT_HIERARCHY = ("subject", "session")
    # DEFAULT_ID_PATTERNS = (("visit", "session:order"),)
    PROV_RESOURCE = "PROVENANCE"

    #############################
    # Store implementations #
    #############################

    def populate_tree(self, tree: DataTree) -> None:
        """
        Populates the nodes of the data tree with those found in the dataset

        Parameters
        ----------
        tree : DataTree
            The tree to populate with nodes via the ``DataTree.add_leaf`` method
        """
        with self.connection:
            # Get all "leaf" nodes, i.e. XNAT imaging session objects
            xproject = self.connection.projects[tree.dataset_id]
            subjects = sorted(xproject.subjects.values(), key=attrgetter("label"))
            for xsubject in subjects:
                # Sort sessions into a logical order
                xsessions = sorted(
                    xsubject.experiments.values(),
                    key=lambda x: (x.date, x.time, x.label),
                )
                for xsess in xsessions:
                    date = xsess.date.strftime("%Y%m%d") if xsess.date else None
                    metadata = {
                        "session": {
                            "date": date,
                            "visit_id": xsess.visit_id,
                            "age": xsess.age,
                            "modality": xsess.modality,
                        }
                    }
                    tree.add_leaf([xsubject.label, xsess.label], metadata=metadata)

    def populate_row(self, row: DataRow) -> None:
        """
        Populate a row with all data entries found in the corresponding node in the data
        store (e.g. files within a directory, scans within an XNAT session).

        Parameters
        ----------
        row : DataRow
            The row to populate with entries using the ``DataRow.add_entry`` method
        """
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
                            datatype = DicomSeries
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
                if xresource.label == self.METADATA_RESOURCE:
                    continue
                uri = self._get_resource_uri(xresource)
                try:
                    datatype = FileSet.from_mime(xresource.format)
                except FormatRecognitionError:
                    datatype = FileSet
                row.add_entry(
                    path=label2path(xresource.label),
                    datatype=datatype,
                    uri=uri,
                    checksums=self.get_checksums(uri),
                )

    def save_frameset_definition(
        self, dataset_id: str, definition: ty.Dict[str, ty.Any], name: str
    ) -> None:
        """Save definition of dataset within the store

        Parameters
        ----------
        dataset_id: str
            The ID/path of the dataset within the store
        definition: ty.Dict[str, Any]
            A dictionary containing the dct FrameSet to be saved. The
            dictionary is in a format ready to be dumped to file as JSON or
            YAML.
        name: str
            Name for the dataset definition to distinguish it from other
            definitions for the same directory/project
        """
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

    def load_frameset_definition(
        self, dataset_id: str, name: str
    ) -> ty.Dict[str, ty.Any]:
        """Load definition of a dataset saved within the store

        Parameters
        ----------
        dataset_id: str
            The ID (e.g. file-system path, XNAT project ID) of the project
        name: str
            Name for the dataset definition to distinguish it from other
            definitions for the same directory/project

        Returns
        -------
        definition: ty.Dict[str, Any]
            A dct FrameSet object that was saved in the data store
        """
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
                    / self.METADATA_RESOURCE
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
        The XnatPy connection to the data store

        Returns
        ----------
        session : xnat.XNATSession
            An XNAT login that has been opened in the code that calls
            the method that calls login. It is wrapped in a
            NoExitWrapper so the returned connection can be used
            in a "with" statement in the method.
        """
        kwargs = {}
        if self.user is not None:
            kwargs["user"] = self.user
        if self.password is not None:
            kwargs["password"] = self.password
        return xnat.connect(server=self.server, verify=self.verify_ssl, **kwargs)

    def disconnect(self, session: xnat.XNATSession) -> None:
        """
        Close the XnatPy session object

        Parameters
        ----------
        session : xnat.XNATSession
            the XnatPy session object returned by `connect` to be closed
        """
        session.disconnect()

    def put_provenance(
        self, provenance: ty.Dict[str, ty.Any], entry: DataEntry
    ) -> None:
        """Stores provenance information for a given data item in the store

        Parameters
        ----------
        entry: DataEntry
            The item to store the provenance data for
        provenance: ty.Dict[str, Any]
            The provenance data to store
        """
        xresource, _, cache_path = self._provenance_location(
            entry, create_resource=True
        )
        with open(cache_path, "w") as f:
            json.dump(provenance, f, indent="  ")
        xresource.upload(str(cache_path), cache_path.name)

    def get_provenance(self, entry: DataEntry) -> ty.Dict[str, ty.Any]:
        """Stores provenance information for a given data item in the store

        Parameters
        ----------
        entry: DataEntry
            The item to store the provenance data for

        Returns
        -------
        provenance: ty.Dict[str, Any] or None
            The provenance data stored in the repository for the data item.
            None if no provenance data has been stored
        """
        try:
            xresource, uri, cache_path = self._provenance_location(entry)
        except KeyError:
            return {}  # Provenance doesn't exist on server
        with open(cache_path, "wb") as f:
            xresource.xnat_session.download_stream(uri, f)
        with open(cache_path) as f:
            provenance = json.load(f)
        return provenance

    def create_data_tree(
        self,
        id: str,
        leaves: ty.List[ty.Tuple[str, ...]],
        hierarchy: ty.List[str],
        axes: ty.Type[Axes],
        **kwargs: ty.Any,
    ) -> None:
        """Creates a new empty dataset within in the store. Used in test routines and
        importing/exporting datasets between stores

        Parameters
        ----------
        id : str
            ID for the newly created dataset
        leaves : list[tuple[str, ...]]
                        list of IDs for each leaf node to be added to the dataset. The IDs for each
            leaf should be a tuple with an ID for each level in the tree's hierarchy, e.g.
            for a hierarchy of [subject, visit] ->
            [("SUBJ01", "TIMEPOINT01"), ("SUBJ01", "TIMEPOINT02"), ....]
        **kwargs
            kwargs are ignored
        """
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

    def download_files(self, entry: DataEntry, download_dir: Path) -> Path:
        """Download files associated with the given entry in the data store, using
        `download_dir` as temporary storage location (will be monitored by downloads
        in sibling processes to detect if download activity has stalled), return the
        path to a directory containing only the downloaded files

        Parameters
        ----------
        entry : DataEntry
            entry in the data store to download the files/directories from
        download_dir : Path
            temporary storage location for the downloaded files and/or compressed
            archives. Monitored by sibling processes to detect if download activity
            has stalled.

        Returns
        -------
        output_dir : Path
            a directory containing the downloaded files/directories and nothing else
        """
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
            raise FrameTreeError(f"Could not unzip file '{zip_path}' ({e})") from e
        data_path = next(expanded_dir.glob("**/files"))
        return data_path

    def upload_files(self, cache_path: Path, entry: DataEntry) -> None:
        """Upload all files contained within `input_dir` to the specified entry in the
        data store

        Parameters
        ----------
        input_dir : Path
            directory containing the files/directories to be uploaded
        entry : DataEntry
            the entry in the data store to upload the files to
        """
        # Copy to cache
        xresource = self.connection.classes.Resource(
            uri=entry.uri, xnat_session=self.connection.session
        )
        # FIXME: work out which exception upload_dir raises when it can't overwrite
        # and catch it here and add more descriptive error message
        xresource.upload_dir(cache_path, overwrite=entry.is_derivative)

    def download_value(
        self, entry: DataEntry
    ) -> ty.Union[float, int, str, ty.List[float], ty.List[int], ty.List[str]]:
        """
        Extract and return the value of the field from the store

        Parameters
        ----------
        entry : DataEntry
            The data entry to retrieve the value from

        Returns
        -------
        value : float or int or str or list[float] or list[int] or list[str]
            The value of the Field
        """
        with self.connection:
            xrow = self.get_xrow(entry.row)
            val = xrow.fields[path2label(entry.path)]
            val = val.replace("&quot;", '"')  # Not sure this is necessary
        return val

    def upload_value(
        self,
        value: ty.Union[float, int, str, ty.List[float], ty.List[int], ty.List[str]],
        entry: DataEntry,
    ) -> None:
        """Store the value for a field in the XNAT repository

        Parameters
        ----------
        value : float or int or str or list[float] or list[int] or list[str]
            the value to store in the entry
        entry : DataEntry
            the entry to store the value in
        """
        with self.connection:
            xrow = self.get_xrow(entry.row)
            field_name = path2label(entry.path)
            if not entry.is_derivative and field_name in xrow.fields:
                field_name
                raise FrameTreeUsageError(
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
        logger.debug("creating %s entry at %s in %s", datatype, path, row)
        with self.connection:
            xrow = self.get_xrow(row)
            if not DataEntry.path_is_derivative(path):
                if row.frequency != Clinical.session:
                    raise FrameTreeUsageError(
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
            logger.debug("Created resource %s", xresource)
            # Add corresponding entry to row
            entry = row.add_entry(
                path=path,
                datatype=datatype,
                uri=self._get_resource_uri(xresource),
            )
        return entry

    def create_field_entry(self, path: str, datatype: type, row: DataRow) -> DataEntry:
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

    def get_checksums(self, uri: str) -> ty.Dict[str, str]:
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
            the way it is generated locally by FrameTree.

        Returns
        -------
        checksums: dict
            a dictionary of checksums for the files in the fileset
        """
        if uri is None:
            raise FrameTreeUsageError(
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

    def calculate_checksums(self, fileset: FileSet) -> ty.Dict[str, str]:
        """
        Downloads the checksum digests associated with the files in the file-set.
        These are saved with the downloaded files in the cache and used to
        check if the files have been updated on the server

        Parameters
        ----------
        uri: str
            uri of the data item to download the checksums for
        """
        return fileset.hash_files(crypto=hashlib.md5, relative_to=fileset.parent)

    ##################
    # Helper methods #
    ##################

    def get_xrow(self, row: DataRow) -> "xnat.ResourceCatalog":
        """
        Returns the XNAT session and cache dir corresponding to the provided
        row

        Parameters
        ----------
        row : DataRow
            The row to get the corresponding XNAT row for
        """
        with self.connection:
            xproject = self.connection.projects[row.frameset.id]
            if row.frequency == Clinical.constant:
                xrow = xproject
            elif row.frequency == Clinical.subject:
                xrow = xproject.subjects[row.frequency_id("subject")]
            elif row.frequency == Clinical.session:
                xrow = xproject.experiments[row.frequency_id("session")]
            else:
                # For rows that don't have a place within the standard XNAT hierarchy,
                # e.g. groups, we create a dummy subject with an escaped name to hold
                # the associated data
                xrow = self.connection.classes.SubjectData(
                    label=self.make_row_name(row), parent=xproject
                )
            return xrow

    def get_dicom_header(self, uri: str) -> ty.Dict[str, ty.Any]:
        def convert(val: ty.Any, code: str) -> ty.Any:
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

    def make_row_name(self, row: DataRow) -> str:
        # Create a "subject" to hold the non-standard row (i.e. not
        # a project, subject or session row)
        if row.id is None:
            id_str = ""
        elif isinstance(row.id, tuple):
            id_str = "_" + "_".join(row.id)
        else:
            id_str = "_" + str(row.id)
        return f"__{row.frequency}{id_str}__"

    def _provenance_location(
        self, entry: DataEntry, create_resource: bool = False
    ) -> ty.Tuple["xnat.ResourceCatalog", str, Path]:
        xrow = self.get_xrow(entry.row)
        fname = path2label(entry.path) + ".json"
        uri = f"{xrow.uri}/resources/{self.PROV_RESOURCE}/files/{fname}"
        cache_path = self.cache_path(uri)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            xresource = xrow.resources[self.PROV_RESOURCE]
        except KeyError:
            if create_resource:
                xresource = self.connection.classes.ResourceCatalog(
                    parent=xrow, label=self.PROV_RESOURCE, format="PROVENANCE"
                )
            else:
                raise
        return xresource, uri, cache_path

    def _encrypt_credentials(self, serialised: ty.Dict[str, ty.Any]) -> None:
        with self.connection:
            (
                serialised["user"],
                serialised["password"],
            ) = self.connection.services.issue_token()

    def asdict(self, **kwargs: ty.Any) -> ty.Dict[str, ty.Any]:
        # Call asdict utility method with 'ignore_instance_method' to avoid
        # infinite recursion
        dct = asdict(self, **kwargs)
        self._encrypt_credentials(dct)
        return dct  # type: ignore[no-any-return]

    @classmethod
    def _get_resource_uri(cls, xresource: "xnat.ResourceCatalog") -> str:
        """Replaces the resource ID with the resource label"""
        return (  # type: ignore[no-any-return]
            re.match(r"(.*/)[^/]+", xresource.uri).group(1) + xresource.label  # type: ignore[union-attr]
        )
