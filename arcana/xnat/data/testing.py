import typing as ty
import attrs
from arcana.core.data.space import DataSpace
from arcana.core.data import Clinical
from arcana.core.data.testing import TestDatasetBlueprint


@attrs.define
class ResourceBlueprint:

    name: str
    datatype: type
    filenames: ty.List[str]


@attrs.define
class ScanBlueprint:

    name: str
    resources: ty.List[ResourceBlueprint]


@attrs.define(slots=False, kw_only=True)
class TestXnatDatasetBlueprint(TestDatasetBlueprint):

    scans: ty.List[ScanBlueprint]

    # Overwrite attributes in core blueprint class
    hierarchy: list[DataSpace] = [Clinical.subject, Clinical.session]
    files: list[str] = None
