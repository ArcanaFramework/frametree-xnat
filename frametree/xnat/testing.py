import typing as ty
from pathlib import Path
import logging
import tempfile
import attrs
from frametree.common import Clinical
from frametree.core.axes import Axes
from frametree.core.row import DataRow
from frametree.testing.blueprint import TestDatasetBlueprint, FileSetEntryBlueprint


logger = logging.getLogger("frametree")

__all__ = ["ScanBlueprint", "TestXnatDatasetBlueprint"]


@attrs.define
class ScanBlueprint:

    name: str
    resources: ty.List[FileSetEntryBlueprint]


@attrs.define(slots=False, kw_only=True)
class TestXnatDatasetBlueprint(TestDatasetBlueprint):  # type: ignore[misc]

    scans: ty.List[ScanBlueprint]

    # Overwrite attributes in core blueprint class
    axes: type = Clinical
    hierarchy: ty.List[Axes] = ["subject", "session"]
    filesets: ty.Optional[ty.List[str]] = None

    def make_entries(self, row: DataRow, source_data: ty.Optional[Path] = None) -> None:
        logger.debug("Making entries in %s row: %s", row, self.scans)
        xrow = row.frameset.store.get_xrow(row)
        xclasses = xrow.xnat_session.classes
        for scan_id, scan_bp in enumerate(self.scans, start=1):
            xscan = xclasses.MrScanData(id=scan_id, type=scan_bp.name, parent=xrow)
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
                item.copy(tmp_dir)
                xresource.upload_dir(tmp_dir)


__all__ = ["TestXnatDatasetBlueprint", "ScanBlueprint"]
