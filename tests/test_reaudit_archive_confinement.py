from __future__ import annotations

import contextlib
import io
import shutil
import stat
import tarfile
import tempfile
import unittest
import warnings
import zipfile
from pathlib import Path
from typing import Any
from unittest import mock

from tools import prepare_market_runtime as preparer


class ArchiveConfinementReauditTests(unittest.TestCase):
    @staticmethod
    def _wheel(path: Path, members: tuple[tuple[str, bytes], ...] = ()) -> Path:
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr(
                "fake_pypi-1.0.dist-info/METADATA",
                "Metadata-Version: 2.1\nName: fake-pypi\nVersion: 1.0\n",
            )
            for name, payload in members:
                archive.writestr(name, payload)
        return path

    @staticmethod
    def _canonical_path(archive: Path) -> str:
        return f"dependencies/dev/{archive.name}"

    def test_safe_fake_pypi_archive_stays_below_root(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-archive-safe-") as raw:
            base = Path(raw)
            root = base / "stage"
            archive = self._wheel(
                base / "fake_pypi-1.0-py3-none-any.whl",
                (("fake_pypi-1.0.dist-info/LICENSE", b"license\n"),),
            )

            record = preparer.preserve_archive(
                archive,
                root=root,
                relative_path=self._canonical_path(archive),
                origin="https://pypi.org/simple/ (fake local test)",
                kind="development",
            )

            self.assertEqual(record["path"], self._canonical_path(archive))
            self.assertTrue((root / record["path"]).is_file())
            self.assertTrue(all((root / item).is_file() for item in record["license_files"]))
            self.assertTrue((root / record["path"]).resolve().is_relative_to(root.resolve()))

    def test_root_metadata_ignores_vendored_dist_info_metadata(self) -> None:
        """A setuptools-like wheel has one root metadata and many vendored ones."""

        with tempfile.TemporaryDirectory(prefix="mtf-archive-vendored-") as raw:
            base = Path(raw)
            root = base / "stage"
            archive = base / "setuptools-84.0.0-py3-none-any.whl"
            with zipfile.ZipFile(archive, "w") as wheel:
                wheel.writestr(
                    "setuptools-84.0.0.dist-info/METADATA",
                    "Metadata-Version: 2.1\nName: setuptools\nVersion: 84.0.0\nLicense: MIT\n",
                )
                wheel.writestr("setuptools-84.0.0.dist-info/WHEEL", "Wheel-Version: 1.0\n")
                wheel.writestr("setuptools-84.0.0.dist-info/licenses/LICENSE", b"root license\n")
                wheel.writestr(
                    "setuptools/_vendor/packaging-26.0.dist-info/METADATA",
                    "Metadata-Version: 2.1\nName: packaging\nVersion: 26.0\n",
                )
                wheel.writestr("setuptools/_vendor/packaging-26.0.dist-info/LICENSE", b"vendored license\n")

            record = preparer.preserve_archive(
                archive,
                root=root,
                relative_path=self._canonical_path(archive),
                origin="test-local-vendored",
                kind="development",
            )

            self.assertEqual("setuptools", record["name"])
            self.assertEqual("84.0.0", record["version"])
            self.assertEqual(2, len(record["license_files"]))
            self.assertTrue((root / record["path"]).is_file())

    def test_root_dist_info_metadata_ambiguity_is_rejected(self) -> None:
        cases = {
            "nested-only": (("pkg/_vendor/dep-1.0.dist-info/METADATA", "Name: dep\nVersion: 1.0\n"),),
            "two-root-metadata": (
                ("pkg-1.0.dist-info/METADATA", "Name: pkg\nVersion: 1.0\n"),
                ("other-1.0.dist-info/METADATA", "Name: other\nVersion: 1.0\n"),
            ),
            "second-root-dist-info": (
                ("pkg-1.0.dist-info/METADATA", "Name: pkg\nVersion: 1.0\n"),
                ("other-1.0.dist-info/WHEEL", "Wheel-Version: 1.0\n"),
            ),
        }

        for label, members in cases.items():
            with self.subTest(label=label), tempfile.TemporaryDirectory(prefix="mtf-archive-ambiguous-") as raw:
                base = Path(raw)
                root = base / "stage"
                archive = base / f"{label}.whl"
                with zipfile.ZipFile(archive, "w") as wheel:
                    for name, payload in members:
                        wheel.writestr(name, payload)

                with self.assertRaises(preparer.PreparationError):
                    preparer.preserve_archive(
                        archive,
                        root=root,
                        relative_path=self._canonical_path(archive),
                        origin="test-local-ambiguous",
                        kind="development",
                    )
                self.assertFalse(root.exists())

    def test_absolute_license_is_rejected_before_any_write(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-archive-absolute-") as raw:
            base = Path(raw)
            root = base / "stage"
            outside = base / "outside" / "LICENSE"
            outside.parent.mkdir()
            archive = self._wheel(
                base / "fake_abs-1.0-py3-none-any.whl",
                ((str(outside), b"must-not-write\n"),),
            )

            with self.assertRaises(preparer.PreparationError):
                preparer.preserve_archive(
                    archive,
                    root=root,
                    relative_path=self._canonical_path(archive),
                    origin="fake-pypi",
                    kind="development",
                )

            self.assertFalse(outside.exists())
            self.assertFalse(root.exists())

    def test_dotdot_license_and_windows_anchor_are_rejected_before_any_write(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-archive-paths-") as raw:
            base = Path(raw)
            root = base / "stage"
            archive = self._wheel(
                base / "fake_paths-1.0-py3-none-any.whl",
                (("../outside/LICENSE", b"must-not-write\n"),),
            )

            with self.assertRaises(preparer.PreparationError):
                preparer.preserve_archive(
                    archive,
                    root=root,
                    relative_path=self._canonical_path(archive),
                    origin="fake-pypi",
                    kind="development",
                )
            self.assertFalse(root.exists())

            safe = self._wheel(base / "fake_windows-1.0-py3-none-any.whl")
            with self.assertRaises(preparer.PreparationError):
                preparer.preserve_archive(
                    safe,
                    root=root,
                    relative_path="C:/outside/fake.whl",
                    origin="fake-pypi",
                    kind="development",
                )
            self.assertFalse(root.exists())

            windows_member = self._wheel(
                base / "fake_windows_member-1.0-py3-none-any.whl",
                (("C:/outside/LICENSE", b"must-not-write\n"),),
            )
            with self.assertRaises(preparer.PreparationError):
                preparer.preserve_archive(
                    windows_member,
                    root=root,
                    relative_path=self._canonical_path(windows_member),
                    origin="fake-pypi",
                    kind="development",
                )
            self.assertFalse(root.exists())

    def test_relative_path_traversal_is_rejected_before_any_write(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-archive-target-") as raw:
            base = Path(raw)
            root = base / "stage"
            archive = self._wheel(base / "fake_target-1.0-py3-none-any.whl")

            with self.assertRaises(preparer.PreparationError):
                preparer.preserve_archive(
                    archive,
                    root=root,
                    relative_path="../escaped.whl",
                    origin="fake-pypi",
                    kind="development",
                )
            self.assertFalse((base / "escaped.whl").exists())
            self.assertFalse(root.exists())

    def test_symlinked_parent_and_target_are_rejected_without_touching_target(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-archive-symlink-") as raw:
            base = Path(raw)
            root = base / "stage"
            outside = base / "outside"
            outside.mkdir()
            archive = self._wheel(base / "fake_symlink-1.0-py3-none-any.whl")

            (root / "dependencies").parent.mkdir(parents=True)
            (root / "dependencies").symlink_to(outside, target_is_directory=True)
            with self.assertRaises(preparer.PreparationError):
                preparer.preserve_archive(
                    archive,
                    root=root,
                    relative_path=self._canonical_path(archive),
                    origin="fake-pypi",
                    kind="development",
                )
            self.assertEqual([], list(outside.iterdir()))

            root = base / "stage-target"
            target = root / "dependencies/dev" / archive.name
            target.parent.mkdir(parents=True)
            target.symlink_to(outside / "preserved.whl")
            (outside / "preserved.whl").write_bytes(b"original\n")
            with self.assertRaises(preparer.PreparationError):
                preparer.preserve_archive(
                    archive,
                    root=root,
                    relative_path=self._canonical_path(archive),
                    origin="fake-pypi",
                    kind="development",
                )
            self.assertEqual(b"original\n", (outside / "preserved.whl").read_bytes())

    def test_divergent_collision_is_rejected_and_originals_survive(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-archive-collision-") as raw:
            base = Path(raw)
            root = base / "stage"
            archive = self._wheel(
                base / "fake_collision-1.0-py3-none-any.whl",
                (("fake_pypi-1.0.dist-info/LICENSE", b"new-license\n"),),
            )
            target = root / self._canonical_path(archive)
            target.parent.mkdir(parents=True)
            target.write_bytes(b"original-wheel\n")

            with self.assertRaises(preparer.PreparationError):
                preparer.preserve_archive(
                    archive,
                    root=root,
                    relative_path=self._canonical_path(archive),
                    origin="fake-pypi",
                    kind="development",
                )

            self.assertEqual(b"original-wheel\n", target.read_bytes())
            self.assertFalse((root / "licenses").exists())

    def test_divergent_license_collision_is_rejected_before_archive_write(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-archive-license-collision-") as raw:
            base = Path(raw)
            root = base / "stage"
            archive = self._wheel(
                base / "fake_license_collision-1.0-py3-none-any.whl",
                (("fake_pypi-1.0.dist-info/LICENSE", b"new-license\n"),),
            )
            license_path = root / "licenses/fake-pypi-1.0/fake_pypi-1.0.dist-info/LICENSE"
            license_path.parent.mkdir(parents=True)
            license_path.write_bytes(b"original-license\n")

            with self.assertRaises(preparer.PreparationError):
                preparer.preserve_archive(
                    archive,
                    root=root,
                    relative_path=self._canonical_path(archive),
                    origin="fake-pypi",
                    kind="development",
                )

            self.assertEqual(b"original-license\n", license_path.read_bytes())
            self.assertFalse((root / "dependencies").exists())

    def test_identical_collision_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-archive-idempotent-") as raw:
            base = Path(raw)
            root = base / "stage"
            archive = self._wheel(
                base / "fake_idempotent-1.0-py3-none-any.whl",
                (("fake_pypi-1.0.dist-info/LICENSE", b"license\n"),),
            )
            first = preparer.preserve_archive(
                archive,
                root=root,
                relative_path=self._canonical_path(archive),
                origin="fake-pypi",
                kind="development",
            )
            wheel_before = (root / first["path"]).read_bytes()
            license_before = (root / first["license_files"][0]).read_bytes()
            second = preparer.preserve_archive(
                archive,
                root=root,
                relative_path=self._canonical_path(archive),
                origin="fake-pypi",
                kind="development",
            )

            self.assertEqual(first["path"], second["path"])
            self.assertEqual(wheel_before, (root / second["path"]).read_bytes())
            self.assertEqual(license_before, (root / second["license_files"][0]).read_bytes())

    def test_multiple_wheel_licenses_are_published_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-archive-multi-license-") as raw:
            base = Path(raw)
            root = base / "stage"
            archive = self._wheel(
                base / "fake_multi-1.0-py3-none-any.whl",
                (
                    ("fake_pypi-1.0.dist-info/LICENSE", b"license\n"),
                    ("fake_pypi-1.0.dist-info/NOTICE", b"notice\n"),
                ),
            )
            first = preparer.preserve_archive(
                archive,
                root=root,
                relative_path=self._canonical_path(archive),
                origin="fake-pypi",
                kind="development",
            )
            second = preparer.preserve_archive(
                archive,
                root=root,
                relative_path=self._canonical_path(archive),
                origin="fake-pypi",
                kind="development",
            )
            self.assertEqual(2, len(first["license_files"]))
            self.assertEqual(first["license_files"], second["license_files"])
            self.assertEqual(
                [b"license\n", b"notice\n"],
                [(root / path).read_bytes() for path in first["license_files"]],
            )

    def test_multiple_tar_licenses_are_published_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-archive-multi-tar-") as raw:
            base = Path(raw)
            root = base / "stage"
            archive = base / "fake_multi-1.0.tar.gz"
            with tarfile.open(archive, "w:gz") as tar:
                metadata = tarfile.TarInfo("fake-1.0/PKG-INFO")
                metadata_payload = b"Name: fake\nVersion: 1.0\n"
                metadata.size = len(metadata_payload)
                tar.addfile(metadata, fileobj=io.BytesIO(metadata_payload))
                for name, payload in (("LICENSE", b"license\n"), ("NOTICE", b"notice\n")):
                    item = tarfile.TarInfo(f"fake-1.0/{name}")
                    item.size = len(payload)
                    tar.addfile(item, fileobj=io.BytesIO(payload))
            first = preparer.preserve_archive(
                archive,
                root=root,
                relative_path=f"dependencies/dev/{archive.name}",
                origin="fake-pypi",
                kind="development",
            )
            second = preparer.preserve_archive(
                archive,
                root=root,
                relative_path=f"dependencies/dev/{archive.name}",
                origin="fake-pypi",
                kind="development",
            )
            self.assertEqual(2, len(first["license_files"]))
            self.assertEqual(first["license_files"], second["license_files"])
            self.assertEqual(
                [b"license\n", b"notice\n"],
                [(root / path).read_bytes() for path in first["license_files"]],
            )

    def test_parent_symlink_swap_after_preflight_cannot_write_outside(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-archive-race-") as raw:
            base = Path(raw)
            root = base / "stage"
            (root / "dependencies/dev").mkdir(parents=True)
            outside = base / "outside"
            outside.mkdir()
            archive = self._wheel(base / "fake_race-1.0-py3-none-any.whl")
            original_publish = preparer._publish_new_copy
            swapped = False

            def swap_parent(*args: Any, **kwargs: Any) -> Any:
                nonlocal swapped
                if not swapped:
                    shutil.rmtree(root / "dependencies")
                    (root / "dependencies").symlink_to(outside, target_is_directory=True)
                    swapped = True
                return original_publish(*args, **kwargs)

            with (
                mock.patch.object(preparer, "_publish_new_copy", side_effect=swap_parent),
                contextlib.suppress(OSError, preparer.PreparationError),
            ):
                preparer.preserve_archive(
                    archive,
                    root=root,
                    relative_path=self._canonical_path(archive),
                    origin="fake-pypi",
                    kind="development",
                )
            self.assertTrue(swapped)
            self.assertEqual([], list(outside.iterdir()))

    def test_source_swap_after_metadata_preflight_publishes_pinned_bytes(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-archive-source-race-") as raw:
            base = Path(raw)
            root = base / "stage"
            root.mkdir()
            archive = self._wheel(
                base / "fake_source_race-1.0-py3-none-any.whl",
                (("fake_pypi-1.0.dist-info/LICENSE", b"original-license\n"),),
            )
            original_bytes = archive.read_bytes()
            original_publish = preparer._publish_new_copy
            swapped = False

            def swap_source(*args: Any, **kwargs: Any) -> Any:
                nonlocal swapped
                if not swapped:
                    archive.write_bytes(b"replacement-source-bytes\n")
                    swapped = True
                return original_publish(*args, **kwargs)

            with mock.patch.object(preparer, "_publish_new_copy", side_effect=swap_source):
                result = preparer.preserve_archive(
                    archive,
                    root=root,
                    relative_path=self._canonical_path(archive),
                    origin="fake-pypi",
                    kind="development",
                )
            self.assertTrue(swapped)
            self.assertEqual(original_bytes, (root / result["path"]).read_bytes())

    def test_source_same_size_mutation_during_snapshot_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-archive-source-mutation-") as raw:
            base = Path(raw)
            root = base / "stage"
            archive = self._wheel(base / "fake_same_size-1.0-py3-none-any.whl")
            replacement = b"X" * len(archive.read_bytes())
            original_copy = shutil.copyfileobj
            mutated = False

            def copy_then_mutate(source: Any, target: Any, length: int = 0) -> Any:
                nonlocal mutated
                result = original_copy(source, target, length=length)
                if not mutated:
                    archive.write_bytes(replacement)
                    mutated = True
                return result

            with (
                mock.patch.object(shutil, "copyfileobj", side_effect=copy_then_mutate),
                self.assertRaises(preparer.PreparationError),
            ):
                preparer.preserve_archive(
                    archive,
                    root=root,
                    relative_path=self._canonical_path(archive),
                    origin="fake-pypi",
                    kind="development",
                )
            self.assertTrue(mutated)
            self.assertFalse(root.exists())

    def test_repo_file_rejects_symlink_alias(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-archive-repo-file-") as raw:
            repo = Path(raw) / "repo"
            repo.mkdir()
            canonical = repo / "canonical.lock"
            canonical.write_text("canonical\n", encoding="utf-8")
            alias = repo / "requirements-dev.lock"
            alias.symlink_to(canonical.name)

            with self.assertRaises(preparer.PreparationError):
                preparer._repo_file(repo, Path("requirements-dev.lock"), "development lock")
            self.assertEqual("canonical\n", canonical.read_text(encoding="utf-8"))

    def test_duplicate_or_symlinked_zip_license_is_rejected_without_writes(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-archive-zip-members-") as raw:
            base = Path(raw)
            root = base / "stage"
            duplicate = base / "fake_duplicate-1.0-py3-none-any.whl"
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                with zipfile.ZipFile(duplicate, "w") as archive:
                    archive.writestr(
                        "fake_pypi-1.0.dist-info/METADATA",
                        "Name: fake-pypi\nVersion: 1.0\n",
                    )
                    archive.writestr("fake_pypi-1.0.dist-info/LICENSE", b"one")
                    archive.writestr("fake_pypi-1.0.dist-info/LICENSE", b"two")
            with self.assertRaises(preparer.PreparationError):
                preparer.preserve_archive(
                    duplicate,
                    root=root,
                    relative_path=self._canonical_path(duplicate),
                    origin="fake-pypi",
                    kind="development",
                )
            self.assertFalse(root.exists())

            symlinked = base / "fake_symlink_member-1.0-py3-none-any.whl"
            with zipfile.ZipFile(symlinked, "w") as archive:
                archive.writestr(
                    "fake_pypi-1.0.dist-info/METADATA",
                    "Name: fake-pypi\nVersion: 1.0\n",
                )
                info = zipfile.ZipInfo("fake_pypi-1.0.dist-info/LICENSE")
                info.create_system = 3
                info.external_attr = (stat.S_IFLNK | 0o777) << 16
                archive.writestr(info, b"outside-target")
            with self.assertRaises(preparer.PreparationError):
                preparer.preserve_archive(
                    symlinked,
                    root=root,
                    relative_path=self._canonical_path(symlinked),
                    origin="fake-pypi",
                    kind="development",
                )
            self.assertFalse(root.exists())

    def test_traversal_or_symlinked_tar_license_is_rejected_without_writes(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-archive-tar-members-") as raw:
            base = Path(raw)
            root = base / "stage"
            archive = base / "fake_tar-1.0.tar.gz"
            with tarfile.open(archive, "w:gz") as tar:
                metadata = tarfile.TarInfo("fake-1.0/PKG-INFO")
                metadata_payload = b"Name: fake\nVersion: 1.0\n"
                metadata.size = len(metadata_payload)
                tar.addfile(metadata, fileobj=io.BytesIO(metadata_payload))
                license_info = tarfile.TarInfo("../outside/LICENSE")
                license_payload = b"must-not-write\n"
                license_info.size = len(license_payload)
                tar.addfile(license_info, fileobj=io.BytesIO(license_payload))
            with self.assertRaises(preparer.PreparationError):
                preparer.preserve_archive(
                    archive,
                    root=root,
                    relative_path=f"dependencies/dev/{archive.name}",
                    origin="fake-pypi",
                    kind="development",
                )
            self.assertFalse(root.exists())

            symlink_archive = base / "fake_tar_symlink-1.0.tar.gz"
            with tarfile.open(symlink_archive, "w:gz") as tar:
                metadata = tarfile.TarInfo("fake-1.0/PKG-INFO")
                metadata_payload = b"Name: fake\nVersion: 1.0\n"
                metadata.size = len(metadata_payload)
                tar.addfile(metadata, fileobj=io.BytesIO(metadata_payload))
                link = tarfile.TarInfo("fake-1.0/LICENSE")
                link.type = tarfile.SYMTYPE
                link.linkname = "/outside/LICENSE"
                tar.addfile(link)
            with self.assertRaises(preparer.PreparationError):
                preparer.preserve_archive(
                    symlink_archive,
                    root=root,
                    relative_path=f"dependencies/dev/{symlink_archive.name}",
                    origin="fake-pypi",
                    kind="development",
                )
            self.assertFalse(root.exists())


if __name__ == "__main__":
    unittest.main()
