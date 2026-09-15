"""Offline contracts for explicit multi-month HistData acquisition."""

from __future__ import annotations

import hashlib
import io
import json
import os
import tempfile
import unittest
import zipfile
from pathlib import Path
from typing import Any, cast
from unittest import mock
from urllib.parse import parse_qs
from urllib.request import Request

import mtf_lab.data.histdata_acquisition as acquisition_impl
from mtf_lab.data.historical import validate_dataset


class _Response:
    def __init__(self, body: bytes, url: str, *, headers: dict[str, str] | None = None, code: int = 200) -> None:
        self._body = io.BytesIO(body)
        self._url = url
        self.headers = headers or {}
        self._code = code

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def geturl(self) -> str:
        return self._url

    def getcode(self) -> int:
        return self._code

    def read(self, size: int = -1) -> bytes:
        return self._body.read(size)


class _FakeOpener:
    def __init__(self, page: str, archive: bytes, form: dict[str, str] | None = None) -> None:
        self.page = page
        self.archive = archive
        self.form = form or {}
        self.requests: list[Request] = []

    def open(self, request: Request, timeout: float) -> _Response:
        del timeout
        self.requests.append(request)
        if request.full_url == self.page:
            fields = {
                "tk": "fixture-token",
                "date": self.form.get("date", "2017"),
                "datemonth": self.form.get("datemonth", "201704"),
                "platform": self.form.get("platform", "ASCII"),
                "timeframe": self.form.get("timeframe", "T"),
                "fxpair": self.form.get("fxpair", "EURUSD"),
            }
            inputs = "".join(f'<input type="hidden" name="{name}" value="{value}">' for name, value in fields.items())
            body = (
                f'<form id="file_down" action="{acquisition_impl.HISTDATA_DOWNLOAD_ENDPOINT}" method="POST">'
                f"{inputs}</form>"
            ).encode("ascii")
            return _Response(body, self.page)
        if request.full_url == acquisition_impl.HISTDATA_DOWNLOAD_ENDPOINT:
            return _Response(
                self.archive,
                acquisition_impl.HISTDATA_DOWNLOAD_ENDPOINT,
                headers={"Content-Length": str(len(self.archive)), "ETag": '"fixture-etag"'},
            )
        raise AssertionError(f"unexpected URL (network would be attempted): {request.full_url}")


def _archive_bytes(month: str, *, member_month: str | None = None, extra_month: str | None = None) -> bytes:
    member_month = member_month or month
    local_date = f"{member_month}03"
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            f"DAT_ASCII_EURUSD_T_{member_month}.csv",
            f"{local_date} 000000000,1.10000,1.10020,0\n{local_date} 000100000,1.10001,1.10021,0\n".encode("ascii"),
        )
        if extra_month is not None:
            archive.writestr(
                f"DAT_ASCII_EURUSD_T_{extra_month}.csv",
                f"{extra_month}03 000000000,1.20000,1.20020,0\n".encode("ascii"),
            )
    return output.getvalue()


class HistDataMultiMonthTests(unittest.TestCase):
    def test_acquire_month_requires_explicit_terms_acceptance(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "market-data"
            with self.assertRaisesRegex(acquisition_impl.AcquisitionError, "terms_accepted=True"):
                acquisition_impl.acquire_month(2017, 4, data_root=root)
            self.assertFalse(root.exists())

    def test_acquire_month_derives_form_url_and_private_artifacts(self) -> None:
        month = "201704"
        page = f"{acquisition_impl.HISTDATA_MONTH_PAGE_PREFIX}/2017/4"
        archive = _archive_bytes(month)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "market-data"
            opener = _FakeOpener(page, archive)
            receipt = acquisition_impl.acquire_month(
                2017, 4, data_root=root, opener=cast(Any, opener), terms_accepted=True
            )

            self.assertEqual(receipt.status, "DOWNLOADED")
            self.assertEqual(receipt.month, month)
            self.assertEqual(receipt.raw_sha256, hashlib.sha256(archive).hexdigest())
            self.assertEqual(receipt.archive_path.name, "HISTDATA_COM_ASCII_EURUSD_T_201704.zip")
            self.assertEqual(receipt.manifest_path.name, "histdata-eurusd-201704.json")
            self.assertEqual(receipt.archive_path.read_bytes(), archive)
            self.assertEqual(os.stat(root).st_mode & 0o777, 0o700)
            self.assertEqual(os.stat(root / "raw").st_mode & 0o777, 0o700)
            self.assertEqual(os.stat(root / "manifests").st_mode & 0o777, 0o700)
            self.assertEqual(os.stat(receipt.archive_path).st_mode & 0o777, 0o600)
            self.assertEqual(os.stat(receipt.manifest_path).st_mode & 0o777, 0o600)
            manifest = json.loads(receipt.manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["partitions"][0]["month"], month)
            self.assertEqual(manifest["partitions"][0]["raw_archive"], "raw/HISTDATA_COM_ASCII_EURUSD_T_201704.zip")
            self.assertTrue(validate_dataset(receipt.manifest_path).ok)

            self.assertEqual(
                [request.full_url for request in opener.requests], [page, acquisition_impl.HISTDATA_DOWNLOAD_ENDPOINT]
            )
            post = opener.requests[1]
            self.assertEqual(post.headers["Referer"], page)
            encoded = parse_qs(cast(bytes, post.data or b"").decode("ascii"), strict_parsing=True)
            self.assertEqual(encoded["date"], ["2017"])
            self.assertEqual(encoded["datemonth"], [month])
            self.assertEqual(encoded["platform"], ["ASCII"])
            self.assertEqual(encoded["timeframe"], ["T"])
            self.assertEqual(encoded["fxpair"], ["EURUSD"])

    def test_acquire_month_accepts_only_development_calendar_months(self) -> None:
        invalid = ((2015, 12), (2020, 1), (2017, 0), (2017, 13), (True, 1), (2017, False), (2017.0, 1))
        for year, month in invalid:
            with self.subTest(year=year, month=month), tempfile.TemporaryDirectory() as directory:
                with self.assertRaises(acquisition_impl.AcquisitionError):
                    acquisition_impl.acquire_month(year, month, data_root=Path(directory) / "market-data")  # type: ignore[arg-type]
                self.assertFalse((Path(directory) / "market-data").exists())

    def test_form_fields_are_bound_to_requested_month(self) -> None:
        month = "201812"
        page = f"{acquisition_impl.HISTDATA_MONTH_PAGE_PREFIX}/2018/12"
        archive = _archive_bytes(month)
        for field, value in (
            ("date", "2017"),
            ("datemonth", "201811"),
            ("platform", "CSV"),
            ("timeframe", "M"),
            ("fxpair", "GBPUSD"),
        ):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                opener = _FakeOpener(page, archive, {"date": "2018", "datemonth": "201812", field: value})
                with self.assertRaises(acquisition_impl.AcquisitionError):
                    acquisition_impl.acquire_month(
                        2018,
                        12,
                        data_root=Path(directory) / "market-data",
                        opener=cast(Any, opener),
                        terms_accepted=True,
                    )
                root = Path(directory) / "market-data"
                self.assertFalse((root / "raw/HISTDATA_COM_ASCII_EURUSD_T_201812.zip").exists())
                self.assertFalse((root / "manifests/histdata-eurusd-201812.json").exists())

    def test_archive_member_month_and_manifest_collision_are_fail_closed(self) -> None:
        month = "201906"
        page = f"{acquisition_impl.HISTDATA_MONTH_PAGE_PREFIX}/2019/6"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "market-data"
            wrong_archive = _archive_bytes(month, member_month="201905")
            opener = _FakeOpener(page, wrong_archive, {"date": "2019", "datemonth": "201906"})
            with self.assertRaises(acquisition_impl.AcquisitionError):
                acquisition_impl.acquire_month(2019, 6, data_root=root, opener=cast(Any, opener), terms_accepted=True)
            target = root / "raw/HISTDATA_COM_ASCII_EURUSD_T_201906.zip"
            self.assertTrue(target.is_file())
            self.assertEqual(target.read_bytes(), wrong_archive)
            self.assertFalse((root / "manifests/histdata-eurusd-201906.json").exists())

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "market-data"
            manifest_path = root / "manifests/histdata-eurusd-201906.json"
            manifest_path.parent.mkdir(parents=True)
            manifest_path.write_bytes(b"do not overwrite\n")
            os.chmod(root, 0o700)
            os.chmod(manifest_path.parent, 0o700)
            opener = _FakeOpener(page, _archive_bytes(month), {"date": "2019", "datemonth": "201906"})
            with self.assertRaises(acquisition_impl.AcquisitionError):
                acquisition_impl.acquire_month(2019, 6, data_root=root, opener=cast(Any, opener), terms_accepted=True)
            self.assertEqual(manifest_path.read_bytes(), b"do not overwrite\n")
            self.assertEqual(len(opener.requests), 0)

    def test_existing_archive_and_manifest_are_reused_without_network_or_overwrite(self) -> None:
        month = "201601"
        page = f"{acquisition_impl.HISTDATA_MONTH_PAGE_PREFIX}/2016/1"
        archive = _archive_bytes(month)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "market-data"
            first = acquisition_impl.acquire_month(
                2016,
                1,
                data_root=root,
                opener=cast(Any, _FakeOpener(page, archive, {"date": "2016", "datemonth": "201601"})),
                terms_accepted=True,
            )
            before_manifest = first.manifest_path.read_bytes()
            opener = _FakeOpener(page, b"not used")
            second = acquisition_impl.acquire_month(
                2016, 1, data_root=root, opener=cast(Any, opener), terms_accepted=True
            )
            self.assertEqual(second.status, "EXISTING")
            self.assertEqual(second.raw_sha256, first.raw_sha256)
            self.assertEqual(second.manifest_path.read_bytes(), before_manifest)
            self.assertEqual(opener.requests, [])

    def test_archive_size_gate_is_shared_with_explicit_month(self) -> None:
        month = "201912"
        page = f"{acquisition_impl.HISTDATA_MONTH_PAGE_PREFIX}/2019/12"
        archive = _archive_bytes(month)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "market-data"
            opener = _FakeOpener(page, archive, {"date": "2019", "datemonth": "201912"})
            with (
                mock.patch.object(acquisition_impl, "MAX_DATASET_BYTES", len(archive) - 1),
                self.assertRaisesRegex(acquisition_impl.AcquisitionError, "40 GiB"),
            ):
                acquisition_impl.acquire_month(2019, 12, data_root=root, opener=cast(Any, opener), terms_accepted=True)
            self.assertFalse((root / "manifests/histdata-eurusd-201912.json").exists())


if __name__ == "__main__":
    unittest.main()
