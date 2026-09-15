#!/usr/bin/env python3
"""Thin command wrapper for approved HistData development-month acquisition."""

from mtf_lab.data.histdata_acquisition import (
    ARCHIVE_NAME,
    HISTDATA_DOWNLOAD_ENDPOINT,
    HISTDATA_MONTH_PAGE,
    HISTDATA_MONTH_PAGE_PREFIX,
    HISTDATA_TERMS_URI,
    MANIFEST_NAME,
    AcquisitionError,
    AcquisitionReceipt,
    acquire,
    acquire_march_2016,
    acquire_month,
    main,
)

__all__ = [
    "ARCHIVE_NAME",
    "AcquisitionError",
    "AcquisitionReceipt",
    "HISTDATA_DOWNLOAD_ENDPOINT",
    "HISTDATA_MONTH_PAGE",
    "HISTDATA_MONTH_PAGE_PREFIX",
    "HISTDATA_TERMS_URI",
    "MANIFEST_NAME",
    "acquire",
    "acquire_month",
    "acquire_march_2016",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
