#!/usr/bin/env python3
"""Thin wrapper for the bounded Dukascopy EUR/USD 2016-03-07 pilot."""

from mtf_lab.data.dukascopy_acquisition import (
    DEFAULT_TIMEOUT_SECONDS,
    MANIFEST_NAME,
    RECEIPT_NAME,
    AcquisitionError,
    AcquisitionReceipt,
    acquire,
    acquire_week,
    main,
)

__all__ = [
    "AcquisitionError",
    "AcquisitionReceipt",
    "DEFAULT_TIMEOUT_SECONDS",
    "MANIFEST_NAME",
    "RECEIPT_NAME",
    "acquire",
    "acquire_week",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
