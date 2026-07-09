from __future__ import annotations

import asyncio
import os
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest
from cryptography.fernet import Fernet
from fastapi import HTTPException
from openpyxl import Workbook
from starlette.requests import Request


def _set_test_env_default(key: str, value: str) -> None:
    if not os.environ.get(key):
        os.environ[key] = value


_set_test_env_default("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
_set_test_env_default("SESSION_SECRET", "financial-upload-test-session-xxxxxxxxxxxx")
_set_test_env_default("ADMIN_PASSWORD", "financial-upload-test-admin-password")
_set_test_env_default("EMPLOYEE_TOKEN_HMAC_KEY", "financial-upload-test-token-key")


from app.discord import bookkeeping as bookkeeping_module
from app.discord.bank_reconciliation import parse_bank_csv
from app.main import app
from app.routers import bookkeeping as bookkeeping_router


_FILE_LIMIT = bookkeeping_module.MAX_BOOKKEEPING_XLSX_COMPRESSED_BYTES
_REQUEST_LIMIT = _FILE_LIMIT + (512 * 1024)
_READ_CHUNK_LIMIT = 64 * 1024
_CSV_CELL_CHARACTER_LIMIT = 32_767
_CSV_CELL_BYTE_LIMIT = 64 * 1024


def _request(path: str) -> Request:
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": path,
            "headers": [],
        }
    )


class SyntheticAsyncUpload:
    def __init__(self, *, filename: str, size: int) -> None:
        self.filename = filename
        self.remaining = size
        self.read_sizes: list[int] = []
        self.bytes_returned = 0

    async def read(self, size: int = -1) -> bytes:
        self.read_sizes.append(size)
        if self.remaining <= 0:
            return b""
        amount = self.remaining if size < 0 else min(size, self.remaining)
        self.remaining -= amount
        self.bytes_returned += amount
        return b"x" * amount


@pytest.mark.parametrize(
    ("path", "route", "route_kwargs", "importer_name"),
    [
        (
            "/bookkeeping/import-form",
            bookkeeping_router.bookkeeping_import_form,
            {
                "show_label": "Security test",
                "show_date": None,
                "range_start": None,
                "range_end": None,
                "source_url": None,
            },
            "import_bookkeeping_file",
        ),
        (
            "/bookkeeping/bank/import-form",
            bookkeeping_router.bank_reconciliation_import_form,
            {
                "account_label": "Security test",
                "account_type": "checking",
            },
            "import_bank_statement_file",
        ),
    ],
)
def test_financial_upload_routes_stop_at_file_limit_plus_one(
    path: str,
    route,
    route_kwargs: dict[str, object],
    importer_name: str,
) -> None:
    upload = SyntheticAsyncUpload(
        filename="oversized.csv",
        size=_FILE_LIMIT + _READ_CHUNK_LIMIT,
    )

    with (
        patch.object(bookkeeping_router, "require_role_response", return_value=None),
        patch.object(bookkeeping_router, importer_name) as importer,
        pytest.raises(HTTPException) as exc_info,
    ):
        asyncio.run(
            route(
                _request(path),
                upload_file=upload,
                session=SimpleNamespace(),
                **route_kwargs,
            )
        )

    assert exc_info.value.status_code == 413
    assert upload.read_sizes
    assert all(0 < size <= _READ_CHUNK_LIMIT for size in upload.read_sizes)
    assert upload.bytes_returned == _FILE_LIMIT + 1
    assert upload.remaining == _READ_CHUNK_LIMIT - 1
    importer.assert_not_called()


@pytest.mark.parametrize(
    "path",
    [
        "/bookkeeping/import-form",
        "/bookkeeping/bank/import-form",
    ],
)
def test_financial_upload_paths_reject_oversized_raw_request_before_routing(
    path: str,
) -> None:
    async def request() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            return await client.post(
                path,
                content=b"x" * (_REQUEST_LIMIT + 1),
                headers={"content-type": "application/octet-stream"},
            )

    response = asyncio.run(request())

    assert response.status_code == 413
    assert response.json() == {"detail": "request_body_too_large"}


def _csv_with_rows(data_rows: int) -> bytes:
    return ("value\n" + ("x\n" * data_rows)).encode("utf-8")


def _csv_with_columns(columns: int, *, data_rows: int = 1) -> bytes:
    header = ",".join(f"column_{index}" for index in range(columns)) + "\n"
    row = ",".join("x" for _ in range(columns)) + "\n"
    return (header + (row * data_rows)).encode("utf-8")


def _single_column_csv_with_exact_size(size: int) -> bytes:
    header = b"value\n"
    full_row = (b"x" * _CSV_CELL_CHARACTER_LIMIT) + b"\n"
    full_rows, final_row_size = divmod(size - len(header), len(full_row))
    assert final_row_size >= 2
    return (
        header
        + (full_row * full_rows)
        + (b"x" * (final_row_size - 1))
        + b"\n"
    )


def _valid_xlsx_bytes() -> bytes:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Sheet 1"
    sheet.append(["column_1", "column_2"])
    sheet.append(["r0c0", "r0c1"])
    output = BytesIO()
    workbook.save(output)
    workbook.close()
    return output.getvalue()


def test_shared_csv_parser_rejects_file_over_upload_cap_before_decode() -> None:
    content = _single_column_csv_with_exact_size(_FILE_LIMIT + 1)
    assert len(content) == _FILE_LIMIT + 1

    with pytest.raises(
        bookkeeping_module.BookkeepingFileValidationError,
        match=f"^{bookkeeping_module.BOOKKEEPING_CSV_VALIDATION_ERROR}$",
    ):
        bookkeeping_module.read_bounded_csv_dict_rows(content)


@pytest.mark.parametrize(
    ("filename", "content"),
    [
        ("too-many-rows.csv", _csv_with_rows(10_001)),
        ("too-many-columns.csv", _csv_with_columns(65)),
        ("too-many-cells.csv", _csv_with_columns(64, data_rows=3_906)),
        ("cell-too-many-characters.csv", ("value\n" + ("x" * (_CSV_CELL_CHARACTER_LIMIT + 1)) + "\n").encode("utf-8")),
        ("cell-too-many-bytes.csv", ("value\n" + ("\U0001f600" * ((_CSV_CELL_BYTE_LIMIT // 4) + 1)) + "\n").encode("utf-8")),
    ],
    ids=[
        "rows",
        "columns",
        "cells",
        "cell-characters",
        "cell-bytes",
    ],
)
def test_bookkeeping_csv_rejects_unsafe_shape(filename: str, content: bytes) -> None:
    with pytest.raises(bookkeeping_module.BookkeepingFileValidationError):
        bookkeeping_module.read_tabular_rows(filename, content)


def test_bank_csv_uses_the_same_column_limit() -> None:
    headers = ["Posting Date", "Description", "Amount"] + [
        f"extra_{index}" for index in range(62)
    ]
    values = ["06/28/2026", "Valid transaction", "10.00"] + ["x"] * 62
    content = (",".join(headers) + "\n" + ",".join(values) + "\n").encode("utf-8")

    with pytest.raises(bookkeeping_module.BookkeepingFileValidationError):
        parse_bank_csv(content, account_label="Security test")


def test_valid_csv_and_xlsx_still_parse() -> None:
    csv_rows = bookkeeping_module.read_tabular_rows(
        "valid.csv",
        b"date,kind,amount\n2026-06-28,sale,50\n",
    )

    xlsx_rows = bookkeeping_module.read_tabular_rows(
        "valid.xlsx",
        _valid_xlsx_bytes(),
    )

    assert csv_rows == [
        {
            "__sheet_name": "import",
            "date": "2026-06-28",
            "kind": "sale",
            "amount": "50",
        }
    ]
    assert xlsx_rows == [
        {
            "__sheet_name": "Sheet 1",
            "column_1": "r0c0",
            "column_2": "r0c1",
        }
    ]
