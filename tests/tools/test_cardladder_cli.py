import json

import pytest

from scripts import cardladder_cli


def test_build_slab_query_adds_grader_grade_and_noise_exclusions():
    query = cardladder_cli.build_slab_query(
        "1986 Fleer Jordan",
        grader="PSA",
        grade="9",
        strict=True,
    )

    assert query.startswith("1986 Fleer Jordan PSA 9")
    assert "-BGS" in query
    assert "-(PSA 10)" in query
    assert "-(PSA 8)" in query
    assert "-Autograph" in query


def test_build_slab_query_can_use_cert_only():
    query = cardladder_cli.build_slab_query(cert="12345678", grader="PSA", strict=False)

    assert query == "PSA 12345678"


def test_walk_json_records_finds_nested_sale_records():
    payload = {
        "data": {
            "items": [
                {
                    "title": "1986 Fleer Michael Jordan PSA 9",
                    "soldDate": "2026-05-01",
                    "soldPrice": "$10,500.00",
                    "platform": "eBay",
                    "grader": "PSA",
                    "grade": "9",
                },
                {"title": "Not a sale"},
            ]
        }
    }

    records = cardladder_cli.walk_json_records(payload)

    assert len(records) == 1
    assert records[0].title == "1986 Fleer Michael Jordan PSA 9"
    assert records[0].price == 10500.0
    assert records[0].sold_date == "2026-05-01"
    assert records[0].platform == "eBay"


def test_record_from_dom_text_parses_price_and_date():
    record = cardladder_cli.record_from_text(
        "1986 Fleer Michael Jordan PSA 9\n"
        "May 1, 2026\n"
        "$10,500.00\n"
        "eBay"
    )

    assert record is not None
    assert record.title == "1986 Fleer Michael Jordan PSA 9"
    assert record.price == 10500.0
    assert record.sold_date == "May 1, 2026"


def test_record_from_cardladder_dom_text_parses_sale_card_layout():
    record = cardladder_cli.record_from_text(
        "EBAY - PROBSTEIN123\n"
        "2002 Pokemon Expedition #40 Charizard PSA 10 GEM MINT\n"
        "Date Sold\n"
        "May 8, 2026\n"
        "Type\n"
        "Fixed Price\n"
        "Price\n"
        "$1,500.00"
    )

    assert record is not None
    assert record.platform == "EBAY - PROBSTEIN123"
    assert record.title == "2002 Pokemon Expedition #40 Charizard PSA 10 GEM MINT"
    assert record.sold_date == "May 8, 2026"
    assert record.sale_type == "Fixed Price"
    assert record.price == 1500.0


def test_json_output_shape_excludes_raw_by_default():
    record = cardladder_cli.CompRecord(
        title="Test Card",
        price=25.0,
        sold_date="2026-05-01",
        raw={"secret": "raw"},
    )

    encoded = json.dumps(record.as_dict())

    assert "Test Card" in encoded
    assert "secret" not in encoded


def test_cache_records_supports_local_search_and_summary(tmp_path):
    cache_db = tmp_path / "cardladder.sqlite"
    record = cardladder_cli.CompRecord(
        title="1986 Fleer Michael Jordan PSA 9",
        price=10500.0,
        sold_date="2026-05-01",
        platform="eBay",
        grader="PSA",
        grade="9",
        raw={"source": "test"},
    )

    saved = cardladder_cli.cache_records(cache_db, "1986 Fleer Jordan PSA 9", [record])
    records = cardladder_cli.load_cached_records(cache_db, text="Jordan", grader="PSA", grade="9")
    summary = cardladder_cli.summarize_records(records)

    assert saved == 1
    assert len(records) == 1
    assert records[0].title == "1986 Fleer Michael Jordan PSA 9"
    assert summary["median"] == 10500.0
    assert summary["low"] == 10500.0


def test_sql_rows_is_read_only(tmp_path):
    cache_db = tmp_path / "cardladder.sqlite"
    cardladder_cli.cache_records(
        cache_db,
        "test query",
        [cardladder_cli.CompRecord(title="Test Card", price=25.0, sold_date="2026-05-01")],
    )

    columns, rows = cardladder_cli.sql_rows(cache_db, "select title, price from comps")

    assert columns == ["title", "price"]
    assert rows[0]["title"] == "Test Card"
    with pytest.raises(RuntimeError):
        cardladder_cli.sql_rows(cache_db, "delete from comps")


def test_sales_history_url_uses_cardladder_query_params():
    url = cardladder_cli.sales_history_url("charizard psa 10", sort="price", direction="asc")

    assert url.startswith("https://app.cardladder.com/sales-history?")
    assert "sort=price" in url
    assert "direction=asc" in url
    assert "q=charizard+psa+10" in url
