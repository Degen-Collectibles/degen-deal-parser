from scripts import cardladder_cli, point130_cli


def test_build_130point_query_adds_slab_details():
    query = point130_cli.build_slab_query(
        "Umbreon VMAX Evolving Skies",
        grader="PSA",
        grade="10",
        card_number="215/203",
    )

    assert query == "Umbreon VMAX Evolving Skies 215/203 PSA 10"


def test_130point_records_from_nested_payload():
    records = point130_cli.records_from_payload(
        {
            "results": [
                {
                    "title": "Umbreon VMAX 215/203 Evolving Skies PSA 10",
                    "sold_price": "$4,550.00",
                    "sold_date": "2026-05-09",
                    "platform": "eBay",
                    "url": "https://www.ebay.com/itm/287293825532",
                }
            ]
        }
    )

    assert len(records) == 1
    assert records[0].title.startswith("Umbreon VMAX")
    assert records[0].price == 4550.0
    assert records[0].sold_date == "2026-05-09"
    assert records[0].platform == "eBay"


def test_130point_cache_uses_shared_comp_schema(tmp_path):
    cache_db = tmp_path / "point130.sqlite"
    record = cardladder_cli.CompRecord(
        title="Umbreon VMAX 215/203 Evolving Skies PSA 10",
        price=4550.0,
        sold_date="2026-05-09",
        platform="eBay",
        grader="PSA",
        grade="10",
    )

    saved = point130_cli.cache_records(cache_db, "Umbreon VMAX PSA 10", [record])
    records = point130_cli.load_cached_records(cache_db, text="Umbreon", grader="PSA", grade="10")

    assert saved == 1
    assert len(records) == 1
    assert records[0].price == 4550.0
