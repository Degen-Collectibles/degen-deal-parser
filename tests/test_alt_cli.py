from scripts import alt_cli


def test_build_slab_query_adds_grade_filters():
    query = alt_cli.build_slab_query(
        "Umbreon VMAX Evolving Skies",
        grader="PSA",
        grade="10",
        card_number="215/203",
    )

    assert query == "Umbreon VMAX Evolving Skies 215/203 PSA 10"


def test_record_from_typesense_hit_maps_sold_listing():
    record = alt_cli.record_from_hit(
        {
            "document": {
                "rawName": "Umbreon VMAX 215/203 Evolving Skies PSA 10",
                "price": 4550.0,
                "soldDate": "2026-05-09",
                "auctionHouse": "eBay",
                "auctionType": "BEST_OFFER",
                "gradingCompany": "PSA",
                "grade": "10",
                "url": "https://example.com/sale",
                "images": [{"url": "https://example.com/front.jpg"}],
            }
        }
    )

    assert record is not None
    assert record.title == "Umbreon VMAX 215/203 Evolving Skies PSA 10"
    assert record.price == 4550.0
    assert record.sold_date == "2026-05-09"
    assert record.platform == "eBay"
    assert record.grader == "PSA"
    assert record.grade == "10"
    assert record.image_url == "https://example.com/front.jpg"


def test_alt_cache_records_support_local_search(tmp_path):
    cache_db = tmp_path / "alt.sqlite"
    record = alt_cli.AltCompRecord(
        title="Umbreon VMAX 215/203 Evolving Skies PSA 10",
        price=4550.0,
        sold_date="2026-05-09",
        platform="eBay",
        grader="PSA",
        grade="10",
    )

    saved = alt_cli.cache_records(cache_db, "Umbreon VMAX Evolving Skies 215/203 PSA 10", [record])
    records = alt_cli.load_cached_records(cache_db, text="Umbreon Evolving", grader="PSA", grade="10")
    summary = alt_cli.summarize_records(records)

    assert saved == 1
    assert len(records) == 1
    assert records[0].title.startswith("Umbreon VMAX")
    assert summary["median"] == 4550.0
