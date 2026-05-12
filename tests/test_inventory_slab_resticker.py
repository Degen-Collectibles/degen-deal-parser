from datetime import datetime, timezone
import shutil
import asyncio
import uuid
from pathlib import Path

from sqlmodel import Session, SQLModel, create_engine, select

from app import inventory_pricing
from app.inventory import (
    _receive_slab_stock,
    _slab_comps_lookup_payload,
    _slab_grade_options,
    _slab_search_fallback_suggestion,
)
from app.inventory_price_updates import record_inventory_price_result
from app.inventory_pricing import (
    _alt_result_from_records,
    _card_ladder_result_from_payload,
    _fetch_card_ladder_cli_cache,
    _myslabs_sales_from_html,
    _pricecharting_product_url,
    _pricecharting_sales_from_html,
    apply_slab_resticker_alert,
    build_card_ladder_cli_query,
    build_card_ladder_slab_query,
    build_myslabs_query,
    clear_slab_resticker_alert,
    combine_slab_price_results,
    fetch_slab_price,
    import_card_ladder_cli_records_for_item,
    normalize_slab_price_source,
)
from app.models import InventoryItem, ITEM_TYPE_SLAB, PriceHistory
from scripts import alt_cli
from scripts import cardladder_cli


def test_card_ladder_payload_sales_drive_slab_market_price():
    result = _card_ladder_result_from_payload(
        "charizard expedition 40 psa 10",
        {
            "sales": [
                {
                    "title": "2002 Pokemon Expedition #40 Charizard PSA 10",
                    "soldPrice": "$1,000.00",
                    "soldDate": "2026-05-08",
                    "platform": "eBay",
                },
                {"title": "Charizard PSA 10", "price": "$1,100.00", "date": "2026-05-01"},
                {"title": "Charizard PSA 10", "price": "$1,300.00", "date": "2026-04-20"},
            ]
        },
        sales_url="https://app.cardladder.com/sales-history?q=test",
    )

    assert result is not None
    assert result["source"] == "card_ladder"
    assert result["market_price"] == 1050.0
    assert result["low_price"] == 1000.0
    assert result["high_price"] == 1300.0
    assert result["raw"]["sample_count"] == 3
    assert result["raw"]["sales"][0]["platform"] == "eBay"


def test_slab_resticker_alert_flags_meaningful_card_ladder_move():
    item = InventoryItem(
        barcode="DGN-000001",
        item_type=ITEM_TYPE_SLAB,
        game="Pokemon",
        card_name="Charizard",
        grading_company="PSA",
        grade="10",
        list_price=100.0,
    )

    event = apply_slab_resticker_alert(
        item,
        suggested_price=125.0,
        min_percent=10.0,
        min_dollars=10.0,
        source="card_ladder",
    )

    assert event == "created"
    assert item.resticker_alert_active is True
    assert item.resticker_reference_price == 100.0
    assert item.resticker_alert_price == 125.0
    assert "Card Ladder" in (item.resticker_alert_reason or "")

    item.list_price = 125.0
    clear_slab_resticker_alert(item, reason="resolved")
    assert item.resticker_alert_active is False
    assert item.resticker_resolved_at is not None


def test_build_card_ladder_slab_query_adds_grader_noise_exclusions():
    item = InventoryItem(
        barcode="DGN-000002",
        item_type=ITEM_TYPE_SLAB,
        game="Pokemon",
        card_name="Charizard",
        set_name="Expedition",
        card_number="40/165",
        grading_company="PSA",
        grade="10",
    )

    query = build_card_ladder_slab_query(item)

    assert query.startswith("Charizard Expedition 40/165 PSA 10")
    assert "-BGS" in query
    assert "-Autograph" in query
    assert "-(PSA 9)" in query


def test_card_ladder_cli_cache_result_uses_saved_comps(tmp_path):
    item = InventoryItem(
        barcode="PREVIEW",
        item_type=ITEM_TYPE_SLAB,
        game="Pokemon",
        card_name="Umbreon VMAX",
        set_name="Evolving Skies",
        card_number="215/203",
        grading_company="PSA",
        grade="10",
    )
    query = build_card_ladder_cli_query(item)
    cache_db = tmp_path / "cardladder.sqlite"

    saved = cardladder_cli.cache_records(
        cache_db,
        query,
        [
            cardladder_cli.CompRecord(
                title="Umbreon VMAX Alt Art PSA 10 #215",
                price=4550.0,
                sold_date="2026-05-09",
                platform="eBay",
                grader="PSA",
                grade="10",
                url="https://example.com/sale",
            ),
            cardladder_cli.CompRecord(
                title="Umbreon VMAX PSA 10",
                price=4300.0,
                sold_date="2026-05-01",
                platform="ALT",
                grader="PSA",
                grade="10",
            ),
        ],
    )

    result = _fetch_card_ladder_cli_cache(item, query=query, cache_path=cache_db)

    assert saved == 2
    assert result is not None
    assert result["source"] == "card_ladder"
    assert result["market_price"] == 4550.0
    assert result["raw"]["source_detail"] == "card_ladder_cli_cache"
    assert result["raw"]["sample_count"] == 2
    assert result["raw"]["sales"][0]["platform"] == "eBay"


def test_card_ladder_manual_import_writes_cli_cache(tmp_path):
    item = InventoryItem(
        barcode="PREVIEW",
        item_type=ITEM_TYPE_SLAB,
        game="Pokemon",
        card_name="Umbreon VMAX",
        set_name="Evolving Skies",
        card_number="215/203",
        grading_company="PSA",
        grade="10",
    )
    query = build_card_ladder_cli_query(item)
    cache_db = tmp_path / "manual-cardladder.sqlite"
    text = """
    Date Sold
    2026-05-09
    Type
    Fixed Price
    Price
    $4,550.00
    eBay
    Umbreon VMAX Alt Art PSA 10 #215
    """

    result = import_card_ladder_cli_records_for_item(
        item,
        text=text,
        query=query,
        cache_path=cache_db,
    )

    assert result["source"] == "card_ladder"
    assert result["market_price"] == 4550.0
    assert result["raw"]["source_detail"] == "card_ladder_manual_import"
    assert result["raw"]["imported_count"] == 1
    assert cache_db.exists()


def test_alt_records_drive_price_result():
    result = _alt_result_from_records(
        "Umbreon VMAX Evolving Skies 215/203 PSA 10",
        [
            alt_cli.AltCompRecord(
                title="Umbreon VMAX 215/203 PSA 10",
                price=4550.0,
                sold_date="2026-05-09",
                platform="eBay",
            ),
            alt_cli.AltCompRecord(
                title="Umbreon VMAX 215/203 PSA 10",
                price=4500.0,
                sold_date="2026-05-07",
                platform="eBay",
            ),
        ],
        source_detail="alt_typesense_live",
    )

    assert result is not None
    assert result["source"] == "alt"
    assert result["market_price"] == 4550.0
    assert result["raw"]["source_detail"] == "alt_typesense_live"
    assert result["raw"]["sample_count"] == 2
    assert result["raw"]["sales"][0]["platform"] == "eBay"


def test_multi_source_comps_dedupe_same_listing_and_tag_sources():
    item = InventoryItem(
        barcode="PREVIEW",
        item_type=ITEM_TYPE_SLAB,
        game="Pokemon",
        card_name="Umbreon VMAX",
        set_name="Evolving Skies",
        card_number="215/203",
        grading_company="PSA",
        grade="10",
    )

    result = combine_slab_price_results(
        item,
        [
            {
                "source": "alt",
                "market_price": 4550.0,
                "raw": {
                    "source_detail": "alt_typesense_live",
                    "sales": [
                        {
                            "sold_date": "2026-05-09",
                            "price": 4550.0,
                            "title": "Umbreon VMAX PSA 10",
                            "platform": "eBay",
                            "url": "https://www.ebay.com/itm/287293825532",
                        }
                    ],
                },
            },
            {
                "source": "130point",
                "market_price": 4550.0,
                "raw": {
                    "source_detail": "130point_cli_cache",
                    "sales": [
                        {
                            "sold_date": "2026-05-09",
                            "price": 4550.0,
                            "title": "Umbreon VMAX 215/203 Evolving Skies PSA 10",
                            "platform": "eBay",
                            "url": "https://www.ebay.com/itm/287293825532?hash=test",
                        }
                    ],
                },
            },
        ],
    )

    assert result is not None
    assert result["source"] == "slab_comps"
    assert result["market_price"] == 4550.0
    assert result["raw"]["sample_count"] == 1
    assert result["raw"]["sources"] == ["130point", "alt"]
    sale = result["raw"]["sales"][0]
    assert sale["sources"] == ["130point", "alt"]
    assert sale["source"] == "130point+alt"


def test_stale_slab_comps_use_latest_sale_instead_of_weighted_median(monkeypatch):
    monkeypatch.setattr(
        inventory_pricing,
        "utcnow",
        lambda: datetime(2026, 5, 11, tzinfo=timezone.utc),
    )
    item = InventoryItem(
        barcode="PREVIEW",
        item_type=ITEM_TYPE_SLAB,
        game="Pokemon",
        card_name="Umbreon VMAX",
        set_name="Evolving Skies",
        card_number="215/203",
        grading_company="PSA",
        grade="10",
    )

    result = combine_slab_price_results(
        item,
        [
            {
                "source": "alt",
                "market_price": 500.0,
                "raw": {
                    "source_detail": "alt_typesense_live",
                    "sales": [
                        {
                            "sold_date": "2026-03-25",
                            "price": 100.0,
                            "title": "Umbreon VMAX PSA 10 latest stale sale",
                            "platform": "eBay",
                            "url": "https://www.ebay.com/itm/100000000001",
                        },
                        {
                            "sold_date": "2026-03-10",
                            "price": 500.0,
                            "title": "Umbreon VMAX PSA 10 older sale",
                            "platform": "eBay",
                            "url": "https://www.ebay.com/itm/100000000002",
                        },
                        {
                            "sold_date": "2026-03-01",
                            "price": 600.0,
                            "title": "Umbreon VMAX PSA 10 oldest sale",
                            "platform": "eBay",
                            "url": "https://www.ebay.com/itm/100000000003",
                        },
                    ],
                },
            }
        ],
    )

    assert result is not None
    assert result["market_price"] == 100.0


def test_myslabs_archive_parser_normalizes_sold_rows():
    html = """
    <div class="slab_item">
        <a href="/slab/view/1452981/"><img class="lazy"
            data-src="https://cdn.myslabs.com/card.png?width=360&amp;height=610"
            alt="2021 Pokemon Evolving Skies Umbreon VMAX 215 PSA 10" /></a>
        <a href="/slab/view/1452981/" class="text-decoration-none">
            <div class="slab-title">
                2021 Pokemon Evolving Skies Umbreon VMAX 215 PSA 10
            </div>
        </a>
        <div class="slab-details">
            <div class="item-price">
                $2,222<i></i>
            </div>
            <small class="">
                Jan 6, 2025
            </small>
        </div>
    </div>
    <script type="application/ld+json">{}</script>
    """

    sales = _myslabs_sales_from_html(html)

    assert sales == [
        {
            "title": "2021 Pokemon Evolving Skies Umbreon VMAX 215 PSA 10",
            "price": 2222.0,
            "sold_date": "2025-01-06",
            "platform": "MySlabs",
            "sale_type": "",
            "url": "https://myslabs.com/slab/view/1452981/",
            "image_url": "https://cdn.myslabs.com/card.png?width=360&height=610",
            "sources": ["myslabs"],
            "source_details": ["myslabs_archive"],
        }
    ]


def test_build_myslabs_query_includes_slab_details():
    item = InventoryItem(
        barcode="PREVIEW",
        item_type=ITEM_TYPE_SLAB,
        game="Pokemon",
        card_name="Umbreon VMAX",
        set_name="Evolving Skies",
        card_number="215/203",
        grading_company="PSA",
        grade="10",
    )

    assert build_myslabs_query(item) == "Umbreon VMAX Evolving Skies 215 PSA 10"


def test_normalize_slab_price_source_aliases():
    assert normalize_slab_price_source("price charting") == "pricecharting"
    assert normalize_slab_price_source("cardladder") == "card_ladder"
    assert normalize_slab_price_source("ALT") == "alt"
    assert normalize_slab_price_source("unknown") == "all"


def test_fetch_slab_price_can_limit_to_pricecharting(monkeypatch):
    item = InventoryItem(
        barcode="PREVIEW",
        item_type=ITEM_TYPE_SLAB,
        game="Pokemon",
        card_name="Umbreon VMAX",
        set_name="Evolving Skies",
        card_number="215/203",
        grading_company="PSA",
        grade="10",
    )
    calls: list[str] = []

    monkeypatch.setattr(inventory_pricing, "_fetch_card_ladder_cli_cache", lambda *args, **kwargs: None)

    async def fake_source(name, result=None):
        async def _inner(*args, **kwargs):
            calls.append(name)
            return result

        return _inner

    async def fake_pricecharting(*args, **kwargs):
        calls.append("pricecharting")
        return {
            "source": "pricecharting",
            "market_price": 123.0,
            "raw": {
                "source_detail": "pricecharting",
                "sample_count": 1,
                "sales": [{"sold_date": "2026-05-01", "price": 123.0, "title": "Umbreon VMAX PSA 10"}],
            },
        }

    monkeypatch.setattr(inventory_pricing, "_fetch_card_ladder_price", asyncio.run(fake_source("card_ladder")))
    monkeypatch.setattr(inventory_pricing, "_fetch_alt_price", asyncio.run(fake_source("alt")))
    monkeypatch.setattr(inventory_pricing, "_fetch_130point_price", asyncio.run(fake_source("130point")))
    monkeypatch.setattr(inventory_pricing, "_fetch_myslabs_price", asyncio.run(fake_source("myslabs")))
    monkeypatch.setattr(inventory_pricing, "_fetch_pricecharting_price", fake_pricecharting)

    result = asyncio.run(fetch_slab_price(item, object(), source_filter="pricecharting"))

    assert result is not None
    assert result["source"] == "pricecharting"
    assert result["market_price"] == 123.0
    assert calls == ["pricecharting"]


def test_slab_grade_options_prioritizes_cert_grade():
    assert _slab_grade_options("PSA") == ("10", "9", "8", "7")
    assert _slab_grade_options("BGS", preferred_grade="8") == ("10", "9.5", "9", "8.5", "8")
    assert _slab_grade_options("PSA", preferred_grade="6") == ("6", "10", "9", "8", "7")


def test_slab_comps_payload_preserves_last_sold_rows():
    item = InventoryItem(
        barcode="PREVIEW",
        item_type=ITEM_TYPE_SLAB,
        game="Pokemon",
        card_name="Umbreon VMAX Alt Art",
        set_name="Evolving Skies",
        card_number="215/203",
        grading_company="PSA",
        grade="10",
    )

    payload = _slab_comps_lookup_payload(
        item,
        {
            "source": "card_ladder",
            "market_price": 1200.0,
            "raw": {
                "sample_count": 2,
                "sales": [
                    {
                        "date": "2026-05-01",
                        "price": 1250.0,
                        "title": "Umbreon VMAX PSA 10",
                        "platform": "eBay",
                    }
                ],
            },
        },
    )

    assert payload["card_name"] == "Umbreon VMAX Alt Art"
    assert payload["suggested_price"] == 1200.0
    assert payload["data_points"] == 2
    assert payload["last_solds"][0]["price"] == 1250.0


def test_slab_search_fallback_handles_common_nickname_when_card_api_times_out():
    card = _slab_search_fallback_suggestion("Umbreon Vmax Alt", game="Pokemon")

    assert card is not None
    assert card["name"] == "Umbreon VMAX"
    assert card["set_name"] == "Evolving Skies"
    assert card["card_number"] == "215/203"
    assert card["lookup_fallback"] is True


def test_pricecharting_fallback_parses_psa_10_sales():
    item = InventoryItem(
        barcode="PREVIEW",
        item_type=ITEM_TYPE_SLAB,
        game="Pokemon",
        card_name="Umbreon VMAX",
        set_name="Evolving Skies",
        card_number="215/203",
        grading_company="PSA",
        grade="10",
    )
    html = """
    <select id="completed-auctions-condition">
      <option value="completed-auctions-manual-only">PSA 10 (30)</option>
      <option value="completed-auctions-graded">Grade 9 (30)</option>
    </select>
    <div class="completed-auctions-manual-only">
      <table><tbody>
        <tr>
          <td class="date">2026-05-09</td>
          <td class="title"><a href="https://example.com/sale">Umbreon VMAX PSA 10 #215</a> [eBay]</td>
          <td class="numeric"><span class="js-price">$4,899.99</span></td>
        </tr>
      </tbody></table>
    </div>
    """

    assert _pricecharting_product_url(item).endswith("/pokemon-evolving-skies/umbreon-vmax-215")
    sales = _pricecharting_sales_from_html(html, item)
    assert sales == [
        {
            "title": "Umbreon VMAX PSA 10 #215",
            "price": 4899.99,
            "sold_date": "2026-05-09",
            "platform": "eBay",
            "sale_type": "",
            "url": "https://example.com/sale",
        }
    ]


def test_receive_slab_stock_records_price_history():
    temp_dir = Path.cwd() / "tests" / ".tmp_slab_resticker" / str(uuid.uuid4())
    temp_dir.mkdir(parents=True, exist_ok=True)
    engine = create_engine(
        f"sqlite:///{(temp_dir / 'inventory.db').as_posix()}",
        connect_args={"check_same_thread": False},
    )
    SQLModel.metadata.create_all(engine)
    try:
        with Session(engine) as session:
            item, movement, created = _receive_slab_stock(
                session,
                game="Pokemon",
                card_name="Charizard",
                set_name="Expedition",
                card_number="40/165",
                grading_company="PSA",
                grade="10",
                cert_number="12345678",
                quantity=1,
                unit_cost=700.0,
                list_price=1050.0,
                auto_price=1050.0,
                price_payload={"source": "card_ladder", "sales": [{"price": 1050.0}]},
                actor_label="counter",
            )

            assert created is True
            assert item.item_type == ITEM_TYPE_SLAB
            assert item.cert_number == "12345678"
            assert item.barcode.startswith("DGN-")
            assert movement.quantity_after == 1
            history = session.exec(select(PriceHistory).where(PriceHistory.item_id == item.id)).one()
            assert history.source == "card_ladder"
            assert history.market_price == 1050.0
    finally:
        engine.dispose()
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_record_inventory_price_result_sets_resticker_alert_without_notifications():
    temp_dir = Path.cwd() / "tests" / ".tmp_slab_price_update" / str(uuid.uuid4())
    temp_dir.mkdir(parents=True, exist_ok=True)
    engine = create_engine(
        f"sqlite:///{(temp_dir / 'inventory.db').as_posix()}",
        connect_args={"check_same_thread": False},
    )
    SQLModel.metadata.create_all(engine)
    try:
        with Session(engine) as session:
            item = InventoryItem(
                barcode="DGN-000003",
                item_type=ITEM_TYPE_SLAB,
                game="Pokemon",
                card_name="Charizard",
                grading_company="PSA",
                grade="10",
                list_price=1000.0,
            )
            session.add(item)
            session.commit()
            session.refresh(item)

            _history, event = record_inventory_price_result(
                session,
                item,
                {
                    "source": "card_ladder",
                    "market_price": 1200.0,
                    "low_price": 1100.0,
                    "high_price": 1300.0,
                    "raw": {"sales": [{"price": 1200.0}]},
                },
                notify=False,
            )
            session.commit()
            session.refresh(item)

            assert event == "created"
            assert item.resticker_alert_active is True
            assert item.resticker_alert_price == 1200.0
            assert item.auto_price == 1200.0
    finally:
        engine.dispose()
        shutil.rmtree(temp_dir, ignore_errors=True)
