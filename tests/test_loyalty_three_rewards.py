"""Three approved point rewards and the 20% cap: terms, register API, and the table rule upgrade."""
import pytest
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import CreateTable

from app.loyalty.models import LoyaltyRedemption
from app.loyalty.redemption import Redemptions, RedemptionError
from app.loyalty.redemption_shopify import discount_input
from app.loyalty.reward_policy import Reward
from app.loyalty.schema import MONEY_RULE, install_schema, normalized_check, schema_ready
from test_loyalty_pos import add_entry, token
from test_loyalty_redemption import CUSTOMER, SALE, SHOP, funded
from test_loyalty_redemption_api import PATH, client
from test_loyalty_service import store  # noqa: F401  (fixture)

VARIANT = 'gid://shopify/ProductVariant/11'
OLD_RULE = 'discount_cents > 0 AND minimum_cents >= discount_cents * 10'


def test_cap_is_twenty_percent_of_the_minimum():
    assert Reward(500, 1000, 5000).minimum_cents == 5000
    with pytest.raises(ValueError, match='reward_exceeds_twenty_percent'):
        Reward(500, 1000, 4999)
    for reward in Reward.approved():
        assert reward.discount_cents * 5 == reward.minimum_cents


@pytest.mark.parametrize('points,discount,minimum', [(250, 500, 2500), (500, 1000, 5000), (1000, 2500, 12500)])
def test_each_reward_holds_its_own_points_and_terms(store, points, discount, minimum):
    rewards = funded(store)  # 1,000 points
    row = rewards.reserve(SHOP, CUSTOMER, 'a' * 64, 'request_key_00001', Reward.for_points(points), now=SALE)
    assert (row.points, row.discount_cents, row.minimum_cents) == (points, discount, minimum)
    assert rewards.overview(SHOP, CUSTOMER)['reserved_points'] == points
    assert rewards.overview(SHOP, CUSTOMER)['available_points'] == 1000 - points
    row.eligibility_json = '{"requested":["%s"],"eligible":["%s"]}' % (VARIANT, VARIANT)
    data = discount_input(row)
    assert data['customerGets']['value']['discountAmount']['amount'] == '%d.00' % (discount // 100)
    assert data['minimumRequirement']['subtotal']['greaterThanOrEqualToSubtotal'] == '%d.00' % (minimum // 100)


def test_same_request_key_cannot_switch_to_another_reward(store):
    rewards = funded(store)
    first = rewards.reserve(SHOP, CUSTOMER, 'a' * 64, 'request_key_00001', Reward.for_points(250), now=SALE)
    assert rewards.reserve(SHOP, CUSTOMER, 'a' * 64, 'request_key_00001', Reward.for_points(250), now=SALE).id == first.id
    with pytest.raises(RedemptionError, match='request_key_conflict'):
        rewards.reserve(SHOP, CUSTOMER, 'a' * 64, 'request_key_00001', Reward.for_points(500), now=SALE)
    assert rewards.overview(SHOP, CUSTOMER)['reserved_points'] == 250


def test_a_larger_reward_needs_enough_points(store):
    add_entry(store.engine, 600)
    rewards = Redemptions(store)
    customer = 'gid://shopify/Customer/101'
    from test_loyalty_pos import SHOP as POS_SHOP
    with pytest.raises(RedemptionError, match='insufficient_points'):
        rewards.reserve(POS_SHOP, customer, 'a' * 64, 'request_key_00001', Reward.for_points(1000))
    assert rewards.reserve(POS_SHOP, customer, 'a' * 64, 'request_key_00002', Reward.for_points(500)).points == 500


def test_register_api_lists_three_rewards_and_reserves_the_chosen_one(store):
    add_entry(store.engine, 1000)
    c = client(store)
    headers = {'Authorization': 'Bearer ' + token()}
    listed = c.get(PATH, headers=headers).json()
    assert 'reward' not in listed
    assert [(r['points'], r['discount_cents'], r['minimum_cents']) for r in listed['rewards']] == [
        (250, 500, 2500), (500, 1000, 5000), (1000, 2500, 12500)]
    base = {'action': 'reserve', 'request_key': 'three_rewards_0001', 'variant_ids': [VARIANT]}
    # The register may only name an approved point cost; it can never send its own terms.
    for bad in (750, 0, -250, '250', 250.0, True, None):
        assert c.post(PATH, headers=headers, json=dict(base, reward_points=bad)).status_code == 400
    assert c.post(PATH, headers=headers, json=base).status_code == 400
    assert c.post(PATH, headers=headers, json=dict(base, reward_points=250, discount_cents=2500)).status_code == 400
    assert c.get(PATH, headers=headers).json()['reserved_points'] == 0
    made = c.post(PATH, headers=headers, json=dict(base, reward_points=250))
    assert made.status_code == 200, made.text
    assert (made.json()['points'], made.json()['discount_cents'], made.json()['minimum_cents']) == (250, 500, 2500)
    after = c.get(PATH, headers=headers).json()
    assert (after['available_points'], after['reserved_points']) == (750, 250)
    assert (after['pending']['points'], after['pending']['discount_cents'], after['pending']['minimum_cents']) == (250, 500, 2500)
    # Same checkout asking for a different reward is refused, and holds nothing more.
    assert c.post(PATH, headers=headers, json=dict(base, reward_points=500)).status_code == 409
    assert c.get(PATH, headers=headers).json()['reserved_points'] == 250


def money_rule(engine):
    return {c['name']: c['sqltext'] for c in inspect(engine).get_check_constraints('loyalty_redemption')}[MONEY_RULE]


def downgrade_to_ten_percent_rule(engine):
    """Recreate the table as the first release made it, with the 10% rule."""
    with engine.begin() as c:
        if engine.dialect.name == 'postgresql':
            c.execute(text(f'ALTER TABLE loyalty_redemption DROP CONSTRAINT {MONEY_RULE}, '
                           f'ADD CONSTRAINT {MONEY_RULE} CHECK ({OLD_RULE})'))
        else:
            ddl = str(CreateTable(LoyaltyRedemption.__table__).compile(engine))
            assert ddl.count('discount_cents * 5') == 1
            c.execute(text('DROP TABLE loyalty_redemption'))
            c.execute(text(ddl.replace('discount_cents * 5', 'discount_cents * 10')))
            for index in LoyaltyRedemption.__table__.indexes:
                index.create(c)
    assert normalized_check(money_rule(engine)) == normalized_check(OLD_RULE)


def insert(engine, key, discount, minimum):
    with engine.begin() as c:
        account = c.execute(text('SELECT id FROM loyalty_account LIMIT 1')).scalar()
        c.execute(text(
            "INSERT INTO loyalty_redemption (shop, account_id, customer_id, request_key, session_hash, code, state, points,"
            " discount_cents, minimum_cents, rule_version, eligibility_json, evidence_json, observed_order_json,"
            " manual_decision_hash, work_generation, acknowledged_generation, review_cause, resolved_evidence_hash,"
            " failure_count, reason, created_at, expires_at)"
            " VALUES (:shop, :account, :customer, :key, :session, :code, 'cancelled', 500, :discount, :minimum, 'test',"
            " '{}', '{}', '{}', '', 0, 0, '', '', 0, '', :at, :at)"),
            dict(shop=SHOP, account=account, customer=CUSTOMER, key=key, session='a' * 64, code='DEGEN-' + key,
                 discount=discount, minimum=minimum, at=SALE.replace(tzinfo=None)))


def test_existing_table_is_upgraded_from_the_ten_percent_rule_and_keeps_its_rows(store):
    funded(store)
    engine = store.engine
    downgrade_to_ten_percent_rule(engine)
    assert not schema_ready(engine)
    insert(engine, 'old_row_0001', 1000, 10000)
    with pytest.raises(IntegrityError):
        insert(engine, 'too_big_0001', 1000, 5000)  # 20% is refused under the old rule

    install_schema(engine)

    assert schema_ready(engine)
    assert normalized_check(money_rule(engine)) == normalized_check('discount_cents > 0 AND minimum_cents >= discount_cents * 5')
    with engine.connect() as c:
        assert c.execute(text("SELECT discount_cents, minimum_cents FROM loyalty_redemption WHERE request_key='old_row_0001'")).one() == (1000, 10000)
        assert 'loyalty_redemption_old' not in inspect(c).get_table_names()
    insert(engine, 'new_row_0001', 1000, 5000)  # exactly 20% is now allowed
    with pytest.raises(IntegrityError):
        insert(engine, 'over_cap_0001', 1000, 4999)
    # Running the installer again changes nothing.
    install_schema(engine)
    assert schema_ready(engine)
    with engine.connect() as c:
        assert c.execute(text('SELECT count(*) FROM loyalty_redemption')).scalar() == 2
