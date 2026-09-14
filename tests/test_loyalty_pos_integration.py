"""Actual application middleware integration under the isolated app harness."""
from fastapi.testclient import TestClient
from uuid import uuid4
from app.loyalty.schema import install_schema
from test_loyalty_pos import configuration, add_entry, token, PATH


def test_actual_app_bearer_route_bypasses_ops_cookie_lookup(monkeypatch):
    from app import main
    # The app harness may reuse its SQLite file across full-suite runs.
    # Keep this fixture in its own shop scope without deleting audit rows.
    shop=f'synthetic-{uuid4().hex}.myshopify.com'
    values=configuration(loyalty_shop_domain=shop)
    for key in ('loyalty_shop_domain','loyalty_shop_id','loyalty_pos_read_enabled','loyalty_pos_all_staff_enabled','loyalty_pos_client_id','loyalty_pos_client_secret'):
        monkeypatch.setattr(main.settings,key,getattr(values,key))
    def forbidden(*args,**kwargs):raise AssertionError('POS reads must not resolve an Ops cookie')
    monkeypatch.setattr(main,'get_request_user',forbidden)
    install_schema(main.engine)
    add_entry(main.engine,20,customer='991',shop=shop)
    client=TestClient(main.app)  # Deliberately no lifespan/background workers.
    path=PATH.replace('101','991')
    assert client.get(path,headers={'Cookie':'session=synthetic'}).status_code==401
    response=client.get(path,headers={'Authorization':'Bearer '+token(dest=f'https://{shop}',iss=f'https://{shop}/admin')})
    assert response.status_code==200
    assert response.json()['balance_points']=='20'
    assert 'set-cookie' not in response.headers
