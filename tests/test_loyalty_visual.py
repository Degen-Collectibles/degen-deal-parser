"""Opt-in offline Chromium check using synthetic TestClient HTML and local assets."""
import os
from pathlib import Path
import pytest
from test_loyalty_ops import ops,login

@pytest.mark.skipif(os.environ.get('LOYALTY_TEST_UI')!='1',reason='Opt-in offline Chromium verification')
def test_offline_desktop_and_mobile(ops):
    from playwright.sync_api import sync_playwright
    c,store,uid,_=ops;login(c)
    html=c.get('/loyalty',params={'customer_id':'3'}).text
    root=Path(__file__).resolve().parents[1]
    output=root/'docs/loyalty/verification'
    output.mkdir(exist_ok=True)
    with sync_playwright() as p:
        browser=p.chromium.launch(headless=True)
        context=browser.new_context()
        def route(req):
            from urllib.parse import urlparse
            parsed=urlparse(req.request.url)
            if parsed.hostname!='loyalty.test':return req.abort()
            if parsed.path=='/loyalty':return req.fulfill(status=200,content_type='text/html',body=html)
            if parsed.path.startswith('/static/'):
                file=(root/'app'/parsed.path.lstrip('/')).resolve()
                if file.is_relative_to((root/'app/static').resolve()) and file.is_file():return req.fulfill(path=str(file))
            req.abort()
        page=context.new_page()
        # Install after the harness page guard: every request is fulfilled from
        # synthetic HTML/local assets or aborted; no network access is allowed.
        page.route('**/*',route)
        page.set_viewport_size({'width':1440,'height':1050})
        page.goto('https://loyalty.test/loyalty')
        assert page.get_by_text('20 points',exact=True).first.is_visible()
        assert page.get_by_text('$19.99',exact=True).is_visible()
        page.get_by_text('Administrator details',exact=True).click()
        assert page.get_by_text('Internal entitlement 1',exact=False).is_visible()
        page.get_by_text('Administrator details',exact=True).click()
        assert page.get_by_role('heading',name='Order entitlements').is_visible()
        page.screenshot(path=str(output/'desktop.png'),full_page=True,animations='disabled')
        page.set_viewport_size({'width':390,'height':844})
        assert page.get_by_role('heading',name='POS loyalty',exact=True).is_visible()
        page.screenshot(path=str(output/'mobile.png'),full_page=True,animations='disabled')
        assert page.evaluate('document.documentElement.scrollWidth <= window.innerWidth + 1'), page.evaluate("Array.from(document.querySelectorAll('body *')).filter(e=>e.getBoundingClientRect().right>innerWidth).slice(0,10).map(e=>[e.tagName,e.className,e.getBoundingClientRect().right])")
        from sqlmodel import Session
        from app.models import User,RolePermission
        with Session(store.engine) as session:
            user=session.get(User,uid);user.role='cashier';session.add(user)
            session.add(RolePermission(role='cashier',resource_key='ops.loyalty.view',is_allowed=True));session.commit()
        html=c.get('/loyalty?customer_id=3').text
        page.reload()
        assert page.get_by_text('Administrator details',exact=True).count()==0
        assert page.get_by_text('$19.99',exact=True).is_visible()
        point=page.locator('.orders .points').bounding_box()
        assert point['x']>=0 and point['x']+point['width']<=390
        page.screenshot(path=str(output/'cashier-mobile.png'),full_page=True,animations='disabled')
        page.set_viewport_size({'width':1440,'height':1050})
        assert page.locator('.linear-sidebar a[href="/loyalty"]').is_visible()
        page.screenshot(path=str(output/'cashier-desktop.png'),full_page=True,animations='disabled')
        context.close();browser.close()
