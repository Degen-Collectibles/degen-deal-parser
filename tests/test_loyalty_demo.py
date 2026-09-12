"""Real localhost browser walkthrough; no static HTML interception."""
import os
from pathlib import Path
import socket
import threading
import time
import pytest


@pytest.mark.skipif(os.environ.get('LOYALTY_TEST_UI')!='1',reason='Opt-in local-server Chromium walkthrough')
def test_live_synthetic_walkthrough():
    from playwright.sync_api import sync_playwright,expect
    from scripts.loyalty_demo import create_demo
    from sqlmodel import Session,select
    from app.models import AuditLog
    from app.loyalty.models import LoyaltyEntitlement
    import uvicorn
    root=Path(__file__).resolve().parents[1]
    scratch=Path(os.environ['LOYALTY_SYNTHETIC_ROOT'])/'live-demo'
    app=create_demo(scratch)
    bound=socket.socket();bound.bind(('127.0.0.1',0));bound.listen(128)
    port=bound.getsockname()[1];origin=f'http://127.0.0.1:{port}'
    server=uvicorn.Server(uvicorn.Config(app,host='127.0.0.1',port=port,proxy_headers=False,log_level='error'))
    thread=threading.Thread(target=lambda:server.run(sockets=[bound]),daemon=True);thread.start()
    deadline=time.monotonic()+15
    while not server.started and time.monotonic()<deadline:time.sleep(.05)
    assert server.started
    output=root/'docs/loyalty/verification';output.mkdir(exist_ok=True)
    try:
        with sync_playwright() as playwright:
            browser=playwright.chromium.launch(executable_path=r'C:\Users\jeffr\AppData\Local\ms-playwright\chromium-1234\chrome-win64\chrome.exe',headless=True)
            context=browser.new_context(viewport={'width':1440,'height':1100})
            context.route('**/*',lambda route:route.continue_() if route.request.url.startswith(origin+'/') else route.abort())
            page=context.new_page();page.set_default_timeout(10000);page.goto(origin+'/demo')
            expect(page.get_by_text('DEMO ONLY',exact=True)).to_be_visible()
            # No demo session exists until the chooser's real CSRF-protected POST.
            assert page.evaluate("fetch('/loyalty').then(r=>r.status)")==401
            def choose(role,sample):
                page.get_by_label('Demo role').select_option(role)
                page.get_by_label('Sample',exact=True).select_option(sample)
                page.get_by_role('button',name='Open example').click()
                frame=page.frame_locator('iframe')
                frame.get_by_role('heading',name='POS loyalty',exact=True).wait_for()
                return frame
            frame=choose('cashier','earned')
            expect(frame.get_by_text('20 points',exact=True).first).to_be_visible()
            expect(frame.get_by_text('$19.99',exact=True)).to_be_visible()
            assert frame.get_by_text('Administrator details',exact=True).count()==0
            expect(frame.get_by_placeholder('Internal ID, not order number')).to_be_visible()
            # Actual server enforces permissions despite a forged admin form.
            token=page.locator('input[name=csrf_token]').first.input_value()
            denied=page.evaluate("async token => (await fetch('/loyalty/replay',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body:new URLSearchParams({ids:'1',request_key:'cashier-denied',csrf_token:token})})).status",token)
            assert denied==403
            page.screenshot(path=str(output/'demo-cashier-desktop.png'),full_page=True,animations='disabled')
            frame=choose('cashier','refund')
            expect(frame.get_by_text('19 points',exact=True).first).to_be_visible()
            expect(frame.get_by_text('-1',exact=True)).to_be_visible()
            expect(frame.get_by_text('$19.49',exact=True)).to_be_visible()
            frame=choose('cashier','pending')
            expect(frame.get_by_text('No customer attached',exact=True)).to_be_visible()
            page.get_by_role('button',name='Simulate existing customer attachment').click()
            frame=page.frame_locator('iframe')
            expect(frame.get_by_text('20 points',exact=True).first).to_be_visible()
            expect(frame.get_by_text('Customer #103',exact=True)).to_be_visible()
            page.set_viewport_size({'width':390,'height':844})
            page.screenshot(path=str(output/'demo-cashier-mobile.png'),full_page=True,animations='disabled')
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
            frame=choose('admin','review')
            expect(frame.get_by_text('Needs review',exact=True).first).to_be_visible()
            expect(frame.get_by_text('20 points',exact=True).first).to_be_visible()
            frame.get_by_text('Administrator details',exact=True).click()
            expect(frame.get_by_text('Reason code: edit_or_exchange_review',exact=True)).to_be_visible()
            frame=choose('admin','earned')
            frame.get_by_text('Correct points',exact=True).click()
            frame.get_by_label('Correct order points').fill('19')
            frame.get_by_label('Case reference (no contact data)').fill('DEMO-case-1')
            frame.get_by_role('button',name='Record correction').click()
            expect(frame.get_by_text('Administrator correction; further changes paused',exact=True)).to_be_visible()
            with Session(app.state.demo_store.engine) as session:
                assert session.exec(select(AuditLog).where(AuditLog.action=='loyalty.correction')).one()
                assert session.exec(select(LoyaltyEntitlement).where(LoyaltyEntitlement.order_id=='gid://shopify/Order/1001')).one().posted_points==19
            # Real replay form -> durable inbox -> demo-only canonical processing.
            frame.get_by_label('Internal entitlement IDs (up to 25, comma separated)').fill('1')
            frame.get_by_role('button',name='Queue Shopify check',exact=True).click()
            page.get_by_role('button',name='Process queued demo checks').click()
            frame=page.frame_locator('iframe')
            expect(frame.get_by_text('19 points',exact=True).first).to_be_visible()
            expect(frame.get_by_text('Administrator correction; further changes paused',exact=True)).to_be_visible()
            page.set_viewport_size({'width':1440,'height':1100})
            page.screenshot(path=str(output/'demo-admin-desktop.png'),full_page=True,animations='disabled')
            # Host/origin boundary is verified by an actual browser request.
            assert page.evaluate("fetch('/demo',{method:'POST'}).then(r=>r.status)") in (403,405)
            context.close();browser.close()
    finally:
        server.should_exit=True;thread.join(timeout=10)
        bound.close();app.state.demo_restore()
    assert not thread.is_alive()
