import {render, screen, fireEvent, waitFor, cleanup, act} from '@testing-library/preact';
import {afterEach, expect, test, vi} from 'vitest';
import {LoyaltyView, type ContextApi} from '../src/LoyaltyView';

const body = (id='101', points='19', next:string|null=null) => ({customer_id:id,balance_points:points,has_history:true,as_of:'2026-09-10T12:00:00Z',checked_at:null as string|null,needs_review:false,updates_pending:false,posting_paused:false,next_cursor:next,history:[{delta_points:'-1',label:'Refund adjustment',created_at:'2026-09-10T12:00:00Z'}]});
function fixture() {
  const listeners=new Set<()=>void>();
  const subscribe=(fn:()=>void)=>{listeners.add(fn);return ()=>{listeners.delete(fn);};};
  const api = {connectivity:{current:{value:{internetConnected:'Connected'},subscribe}},customer:{id:101},session:{currentSession:{shopId:1,shopDomain:'synthetic.myshopify.com',userId:42,locationId:1},staffMember:{value:{id:7},subscribe},getSessionToken:vi.fn(async()=> 'synthetic-token')},action:{presentModal:vi.fn()}};
  return {api:api as unknown as ContextApi, change(){for(const listener of [...listeners])listener();}};
}
const reply = (data=body()) => Promise.resolve(new Response(JSON.stringify(data),{status:200}));
afterEach(()=>{cleanup();vi.unstubAllGlobals();});

test('block reads native customer, exact points, fresh bearer without cookies, opens native modal',async()=>{
  const {api}=fixture(); const fetcher=vi.fn((_url:string,_options:RequestInit)=>reply(body('101','9007199254740993')));vi.stubGlobal('fetch',fetcher);
  render(<LoyaltyView api={api} mode="block"/>);
  expect(await screen.findByText('9,007,199,254,740,993 points')).toBeTruthy();
  expect(fetcher.mock.calls[0][0]).toBe('/api/loyalty/pos/customers/101?limit=10');
  expect(fetcher.mock.calls[0][1]).toMatchObject({credentials:'omit',cache:'no-store',headers:{Authorization:'Bearer synthetic-token'}});
  fireEvent.click(screen.getByText('View history'));expect(api.action?.presentModal).toHaveBeenCalledOnce();
  expect(screen.queryByText('Refund adjustment')).toBeNull();
});
test('modal paginates bounded history and distinguishes review and paused posting',async()=>{
  const {api}=fixture();const first={...body('101','19','signed-cursor'),needs_review:true,posting_paused:true};
  const second={...body(),history:[{delta_points:'20',label:'Purchase points',created_at:'2026-09-09T12:00:00Z'}]};
  const fetcher=vi.fn().mockImplementationOnce(()=>reply(first)).mockImplementationOnce(()=>reply(second));vi.stubGlobal('fetch',fetcher);
  render(<LoyaltyView api={api} mode="history"/>);
  expect(await screen.findByText('Refund adjustment')).toBeTruthy();
  expect(screen.getByText('Order needs review · Updates paused')).toBeTruthy();
  expect(screen.getByText('Ask an Ops administrator to review this customer’s order.')).toBeTruthy();
  fireEvent.click(screen.getByText('Older activity'));
  expect(await screen.findByText('Purchase points')).toBeTruthy();expect(screen.queryByText('Refund adjustment')).toBeNull();
  expect(fetcher.mock.calls[1][0]).toContain('cursor=signed-cursor');
  expect(api.session.getSessionToken).toHaveBeenCalledTimes(2);
});

test.each([401,403,409,429])('refresh recovers from HTTP %s with fresh authorization and no stale history',async(status)=>{
  const {api}=fixture();
  const fetcher=vi.fn().mockImplementationOnce(()=>reply(body('101','19','old-cursor')))
    .mockResolvedValueOnce(new Response('{}',{status}))
    .mockImplementationOnce(()=>reply(body('101','21')));
  vi.stubGlobal('fetch',fetcher);
  render(<LoyaltyView api={api} mode="history"/>);await screen.findByText('19 points');
  fireEvent.click(screen.getByText('Older activity'));
  await screen.findByText(status===409?'Activity changed. Refresh to see the latest points.':status===429?'Please wait a minute, then retry.':'Loyalty access is unavailable.');
  expect(screen.queryByText('19 points')).toBeNull();expect(screen.queryByText('Refund adjustment')).toBeNull();
  expect(screen.queryByText('0 points')).toBeNull();
  fireEvent.click(screen.getByText('Refresh'));await screen.findByText('21 points');
  expect(fetcher.mock.calls[2][0]).not.toContain('cursor=');
  expect(api.session.getSessionToken).toHaveBeenCalledTimes(3);
});

test('customer switch while authentication is pending never sends the old customer request',async()=>{
  const {api,change}=fixture();let finish!:(token:string)=>void;
  vi.mocked(api.session.getSessionToken).mockImplementationOnce(()=>new Promise(resolve=>{finish=resolve;}));
  const fetcher=vi.fn((_url:string)=>reply(body('102','7')));vi.stubGlobal('fetch',fetcher);
  render(<LoyaltyView api={api} mode="history"/>);
  await waitFor(()=>expect(api.session.getSessionToken).toHaveBeenCalledOnce());
  api.customer.id=102;act(()=>change());await screen.findByText('7 points');
  await act(async()=>finish('old-synthetic-token'));
  expect(fetcher).toHaveBeenCalledOnce();expect(fetcher.mock.calls[0]?.[0]).toContain('/customers/102?');
});
test('no history is explicit, never confused with unavailable authorization',async()=>{
  const {api}=fixture();vi.stubGlobal('fetch',()=>reply({...body('101','0'),has_history:false,history:[]}));
  render(<LoyaltyView api={api} mode="history"/>);expect(await screen.findByText('No posted points yet.')).toBeTruthy();
  cleanup();vi.mocked(api.session.getSessionToken).mockResolvedValue(undefined);
  render(<LoyaltyView api={api} mode="history"/>);expect(await screen.findByText('Loyalty access is unavailable.')).toBeTruthy();
  expect(screen.queryByText('0 points')).toBeNull();
});
test.each([401,403,429,503,409])('HTTP %s clears protected content instead of showing zero',async(status)=>{
  const {api}=fixture();vi.stubGlobal('fetch',()=>Promise.resolve(new Response('{}',{status})));
  render(<LoyaltyView api={api} mode="history"/>);
  await screen.findByText(status===409?'Activity changed. Refresh to see the latest points.':status===429?'Please wait a minute, then retry.':status===401||status===403?'Loyalty access is unavailable.':'Loyalty is unavailable. Try again when connected.');
  expect(screen.queryByText('0 points')).toBeNull();
});
test('customer and PIN changes discard late responses, without treating PIN as authorization',async()=>{
  const {api,change}=fixture();let finish!:(value:Response)=>void;
  const fetcher=vi.fn().mockImplementationOnce(()=>new Promise<Response>(resolve=>{finish=resolve;})).mockImplementation(()=>reply(body('102','7')));vi.stubGlobal('fetch',fetcher);
  render(<LoyaltyView api={api} mode="history"/>);await waitFor(()=>expect(fetcher).toHaveBeenCalledOnce());
  api.customer.id=102;act(()=>change());
  expect(await screen.findByText('7 points')).toBeTruthy();
  await act(async()=>finish(new Response(JSON.stringify(body('101','99')))));
  expect(screen.queryByText('99 points')).toBeNull();
  (api.session.staffMember as {value:{id:number}}).value={id:8};act(()=>change());await waitFor(()=>expect(fetcher).toHaveBeenCalledTimes(3));
  expect(fetcher.mock.calls[2][0]).not.toContain('staff');
});
test('unsafe customer or mismatched response never displays another balance',async()=>{
  const {api}=fixture();api.customer.id=Number.MAX_SAFE_INTEGER+1;const fetcher=vi.fn(()=>reply());vi.stubGlobal('fetch',fetcher);
  render(<LoyaltyView api={api} mode="history"/>);expect(await screen.findByText('Select an existing customer in POS.')).toBeTruthy();expect(fetcher).not.toHaveBeenCalled();
  cleanup();api.customer.id=102;render(<LoyaltyView api={api} mode="history"/>);
  await screen.findByText('Loyalty is unavailable. Try again when connected.');expect(screen.queryByText('19 points')).toBeNull();
});

test('a token request that never resolves has a bounded unavailable state',async()=>{
  vi.useFakeTimers();
  try {
    const {api}=fixture();vi.mocked(api.session.getSessionToken).mockImplementation(()=>new Promise(()=>{}));
    render(<LoyaltyView api={api} mode="block"/>);
    await act(async()=>{await vi.advanceTimersByTimeAsync(10001);});
    expect(screen.queryByText('Loyalty is unavailable. Try again when connected.')).toBeTruthy();
  } finally {vi.useRealTimers();}
});


test('native connectivity loss clears an already displayed balance without a request',async()=>{
  const {api,change}=fixture();const fetcher=vi.fn(()=>reply());vi.stubGlobal('fetch',fetcher);
  render(<LoyaltyView api={api} mode="history"/>);await screen.findByText('19 points');
  (api as unknown as {connectivity:{current:{value:{internetConnected:string}}}}).connectivity.current.value.internetConnected='Disconnected';
  act(()=>change());await screen.findByText('Loyalty is unavailable. Try again when connected.');
  expect(screen.queryByText('19 points')).toBeNull();expect(fetcher).toHaveBeenCalledOnce();
});


test('compact block keeps one native action and one timestamp without technical order-check text',async()=>{
  const {api}=fixture();vi.stubGlobal('fetch',()=>reply({...body(),checked_at:'2026-09-09T12:00:00Z',needs_review:true,updates_pending:true,posting_paused:true}));
  const {container}=render(<LoyaltyView api={api} mode="block"/>);
  await screen.findByText('19 points');
  expect(screen.getByText('Order needs review · Updates pending · Updates paused')).toBeTruthy();
  expect(screen.getAllByText(/^As of /)).toHaveLength(1);
  expect(screen.queryByText(/order check|Orders last checked|Balance read/i)).toBeNull();
  expect(screen.queryByText('Refresh')).toBeNull();
  expect(container.querySelectorAll('s-button')).toHaveLength(1);
  const action=screen.getByText('View history');
  expect(action.parentElement?.tagName).toBe('S-POS-BLOCK');
  expect(action.getAttribute('slot')).toBe('secondary-actions');
  expect(container.querySelector('s-heading')).toBeNull();
});

test('history has padded content, compact activity, and refresh in the native action bar',async()=>{
  const {api}=fixture();const fetcher=vi.fn((_url:string)=>reply({...body('101','19','cursor'),history:[
    {delta_points:'-1',label:'Refund adjustment',created_at:'2026-09-10T12:00:00Z'},
    {delta_points:'20',label:'Purchase points',created_at:'2026-09-09T12:00:00Z'},
  ]}));vi.stubGlobal('fetch',fetcher);
  const {container}=render(<LoyaltyView api={api} mode="history"/>);
  await screen.findByText('19 points');
  const action=screen.getByText('Refresh');
  expect(action.parentElement?.tagName).toBe('S-PAGE');expect(action.getAttribute('slot')).toBe('secondary-actions');
  expect(container.querySelector('s-scroll-box > s-box')?.getAttribute('padding')).toBe('base');
  expect(screen.getByText('-1 point')).toBeTruthy();expect(screen.getByText('+20 points')).toBeTruthy();
  const labels=[...container.querySelectorAll('s-stack[direction="inline"]')].map(e=>e.textContent);
  expect(labels[0]).toContain('Refund adjustment');expect(labels[1]).toContain('Purchase points');
  expect(container.querySelector('s-heading')).toBeNull();
  fireEvent.click(screen.getByText('Older activity'));await waitFor(()=>expect(fetcher).toHaveBeenCalledTimes(2));
  await screen.findByText('Refresh');fireEvent.click(screen.getByText('Refresh'));
  await waitFor(()=>expect(fetcher).toHaveBeenCalledTimes(3));
  expect(fetcher.mock.calls[2][0]).toBe('/api/loyalty/pos/customers/101?limit=10');
  expect(api.session.getSessionToken).toHaveBeenCalledTimes(3);
});

test('disconnect discards in-flight history; reconnect fetches a fresh token and starts at newest activity',async()=>{
  const {api,change}=fixture();let finish!:(value:Response)=>void;
  const fetcher=vi.fn().mockImplementationOnce(()=>reply(body('101','19','old-cursor')))
    .mockImplementationOnce(()=>new Promise<Response>(resolve=>{finish=resolve;}))
    .mockImplementation(()=>reply(body('101','21')));vi.stubGlobal('fetch',fetcher);
  render(<LoyaltyView api={api} mode="history"/>);await screen.findByText('19 points');
  fireEvent.click(screen.getByText('Older activity'));await waitFor(()=>expect(fetcher).toHaveBeenCalledTimes(2));
  const connection=api.connectivity.current.value as {internetConnected:string};connection.internetConnected='Disconnected';act(()=>change());
  await screen.findByText('Loyalty is unavailable. Try again when connected.');
  await act(async()=>finish(new Response(JSON.stringify(body('101','999')))));
  expect(screen.queryByText('999 points')).toBeNull();expect(screen.queryByText('Refund adjustment')).toBeNull();
  expect(fetcher).toHaveBeenCalledTimes(2);
  connection.internetConnected='Connected';act(()=>change());await screen.findByText('21 points');
  expect(fetcher.mock.calls[2][0]).not.toContain('cursor=');expect(api.session.getSessionToken).toHaveBeenCalledTimes(3);
});

test.each(['userId','locationId','shopDomain'])('session %s switch hides old data while a fresh read is pending',async(field)=>{
  const {api}=fixture();let finish!:(value:Response)=>void;
  const fetcher=vi.fn().mockImplementationOnce(()=>reply()).mockImplementationOnce(()=>new Promise<Response>(resolve=>{finish=resolve;}));vi.stubGlobal('fetch',fetcher);
  render(<LoyaltyView api={api} mode="history"/>);await screen.findByText('19 points');
  const session=api.session.currentSession as unknown as Record<string,unknown>;
  session[field]=field==='shopDomain'?'another-synthetic.myshopify.com':99;
  await waitFor(()=>expect(fetcher).toHaveBeenCalledTimes(2));
  expect(screen.queryByText('19 points')).toBeNull();expect(screen.queryByText('Refund adjustment')).toBeNull();
  await act(async()=>finish(new Response(JSON.stringify(body('101','7')))));
  await screen.findByText('7 points');expect(api.session.getSessionToken).toHaveBeenCalledTimes(2);
});

test.each([
  {balance_points:19},{balance_points:'19.5'},{balance_points:'-1'},{history:[null]},
  {history:[{delta_points:'1.5',label:'Purchase points',created_at:'2026-09-10'}]},
  {history:[{delta_points:'20',label:'Unexpected private evidence',created_at:'2026-09-10'}]},
  {as_of:'invalid'},{needs_review:'false'},{customer_id:'other-customer'},
])('malformed snapshot clears an existing balance on refresh: %j',async(invalid)=>{
  const {api}=fixture();vi.stubGlobal('fetch',vi.fn().mockImplementationOnce(()=>reply()).mockImplementationOnce(()=>reply({...body(),...invalid} as ReturnType<typeof body>)));
  render(<LoyaltyView api={api} mode="history"/>);await screen.findByText('19 points');fireEvent.click(screen.getByText('Refresh'));
  await screen.findByText('Loyalty is unavailable. Try again when connected.');
  expect(screen.queryByText('19 points')).toBeNull();expect(screen.queryByText('0 points')).toBeNull();expect(screen.queryByText('Refund adjustment')).toBeNull();
});
