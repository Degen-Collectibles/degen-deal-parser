import {render,screen,fireEvent,waitFor,cleanup,act} from '@testing-library/preact';
import {afterEach,expect,test,vi} from 'vitest';
import {LoyaltyView,type ContextApi} from '../src/LoyaltyView';
import {WalletEnroll} from '../src/WalletEnroll';

const TOKEN='A'.repeat(43);
const QR=`https://cards.example.test/rewards/card/${TOKEN}/qr.png`;
const snapshot=(available:boolean|undefined)=>({customer_id:'101',balance_points:'19',has_history:false,as_of:'2026-09-10T12:00:00Z',
  checked_at:null,needs_review:false,updates_pending:false,posting_paused:false,next_cursor:null,history:[],wallet_card_available:available});
const card=(changes:object={})=>({customer_id:'101',url:QR.slice(0,-7),qr_url:QR,expires_at:new Date(Date.now()+600000).toISOString(),...changes});
function fixture() {
  const listeners=new Set<()=>void>();
  const subscribe=(fn:()=>void)=>{listeners.add(fn);return ()=>{listeners.delete(fn);};};
  const api={connectivity:{current:{value:{internetConnected:'Connected'},subscribe}},customer:{id:101},
    session:{currentSession:{shopId:1,shopDomain:'synthetic.myshopify.com',userId:42,locationId:1},staffMember:{value:{id:7},subscribe},
      getSessionToken:vi.fn(async()=>'synthetic-token')},action:{presentModal:vi.fn()}};
  return {api:api as unknown as ContextApi,raw:api,change(){for(const listener of [...listeners])listener();}};
}
const reply=(data:object,status=200)=>Promise.resolve(new Response(JSON.stringify(data),{status}));
const image=()=>document.querySelector('s-image');
afterEach(()=>{cleanup();vi.unstubAllGlobals();vi.useRealTimers();});

test('the sign-up button appears only when the server says it is available, and never on the customer block',async()=>{
  for (const [available,mode,shown] of [[true,'history',true],[false,'history',false],[undefined,'history',false],[true,'block',false]] as const) {
    vi.stubGlobal('fetch',vi.fn(()=>reply(snapshot(available))));
    render(<LoyaltyView api={fixture().api} mode={mode}/>);
    await screen.findByText('19 points');
    expect(screen.queryByText('Add rewards card to phone')!==null).toBe(shown);
    cleanup();
  }
});
test('tapping it asks the server for a code for this customer and shows the QR image',async()=>{
  const {api}=fixture();
  const fetcher=vi.fn((url:string,_options?:RequestInit)=>url.endsWith('/wallet-card')?reply(card()):reply(snapshot(true)));
  vi.stubGlobal('fetch',fetcher);
  render(<LoyaltyView api={api} mode="history"/>);
  fireEvent.click(await screen.findByText('Add rewards card to phone'));
  await waitFor(()=>expect(image()?.getAttribute('src')).toBe(QR));
  const [url,options]=fetcher.mock.calls.find(call=>call[0].endsWith('/wallet-card'))!;
  expect(url).toBe('/api/loyalty/pos/customers/101/wallet-card');
  expect(options).toMatchObject({method:'POST',credentials:'omit',cache:'no-store',headers:{Authorization:'Bearer synthetic-token'}});
  expect(options!.body).toBeUndefined();
  expect(screen.getByText('Works once. Expires in 10 minutes.')).toBeTruthy();
  fireEvent.click(screen.getByText('Back to points'));
  expect(await screen.findByText('19 points')).toBeTruthy();
  expect(image()).toBeNull();
});
test('the code is dropped when the customer on screen changes',async()=>{
  const {api,raw}=fixture();
  vi.stubGlobal('fetch',vi.fn((url:string)=>url.endsWith('/wallet-card')?reply(card()):
    reply({...snapshot(true),customer_id:url.includes('/202')?'202':'101'})));
  render(<LoyaltyView api={api} mode="history"/>);
  fireEvent.click(await screen.findByText('Add rewards card to phone'));
  await waitFor(()=>expect(image()).not.toBeNull());
  raw.customer.id=202;
  await waitFor(()=>expect(image()).toBeNull(),{timeout:2000});
  expect(await screen.findByText('Add rewards card to phone')).toBeTruthy();
});
test.each([
  ['another customer',{customer_id:'202'}],
  ['a plain http address',{qr_url:QR.replace('https','http')}],
  ['a different path',{qr_url:'https://cards.example.test/other.png'}],
  ['a script address',{qr_url:'javascript:alert(1)'}],
  ['no expiry',{expires_at:'soon'}],
])('a reply with %s is never displayed',async(_name,changes)=>{
  vi.stubGlobal('fetch',vi.fn(()=>reply(card(changes))));
  render(<WalletEnroll api={fixture().api} onBack={()=>{}}/>);
  expect(await screen.findByText(/sign-up is unavailable/)).toBeTruthy();
  expect(image()).toBeNull();
});
test('errors can be retried and a rate limit says to wait',async()=>{
  const fetcher=vi.fn().mockImplementationOnce(()=>reply({error:'rate_limited'},429))
    .mockImplementationOnce(()=>reply({error:'wallet_enrollment_unavailable'},503)).mockImplementationOnce(()=>reply(card()));
  vi.stubGlobal('fetch',fetcher);
  render(<WalletEnroll api={fixture().api} onBack={()=>{}}/>);
  expect(await screen.findByText('Please wait a minute, then try again.')).toBeTruthy();
  fireEvent.click(screen.getByText('Try again'));
  expect(await screen.findByText(/sign-up is unavailable/)).toBeTruthy();
  fireEvent.click(screen.getByText('Try again'));
  await waitFor(()=>expect(image()?.getAttribute('src')).toBe(QR));
});
test('offline and missing customer never call the server',async()=>{
  const fetcher=vi.fn();vi.stubGlobal('fetch',fetcher);
  const offline=fixture();offline.raw.connectivity.current.value.internetConnected='Disconnected';
  render(<WalletEnroll api={offline.api} onBack={()=>{}}/>);
  expect(await screen.findByText(/sign-up is unavailable/)).toBeTruthy();
  cleanup();
  const nobody=fixture();nobody.raw.customer.id=0;
  render(<WalletEnroll api={nobody.api} onBack={()=>{}}/>);
  expect(await screen.findByText(/sign-up is unavailable/)).toBeTruthy();
  expect(fetcher).not.toHaveBeenCalled();
});
test('an expired code is taken off the screen and a new one can be requested',async()=>{
  const fetcher=vi.fn().mockImplementationOnce(()=>reply(card({expires_at:new Date(Date.now()+1500).toISOString()})))
    .mockImplementationOnce(()=>reply(card()));
  vi.stubGlobal('fetch',fetcher);
  render(<WalletEnroll api={fixture().api} onBack={()=>{}}/>);
  await waitFor(()=>expect(image()).not.toBeNull());
  expect(screen.getByText('Works once. Expires in 1 minute.')).toBeTruthy();
  expect(await screen.findByText('This code has expired.',{},{timeout:4000})).toBeTruthy();
  expect(image()).toBeNull();
  await act(async()=>{fireEvent.click(screen.getByText('New code'));});
  await waitFor(()=>expect(image()?.getAttribute('src')).toBe(QR));
  expect(fetcher).toHaveBeenCalledTimes(2);
});
