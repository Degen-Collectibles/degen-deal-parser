import {render,screen,waitFor,cleanup} from '@testing-library/preact';
import {afterEach,beforeEach,expect,test,vi} from 'vitest';
import {MemberScan,REPLAY_MS,type ScanApi} from '../src/MemberScan';

const CODE='DGR1'+'A'.repeat(26);
type Scan={data?:string;source?:string};
function fixture(sources:string[]=['external'],previous:Scan={}) {
  const listeners=new Set<(value:Scan)=>void>();
  const data={value:previous,subscribe(fn:(value:Scan)=>void){listeners.add(fn);return ()=>listeners.delete(fn);}};
  const api={
    scanner:{scannerData:{current:data},sources:{current:{value:sources,subscribe:()=>()=>{}}},
      showCameraScanner:vi.fn(),hideCameraScanner:vi.fn()},
    session:{getSessionToken:vi.fn(async()=>'test-token')},
    connectivity:{current:{value:{internetConnected:'Connected'},subscribe:()=>()=>{}}},
    cart:{setCustomer:vi.fn(async(_customer:{id:number})=>{})},
  };
  const scan=(value:string)=>{data.value={data:value,source:'external'};listeners.forEach(fn=>fn(data.value));};
  const replay=()=>listeners.forEach(fn=>fn(data.value));
  return {api:api as unknown as ScanApi,raw:api,scan,replay,listeners};
}
const reply=(body:object,status=200)=>new Response(JSON.stringify(body),{status});
let now=1_000_000;
const open=()=>{now+=REPLAY_MS+1;};
beforeEach(()=>{vi.spyOn(Date,'now').mockImplementation(()=>now);});
afterEach(()=>{cleanup();vi.unstubAllGlobals();});

test('a scanned card is looked up with the register token and attached to the cart',async()=>{
  const f=fixture();const fetcher=vi.fn(async(..._args:unknown[])=>reply({customer_id:'101'}));vi.stubGlobal('fetch',fetcher);
  render(<MemberScan api={f.api}/>);open();
  f.scan(CODE);
  await waitFor(()=>expect(f.raw.cart.setCustomer).toHaveBeenCalledWith({id:101}));
  const [url,options]=fetcher.mock.calls[0] as [string,RequestInit];
  expect(url).toBe('/api/loyalty/pos/member-codes/resolve');
  expect(options.method).toBe('POST');
  expect(options.credentials).toBe('omit');
  expect((options.headers as Record<string,string>).Authorization).toBe('Bearer test-token');
  expect(JSON.parse(options.body as string)).toEqual({code:CODE});
  expect(await screen.findByText('Customer added to the cart.')).toBeTruthy();
});
test('a scan left over from before the screen opened is never attached',async()=>{
  const f=fixture(['external'],{data:CODE,source:'external'});const fetcher=vi.fn();vi.stubGlobal('fetch',fetcher);
  render(<MemberScan api={f.api}/>);
  f.replay();                       // same object redelivered on open
  f.scan(CODE);                     // new object, but still inside the replay window
  await new Promise(r=>setTimeout(r,0));
  expect(fetcher).not.toHaveBeenCalled();
  expect(f.raw.cart.setCustomer).not.toHaveBeenCalled();
});
test('the scan that was already on screen at open is ignored even later',async()=>{
  const f=fixture(['external'],{data:CODE,source:'external'});const fetcher=vi.fn();vi.stubGlobal('fetch',fetcher);
  render(<MemberScan api={f.api}/>);open();
  f.replay();
  await new Promise(r=>setTimeout(r,0));
  expect(fetcher).not.toHaveBeenCalled();
});
test('anything that is not a rewards card is rejected without a request',async()=>{
  const f=fixture();const fetcher=vi.fn();vi.stubGlobal('fetch',fetcher);
  render(<MemberScan api={f.api}/>);open();
  for (const value of ['012345678905','https://example.com/'+CODE,CODE.toLowerCase(),CODE+'A','gid://shopify/Customer/101']) f.scan(value);
  expect(await screen.findByText('That is not a Degen Rewards card.')).toBeTruthy();
  expect(fetcher).not.toHaveBeenCalled();
  // The camera view covers the message, so a rejected code must close it.
  expect(f.raw.scanner.hideCameraScanner).toHaveBeenCalled();
  expect(f.raw.cart.setCustomer).not.toHaveBeenCalled();
});
test('an inactive card, a bad reply and a failed attach each leave the cart alone or say so',async()=>{
  const f=fixture();
  const fetcher=vi.fn().mockResolvedValueOnce(reply({error:'unknown_code'},404))
    .mockResolvedValueOnce(reply({customer_id:'0'}))
    .mockResolvedValueOnce(reply({error:'rate_limited'},429))
    .mockResolvedValueOnce(reply({customer_id:101}))
    .mockResolvedValueOnce(reply({customer_id:'101'}));
  vi.stubGlobal('fetch',fetcher);
  render(<MemberScan api={f.api}/>);open();
  f.scan(CODE);expect(await screen.findByText(/This card is not active/)).toBeTruthy();
  f.scan(CODE);expect(await screen.findByText(/scanning is unavailable/)).toBeTruthy();
  f.scan(CODE);expect(await screen.findByText(/wait a minute/)).toBeTruthy();
  f.scan(CODE);expect(await screen.findByText(/scanning is unavailable/)).toBeTruthy();
  expect(fetcher).toHaveBeenCalledTimes(4);
  expect(f.raw.cart.setCustomer).not.toHaveBeenCalled();
  f.raw.cart.setCustomer.mockRejectedValueOnce(new Error('native'));
  f.scan(CODE);expect(await screen.findByText(/could not attach this customer/)).toBeTruthy();
});
test('a second scan during a lookup is ignored',async()=>{
  const f=fixture();let resolve!:(value:Response)=>void;
  const fetcher=vi.fn(()=>new Promise<Response>(r=>{resolve=r;}));vi.stubGlobal('fetch',fetcher);
  render(<MemberScan api={f.api}/>);open();
  f.scan(CODE);await waitFor(()=>expect(fetcher).toHaveBeenCalledTimes(1));
  f.scan('DGR1'+'B'.repeat(26));
  resolve(reply({customer_id:'101'}));
  await waitFor(()=>expect(f.raw.cart.setCustomer).toHaveBeenCalledTimes(1));
  expect(fetcher).toHaveBeenCalledTimes(1);
});
test('offline registers do not look anything up',async()=>{
  const f=fixture();f.raw.connectivity.current.value.internetConnected='Disconnected';
  const fetcher=vi.fn();vi.stubGlobal('fetch',fetcher);
  render(<MemberScan api={f.api}/>);open();f.scan(CODE);
  expect(await screen.findByText(/scanning is unavailable/)).toBeTruthy();
  expect(fetcher).not.toHaveBeenCalled();
});
test('camera opens by itself only when there is no hardware scanner, and closes on exit',()=>{
  const hardware=fixture(['external','camera']);
  render(<MemberScan api={hardware.api}/>);
  expect(hardware.raw.scanner.showCameraScanner).not.toHaveBeenCalled();
  expect(screen.getByText('Use camera')).toBeTruthy();
  cleanup();
  const camera=fixture(['camera']);
  const view=render(<MemberScan api={camera.api}/>);
  expect(camera.raw.scanner.showCameraScanner).toHaveBeenCalledTimes(1);
  view.unmount();
  expect(camera.raw.scanner.hideCameraScanner).toHaveBeenCalled();
  expect(camera.listeners.size).toBe(0);
  render(<MemberScan api={fixture([]).api}/>);
  expect(screen.getByText(/No scanner is available/)).toBeTruthy();
});
