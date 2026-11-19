import {useEffect, useRef, useState} from 'preact/hooks';
import type {Api} from '@shopify/ui-extensions/pos.home.modal.render';

export type ScanApi = Pick<Api, 'scanner' | 'session' | 'connectivity'> & {cart: Pick<Api['cart'], 'setCustomer'>};
type State = {kind:'ready'} | {kind:'looking'} | {kind:'attached'} | {kind:'error';message:string};

// Must match the server's member code format. Anything else (a product
// barcode, a web link) is rejected here without a network request.
const CODE = /^DGR1[A-Z2-7]{26}$/;
const CUSTOMER = /^[1-9][0-9]{0,15}$/;
// POS can redeliver the previous scan when this screen opens. A scan that was
// already present, or that arrives this soon after opening, is not the
// cashier's: attaching it would credit the wrong customer.
export const REPLAY_MS = 400;
const unavailable = 'Rewards card scanning is unavailable. Look the customer up by name.';
const attachFailed = 'The register could not attach this customer. Try again.';
const messages:Record<number,string> = {
  404:'This card is not active. Look the customer up by name.',
  429:'Please wait a minute, then scan again.',
  401:'Loyalty access is unavailable.',
  403:'Loyalty access is unavailable.',
};
const known = [...Object.values(messages), attachFailed];

export function MemberScan({api}:{api:ScanApi}) {
  const [state,setState]=useState<State>({kind:'ready'});
  const [sources,setSources]=useState(()=>api.scanner.sources.current.value ?? []);
  const busy=useRef(false);
  const mounted=useRef(true);
  const hasCamera=sources.includes('camera');
  const hasHardware=sources.includes('external') || sources.includes('embedded');

  useEffect(()=>api.scanner.sources.current.subscribe(value=>setSources(value ?? [])),[api]);
  useEffect(()=>{
    mounted.current=true;
    const opened=Date.now();
    const stale=api.scanner.scannerData.current.value;
    async function handle(code:string) {
      if (busy.current) return;
      // The camera view covers this screen, so close it on every scan: the
      // cashier must see the result, including a rejected code.
      api.scanner.hideCameraScanner();
      if (!CODE.test(code)) {setState({kind:'error',message:'That is not a Degen Rewards card.'});return;}
      if (api.connectivity.current.value.internetConnected!=='Connected') {setState({kind:'error',message:unavailable});return;}
      busy.current=true;setState({kind:'looking'});
      const controller=new AbortController();
      const timeout=setTimeout(()=>controller.abort(),10000);
      let next:State;
      try {
        const token=await api.session.getSessionToken();
        if (!token) throw Error(messages[401]);
        const response=await fetch('/api/loyalty/pos/member-codes/resolve',{method:'POST',
          headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/json'},
          body:JSON.stringify({code}),credentials:'omit',cache:'no-store',signal:controller.signal});
        if (!response.ok) throw Error(messages[response.status] ?? unavailable);
        const id=(await response.json() as {customer_id?:unknown}).customer_id;
        if (typeof id!=='string' || !CUSTOMER.test(id) || !Number.isSafeInteger(Number(id))) throw Error(unavailable);
        try {await api.cart.setCustomer({id:Number(id)});}
        catch {throw Error(attachFailed);}
        next={kind:'attached'};
      } catch (error) {
        const message=error instanceof Error?error.message:'';
        next={kind:'error',message:known.includes(message)?message:unavailable};
      } finally {clearTimeout(timeout);busy.current=false;}
      if (mounted.current) setState(next);
    }
    const unsubscribe=api.scanner.scannerData.current.subscribe(result=>{
      if (result===stale || Date.now()-opened<REPLAY_MS) return;
      const data=result?.data;
      if (typeof data==='string' && data.trim()) void handle(data.trim());
    });
    return ()=>{mounted.current=false;unsubscribe();api.scanner.hideCameraScanner();};
  },[api]);
  // A register with no hardware scanner goes straight to the camera.
  useEffect(()=>{
    const available=api.scanner.sources.current.value ?? [];
    if (available.includes('camera') && !available.some(s=>s==='external' || s==='embedded')) api.scanner.showCameraScanner();
  },[api]);

  return <s-page heading="Scan rewards card">
    <s-scroll-box><s-box padding="base"><s-stack direction="block" gap="base">
      {state.kind==='attached'?<>
        <s-text type="strong">Customer added to the cart.</s-text>
        <s-text color="subdued">Close this screen to continue the sale.</s-text>
      </>:state.kind==='looking'?<s-text>Looking up the card…</s-text>:<>
        {state.kind==='error' && <s-text tone="caution">{state.message}</s-text>}
        <s-text>{hasHardware?'Scan the code on the customer’s rewards card.':
          hasCamera?'Point the camera at the code on the customer’s rewards card.':
          'No scanner is available on this register. Look the customer up by name.'}</s-text>
        {hasCamera && <s-button variant="secondary" onClick={()=>api.scanner.showCameraScanner()}>Use camera</s-button>}
      </>}
    </s-stack></s-box></s-scroll-box>
  </s-page>;
}
