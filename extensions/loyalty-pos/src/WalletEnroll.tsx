import {useEffect, useState} from 'preact/hooks';
import type {ContextApi} from './LoyaltyView';

type State = {qr?:string;expires?:number;error?:string};
const unavailable = 'Rewards card sign-up is unavailable. Try again when connected.';
const wait = 'Please wait a minute, then try again.';
// The register only ever displays an image from our own https address.
const QR = /^https:\/\/[A-Za-z0-9.-]{1,253}(:[0-9]{1,5})?\/rewards\/card\/[A-Za-z0-9_-]{43}\/qr\.png$/;

export function WalletEnroll({api,onBack}:{api:Pick<ContextApi,'customer'|'session'|'connectivity'>;onBack:()=>void}) {
  const [state,setState]=useState<State>({});
  const [attempt,setAttempt]=useState(0);
  const [now,setNow]=useState(()=>Date.now());
  useEffect(()=>{
    let live=true;
    const controller=new AbortController();
    const timeout=setTimeout(()=>controller.abort(),10000);
    const id=api.customer.id;
    setState({});
    async function load() {
      try {
        if (!Number.isSafeInteger(id) || id<=0) throw Error(unavailable);
        if (api.connectivity.current.value.internetConnected!=='Connected') throw Error(unavailable);
        const token=await api.session.getSessionToken();
        if (!token) throw Error(unavailable);
        const response=await fetch(`/api/loyalty/pos/customers/${id}/wallet-card`,{method:'POST',
          headers:{Authorization:`Bearer ${token}`},credentials:'omit',cache:'no-store',signal:controller.signal});
        if (!response.ok) throw Error(response.status===429?wait:unavailable);
        const data=await response.json() as {customer_id?:unknown;qr_url?:unknown;expires_at?:unknown};
        const expires=typeof data.expires_at==='string'?Date.parse(data.expires_at):NaN;
        // A reply for another customer must never be shown on this screen.
        if (data.customer_id!==String(id) || typeof data.qr_url!=='string' || !QR.test(data.qr_url) || !Number.isFinite(expires)) throw Error(unavailable);
        if (live) setState({qr:data.qr_url,expires});
      } catch (error) {
        if (live) setState({error:error instanceof Error && error.message===wait?wait:unavailable});
      } finally {clearTimeout(timeout);}
    }
    void load();
    return ()=>{live=false;controller.abort();clearTimeout(timeout);};
  },[api,attempt]);
  useEffect(()=>{const timer=setInterval(()=>setNow(Date.now()),1000);return ()=>clearInterval(timer);},[]);
  const expired=state.expires!==undefined && now>=state.expires;
  const minutes=state.expires!==undefined?Math.max(1,Math.round((state.expires-now)/60000)):0;
  const again=()=>setAttempt(v=>v+1);
  return <s-stack direction="block" gap="base" paddingBlock="small">
    {state.error?<>
      <s-text tone="caution">{state.error}</s-text>
      <s-button variant="secondary" onClick={again}>Try again</s-button>
    </>:!state.qr?<s-text>Making a code…</s-text>:expired?<>
      <s-text>This code has expired.</s-text>
      <s-button variant="secondary" onClick={again}>New code</s-button>
    </>:<>
      <s-text type="strong">Ask the customer to scan this with their phone camera.</s-text>
      <s-box maxInlineSize="320px"><s-image src={state.qr} inlineSize="fill" objectFit="contain"/></s-box>
      <s-text type="small" color="subdued">Works once. Expires in {minutes} {minutes===1?'minute':'minutes'}.</s-text>
      <s-button variant="secondary" onClick={again}>New code</s-button>
    </>}
    <s-button variant="secondary" onClick={onBack}>Back to points</s-button>
  </s-stack>;
}
