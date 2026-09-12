import {useEffect, useRef, useState} from 'preact/hooks';
import type {Api as BlockApi} from '@shopify/ui-extensions/pos.customer-details.block.render';
import type {} from '@shopify/ui-extensions/pos.customer-details.action.render';

export type ContextApi = Pick<BlockApi, 'customer' | 'session' | 'connectivity'> & {action?: Pick<BlockApi['action'], 'presentModal'>};
type Entry = {delta_points:string;label:string;created_at:string};
type Snapshot = {customer_id:string;balance_points:string;has_history:boolean;as_of:string;checked_at:string|null;needs_review:boolean;updates_pending:boolean;posting_paused:boolean;next_cursor:string|null;history:Entry[]};
const unavailable = 'Loyalty is unavailable. Try again when connected.';
const integer = /^-?(0|[1-9][0-9]{0,29})$/;
function unit(value:string) {return value==='1'||value==='-1'?'point':'points';}
export function points(value:string) {return value.replace(/\B(?=(\d{3})+(?!\d))/g, ',');}
function context(api:ContextApi) {
  const s=api.session.currentSession;
  return JSON.stringify([api.customer.id,s.shopId,s.shopDomain,s.userId,s.locationId,api.session.staffMember.value?.id,api.connectivity.current.value.internetConnected]);
}
function validate(value:unknown, customer:string):Snapshot {
  const s=value as Snapshot;
  if (!s || s.customer_id!==customer || typeof s.balance_points!=='string' || !integer.test(s.balance_points) || s.balance_points.startsWith('-') ||
      !['has_history','needs_review','updates_pending','posting_paused'].every(k=>typeof s[k as keyof Snapshot]==='boolean') ||
      typeof s.as_of!=='string' || !Number.isFinite(Date.parse(s.as_of)) ||
      !(s.checked_at===null || (typeof s.checked_at==='string' && Number.isFinite(Date.parse(s.checked_at)))) ||
      !(s.next_cursor===null || (typeof s.next_cursor==='string' && s.next_cursor.length<=1800)) ||
      !Array.isArray(s.history) || s.history.length>25 || !s.history.every(e=>
        typeof e.delta_points==='string' && integer.test(e.delta_points) &&
        ['Purchase points','Refund adjustment','Administrator adjustment'].includes(e.label) &&
        typeof e.created_at==='string' && Number.isFinite(Date.parse(e.created_at)))) throw Error(unavailable);
  return s;
}
function date(value:string) {return new Date(value).toLocaleString(undefined,{month:'short',day:'numeric',hour:'numeric',minute:'2-digit'});}

export function LoyaltyView({api,mode}:{api:ContextApi;mode:'block'|'history'}) {
  const [key,setKey]=useState(()=>context(api));
  const [cursor,setCursor]=useState<string|null>(null);
  const [refresh,setRefresh]=useState(0);
  const [state,setState]=useState<{key:string;data?:Snapshot;error?:string}>({key});
  const generation=useRef(0);
  const abort=useRef<AbortController>();
  useEffect(()=>{
    const changed=()=>{
      const next=context(api);
      if (next!==key) {
        ++generation.current;abort.current?.abort();setState({key:next});setCursor(null);setKey(next);
      }
    };
    const unsubscribe=api.session.staffMember.subscribe(changed);
    const disconnect=api.connectivity.current.subscribe(changed);
    // Customer API is a contextual ID, not a subscription. Native targets remount
    // on navigation; polling also detects an in-place session/context replacement.
    const timer=setInterval(changed,250);
    return ()=>{unsubscribe();disconnect();clearInterval(timer);};
  },[api,key]);
  useEffect(()=>{
    const id=api.customer.id;
    const own=++generation.current;
    const controller=new AbortController();abort.current=controller;
    setState({key});
    if (!Number.isSafeInteger(id) || id<=0) {setState({key,error:'Select an existing customer in POS.'});return;}
    if(api.connectivity.current.value.internetConnected!=='Connected'){setState({key,error:unavailable});return;}
    const current=()=>own===generation.current && context(api)===key;
    const timeout=setTimeout(()=>{
      if(current()){setState({key,error:unavailable});++generation.current;}
      controller.abort();
    },10000);
    async function load() {
      try {
        const token=await api.session.getSessionToken();
        if (!current())return;
        if (!token)throw Error('Loyalty access is unavailable.');
        if (controller.signal.aborted)throw Error(unavailable);
        const query=new URLSearchParams({limit:'10'});if(cursor)query.set('cursor',cursor);
        const response=await fetch(`/api/loyalty/pos/customers/${id}?${query}`,{headers:{Authorization:`Bearer ${token}`},credentials:'omit',cache:'no-store',signal:controller.signal});
        if (!response.ok)throw Error(response.status===409?'Activity changed. Refresh to see the latest points.':response.status===429?'Please wait a minute, then retry.':[401,403].includes(response.status)?'Loyalty access is unavailable.':unavailable);
        const data=validate(await response.json(),String(id));
        if (current() && !controller.signal.aborted)setState({key,data});
      } catch (error) {
        if(current())setState({key,error:error instanceof Error && [unavailable,'Loyalty access is unavailable.','Activity changed. Refresh to see the latest points.','Please wait a minute, then retry.'].includes(error.message)?error.message:unavailable});
      } finally {clearTimeout(timeout);}
    }
    void load();
    return ()=>{++generation.current;controller.abort();clearTimeout(timeout);};
  },[api,key,cursor,refresh]);
  const visible=state.key===context(api)?state:{key};
  const data=visible.data;
  const retry=()=>{setCursor(null);setRefresh(v=>v+1);};
  // Native Heading has no size prop; strong text keeps the balance prominent
  // without the oversized section heading. POSBlock owns the outer card inset.
  const status=data?[
    data.needs_review && 'Order needs review',
    data.updates_pending && 'Updates pending',
    data.posting_paused && 'Updates paused',
  ].filter(Boolean).join(' · '):'';
  const content=<s-stack direction="block" gap="base" paddingBlock="small">
    {visible.error?<s-text>{visible.error}</s-text>:!data?<s-text>Loading points…</s-text>:<>
      <s-stack direction="block" gap="small">
        <s-text type="strong">{points(data.balance_points)} {unit(data.balance_points)}</s-text>
        {!data.has_history && <s-text color="subdued">No posted points yet.</s-text>}
        {status && <s-text type="small" tone={data.needs_review?'caution':'auto'}>{status}</s-text>}
        {mode==='history' && data.needs_review && <s-text type="small" color="subdued">Ask an Ops administrator to review this customer’s order.</s-text>}
        <s-text type="small" color="subdued">As of {date(data.as_of)}</s-text>
      </s-stack>
      {mode==='history' && data.history.length>0 && <s-stack direction="block" gap="none">
        <s-box paddingBlock="small"><s-text type="strong">Recent activity</s-text></s-box>
        {data.history.map((entry,index)=><s-stack key={`${cursor ?? 'first'}-${index}`} direction="block" gap="none">
          <s-divider/>
          <s-stack direction="inline" justifyContent="space-between" alignItems="start" gap="base" paddingBlock="base">
            <s-stack direction="block" gap="small-200" minInlineSize="0">
              <s-text>{entry.label}</s-text>
              <s-text type="small" color="subdued">{date(entry.created_at)}</s-text>
            </s-stack>
            <s-text type="strong">{entry.delta_points.startsWith('-')?'':'+'}{points(entry.delta_points)} {unit(entry.delta_points)}</s-text>
          </s-stack>
        </s-stack>)}
        {data.next_cursor && <s-box paddingBlockStart="small">
          <s-button variant="secondary" onClick={()=>setCursor(data.next_cursor)}>Older activity</s-button>
        </s-box>}
      </s-stack>}
    </>}
  </s-stack>;
  // Slot actions must be direct children: native POS places them in its action
  // bar, unlike a regular button in the body. Each slot supports one action.
  return mode==='block'?<s-pos-block heading="Loyalty points">
    {content}
    {data?<s-button slot="secondary-actions" variant="secondary" onClick={()=>api.action?.presentModal()}>View history</s-button>:
      visible.error && <s-button slot="secondary-actions" variant="secondary" onClick={retry}>Refresh</s-button>}
  </s-pos-block>:<s-page heading="Loyalty activity">
    {(data || visible.error) && <s-button slot="secondary-actions" variant="secondary" onClick={retry}>Refresh</s-button>}
    <s-scroll-box><s-box padding="base">{content}</s-box></s-scroll-box>
  </s-page>;
}
