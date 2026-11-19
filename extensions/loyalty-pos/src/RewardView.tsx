import {useEffect, useRef, useState} from 'preact/hooks';
import type {ContextApi} from './LoyaltyView';

type Terms = {points:number;discount_cents:number;minimum_cents:number};
type Status = {available_points:number;reserved_points:number;needs_review:boolean;issuance_enabled?:boolean;rewards:Terms[]};
type Reservation = {redemption_id:number;state:string;code:string|null;can_cancel?:boolean;eligible_variant_ids:string[];custom_sales?:boolean}&Partial<Terms>;
// A discount is never more than 20% of the merchandise minimum.
const validTerms=(t:Partial<Terms>|undefined):t is Terms=>!!t && [t.points,t.discount_cents,t.minimum_cents].every(Number.isSafeInteger) &&
  t.points!>0 && t.discount_cents!>0 && t.minimum_cents!>=t.discount_cents!*5;
// Survives modal remounts while a native mutation is still unresolved.
const pendingApplications=new Set<string>();
const dollars=(cents:number)=>`$${(cents/100).toFixed(2)}`;
// Some register runtimes (Android) do not provide crypto.randomUUID.
export function newRequestKey() {
  const c=(globalThis as {crypto?:Crypto}).crypto;
  if(typeof c?.randomUUID==='function')return c.randomUUID();
  const bytes=new Uint8Array(16);
  if(typeof c?.getRandomValues==='function')c.getRandomValues(bytes);
  else for(let i=0;i<16;i++)bytes[i]=Math.floor(Math.random()*256);
  return Array.from(bytes,b=>b.toString(16).padStart(2,'0')).join('');
}
export function cartVariantIds(api:ContextApi) {
  const lines=api.cart?.current.value.lineItems??[];
  return [...new Set(lines.filter(l=>Number.isSafeInteger(l.variantId) && l.variantId!>0)
    .map(l=>`gid://shopify/ProductVariant/${l.variantId}`))].sort();
}
export function hasCustomSales(api:ContextApi) {
  return !!api.cart?.current.value.lineItems.some(l=>l.variantId==null && !l.isGiftCard);
}
export function canRedeem(api:ContextApi, minimum:number, eligible?:string[]) {
  const cart=api.cart?.current.value;
  if(!cart || cart.editable===false || cart.customer?.id!==api.customer.id ||
    !/^\d+(\.\d{1,2})?$/.test(cart.subtotal) || cart.cartDiscount || cart.cartDiscounts.length ||
    !cart.lineItems.length || !cart.lineItems.every(l=>!l.discounts.length && !l.discountAllocations?.length))return false;
  const ids=cartVariantIds(api);
  const custom=hasCustomSales(api);
  if((!ids.length && !custom) || ids.length>100)return false;
  if(cart.lineItems.some(l=>l.variantId!=null && (!Number.isSafeInteger(l.variantId) || l.variantId<=0)))return false;
  // Custom-sale codes cover the order. Keep excluded catalog items out of that
  // flow; receipt verification remains authoritative after checkout.
  const excluded=cart.lineItems.filter(l=>l.isGiftCard ||
    (l.variantId!=null && eligible && !eligible.includes(`gid://shopify/ProductVariant/${l.variantId}`)));
  if(custom && excluded.length)return false;
  if(excluded.some(l=>typeof l.price!=='number' || !Number.isFinite(l.price) || l.price<0 ||
    !Number.isSafeInteger(l.quantity) || l.quantity<=0))return false;
  const excludedCents=excluded.reduce((n,l)=>n+Math.round(l.price!*100)*l.quantity,0);
  return Math.round(Number(cart.subtotal)*100)-excludedCents>=minimum;
}
type Cart = NonNullable<ContextApi['cart']>['current']['value'];
const cartBasis=(cart:Cart)=>JSON.stringify({customer:cart.customer,subtotal:cart.subtotal,
  lines:cart.lineItems.map(({discounts,discountAllocations,taxLines,...line})=>line)});
export function appliedReward(api:ContextApi, code:string, cents:number, basis:string, eligible?:string[]) {
  const cart=api.cart?.current.value;
  if(!cart || cart.editable===false || cart.customer?.id!==api.customer.id || cartBasis(cart)!==basis)return false;
  const discounts=cart.cartDiscounts.length?cart.cartDiscounts:(cart.cartDiscount?[cart.cartDiscount]:[]);
  const matches=(d:typeof discounts[number])=>d.discountDescription===code &&
    d.type!=='Percentage' && Number.isFinite(d.amount) && Math.round(d.amount*100)===cents &&
    (!d.currency || d.currency==='USD');
  if(discounts.length!==1 || !matches(discounts[0]) || (cart.cartDiscount && !matches(cart.cartDiscount)))return false;
  if(cart.lineItems.some(line=>line.discounts.length || ((line.isGiftCard || (line.variantId!=null && eligible && !eligible.includes(`gid://shopify/ProductVariant/${line.variantId}`))) && line.discountAllocations?.length)))return false;
  const allocations=cart.lineItems.flatMap(line=>line.discountAllocations??[]);
  if(allocations.length && (allocations.some(a=>a.allocatedAmount.currencyCode!=='USD') ||
    allocations.reduce((n,a)=>n+Math.round(Number(a.allocatedAmount.amount)*100),0)!==cents))return false;
  return true;
}

export function RewardView({api}:{api:ContextApi}) {
  const [status,setStatus]=useState<Status>();
  const [reservation,setReservation]=useState<Reservation>();
  // The reward this checkout is using, once a cashier has picked one.
  const [chosen,setChosen]=useState<Terms>();
  const [message,setMessage]=useState('Loading rewards…');
  const [busy,setBusy]=useState(false);
  const [reload,setReload]=useState(0);
  const [,redraw]=useState(0);
  const requestKey=useRef<string>();
  const applicationAttempted=useRef(false);
  const mounted=useRef(true);
  const customer=api.customer.id;
  const identity=()=>JSON.stringify([api.customer.id,api.session.currentSession,
    api.session.staffMember.value?.id,api.connectivity.current.value.internetConnected]);
  const initial=useRef(identity());
  const current=()=>mounted.current && initial.current===identity();
  async function request(body?:object) {
    const controller=new AbortController();
    let timer:ReturnType<typeof setTimeout>;
    const timeout=new Promise<never>((_,reject)=>{timer=setTimeout(()=>{
      controller.abort();reject(Error('Reward request timed out. Check the reward before trying again.'));
    },45000);});
    try {
      return await Promise.race([timeout,(async()=>{
      const token=await api.session.getSessionToken();
      if(controller.signal.aborted)throw Error('Reward request timed out.');
      if(!token || !current())throw Error('Customer or staff changed. Reopen rewards.');
      const result=await fetch(`/api/loyalty/pos/customers/${customer}/rewards`,{
        method:body?'POST':'GET',headers:{Authorization:`Bearer ${token}`,...(body?{'Content-Type':'application/json'}:{})},
        body:body?JSON.stringify(body):undefined,credentials:'omit',cache:'no-store',signal:controller.signal});
      if(!result.ok) {
        const error=await result.json().catch(()=>({}));
        const messages:Record<string,string>={
          no_eligible_merchandise:'This cart has no eligible physical merchandise.',
          custom_sale_excluded_items:'Please sell gift cards or services separately when using a reward with custom sales.',
          reward_pricing_unsupported:'Rewards currently require USD prices with tax added at checkout.',
          catalog_incomplete:'Product eligibility could not be verified. Refresh the cart and try again.',
          request_key_conflict:'This pending reward belongs to an earlier cart. Ask an administrator to review it.',
        };
        throw Error(messages[error.error] ?? (result.status===503||result.status===404?'Redemption is not available yet.':
          'Reward could not be confirmed. Retry here or ask an administrator to review pending points.'));
      }
      return await result.json();
      })()]);
    } finally {clearTimeout(timer!);}
  }
  useEffect(()=>{
    mounted.current=true;
    const refresh=()=>redraw(n=>n+1);
    const unsubscribe=api.cart?.current.subscribe(refresh);
    const timer=setInterval(refresh,250);
    void request().then(data=>{
      if(!current())return;
      if(data.customer_id!==String(customer) || typeof data.needs_review!=='boolean' ||
        ![data.available_points,data.reserved_points].every(Number.isSafeInteger) ||
        !Array.isArray(data.rewards) || !data.rewards.length || data.rewards.length>10 || !data.rewards.every(validTerms) ||
        new Set(data.rewards.map((r:Terms)=>r.points)).size!==data.rewards.length)throw Error('Invalid reward terms.');
      data.rewards=[...data.rewards].sort((a:Terms,b:Terms)=>a.points-b.points);
      setStatus(data);setMessage('Custom sales count as merchandise. Ask the customer before applying a reward. Gift cards and services are excluded; sell them separately when using custom sales. One reward per sale; no other discounts.');
      if(data.issuance_enabled===false)setMessage('New rewards are paused. Existing rewards can still be checked or cancelled.');
      if(data.reserved_points>0 && !data.pending)setMessage('Points are reserved on another register. Use the original register or ask an administrator to review the reward.');
      if(data.pending && Number.isSafeInteger(data.pending.redemption_id) && typeof data.pending.request_key==='string') {
        setReservation(data.pending);requestKey.current=data.pending.request_key;
        if(validTerms(data.pending))setChosen({points:data.pending.points,discount_cents:data.pending.discount_cents,minimum_cents:data.pending.minimum_cents});
        setMessage('A reward is already reserved for this checkout. Check its status before continuing.');
      }
    }).catch(error=>{if(current())setMessage(error.message);});
    return ()=>{mounted.current=false;unsubscribe?.();clearInterval(timer);};
  },[api,reload]);
  // Move one reward's points between the displayed "available" and "reserved" counts.
  const shift=(available:number,reserved:number,points:number)=>setStatus(s=>s && {...s,
    available_points:Math.max(0,s.available_points+available*points),
    reserved_points:Math.max(0,s.reserved_points+reserved*points)});
  async function redeem(reward:Terms) {
    if(busy || applicationAttempted.current || !status || status.issuance_enabled===false || !current() || !canRedeem(api,reward.minimum_cents))return;
    // A checkout that already holds a reward can only continue with that same reward.
    if(reservation && (!chosen || chosen.points!==reward.points))return;
    setBusy(true);
    const fresh=!reservation;
    try {
      // Everything that can throw stays inside try so the button never sticks disabled.
      const cartBefore=JSON.stringify(api.cart!.current.value);
      const basis=cartBasis(api.cart!.current.value);
      requestKey.current??=newRequestKey();
      const variantIds=cartVariantIds(api);
      const custom=hasCustomSales(api);
      const scope={variant_ids:variantIds,...(custom?{custom_sales:true}:{})};
      const check=await request({action:'eligibility',...scope});
      if(!current() || JSON.stringify(api.cart!.current.value)!==cartBefore)
        throw Error('Cart changed. Check the cart and try again.');
      if(check.taxes_included!==false || !Array.isArray(check.eligible_variant_ids) ||
        (!check.eligible_variant_ids.length && !custom) || (custom && check.custom_sales!==true) || !check.eligible_variant_ids.every((id:unknown)=>typeof id==='string' && variantIds.includes(id)) ||
        !canRedeem(api,reward.minimum_cents,check.eligible_variant_ids))
        throw Error(`At least ${dollars(reward.minimum_cents)} of eligible physical merchandise before tax is required. Custom sales count. Gift cards and services do not; sell them separately when using custom sales.`);
      // Only the point cost is sent; the server decides the discount and the minimum.
      const row:Reservation=await request({action:'reserve',request_key:requestKey.current,reward_points:reward.points,...scope});
      if(!Number.isSafeInteger(row.redemption_id))throw Error('Reward response is unavailable. Retry here.');
      if(current()) {setReservation(row);setChosen(reward);}
      // Keep the displayed counts in step with a newly created hold.
      if(current() && fresh && ['ready','reserved'].includes(row.state))shift(-1,1,reward.points);
      if(row.points!==reward.points || row.discount_cents!==reward.discount_cents || row.minimum_cents!==reward.minimum_cents)
        throw Error('Reward terms changed. Do not complete checkout; cancel this reward and reopen rewards.');
      if(!current() || JSON.stringify(api.cart!.current.value)!==cartBefore)
        throw Error('Cart changed. Reward remains reserved; ask an administrator to review it.');
      if(row.state!=='ready' || !row.code || !/^DEGEN-[A-F0-9]{32}$/.test(row.code))
        throw Error('Reward is pending review. No new discount was applied.');
      if((custom && row.custom_sales!==true) || (!custom && row.custom_sales===true) || !Array.isArray(row.eligible_variant_ids) || JSON.stringify([...row.eligible_variant_ids].sort())!==JSON.stringify([...check.eligible_variant_ids].sort()))
        throw Error('Eligibility changed. Do not complete checkout; ask an administrator to review the reward.');
      if(pendingApplications.has(row.code))
        throw Error('Discount application is still pending. Check the cart and redemption before continuing.');
      applicationAttempted.current=true;
      pendingApplications.add(row.code);
      const deadline=Date.now()+3000;
      let nativeTimer:ReturnType<typeof setTimeout>;
      try {
        await Promise.race([Promise.resolve().then(()=>{
          if(!current())throw Error('Customer or staff changed. Reopen rewards.');
          return api.cart!.addCartCodeDiscount(row.code!);
        })
          .finally(()=>pendingApplications.delete(row.code!)),new Promise<never>((_,reject)=>{
          nativeTimer=setTimeout(()=>reject(Error('Discount application timed out. Do not complete checkout. Check the cart and redemption; points remain reserved.')),3000);
        })]);
      } finally {clearTimeout(nativeTimer!);}
      while(current() && !appliedReward(api,row.code,reward.discount_cents,basis,row.eligible_variant_ids) && Date.now()<deadline) {
        if(cartBasis(api.cart!.current.value)!==basis)break;
        await new Promise(resolve=>setTimeout(resolve,100));
      }
      if(!current() || !appliedReward(api,row.code,reward.discount_cents,basis,row.eligible_variant_ids))
        throw Error('Discount could not be verified. Do not complete checkout. Check the customer, cart and discount; points remain reserved.');
      if(current())setMessage('Reward applied. Complete this sale in POS. Points remain reserved until the completed order is verified.');
    } catch(error) {
      if(current())setMessage(error instanceof Error?error.message:'Reward could not be confirmed. Retry here.');
    } finally {if(mounted.current)setBusy(false);}
  }
  async function check() {
    if(!reservation || busy)return;
    setBusy(true);
    try {
      const row=await request({action:'refresh',redemption_id:reservation.redemption_id});
      if(current() && row.state!==reservation.state) {
        const points=chosen?.points ?? row.points ?? 0;
        if(row.state==='committed')shift(0,-1,points);
        if(row.state==='restored')shift(1,-1,points);
      }
      if(current()) {setReservation(row);setMessage(row.state==='committed'?'Sale verified. Points redeemed.':
        row.state==='restored'?'Full refund verified. Reward points restored.':'Reward is still pending. Do not redeem again for this sale.');}
    } catch(error) {if(current())setMessage(error instanceof Error?error.message:'Unable to check reward.');}
    finally {if(mounted.current)setBusy(false);}
  }
  async function cancel() {
    if(!reservation || busy)return;
    setBusy(true);
    try {
      const row=await request({action:'cancel',redemption_id:reservation.redemption_id});
      if(current() && row.state==='cancelled') {
        // The server confirmed the code is dead and unused, so the points are free again.
        // Take the dead discount off the cart when it is still the one we applied.
        const cart=api.cart?.current.value;
        const applied=cart?(cart.cartDiscounts.length?cart.cartDiscounts:(cart.cartDiscount?[cart.cartDiscount]:[])):[];
        let cleared=!applied.length;
        if(!cleared && reservation.code && applied.length===1 && applied[0].discountDescription===reservation.code &&
          typeof api.cart?.removeCartDiscount==='function') {
          try {await api.cart.removeCartDiscount();cleared=true;} catch {cleared=false;}
        }
        if(!current())return;
        if(reservation.state!=='cancelled')shift(1,-1,chosen?.points ?? row.points ?? 0);
        setReservation(undefined);setChosen(undefined);requestKey.current=undefined;applicationAttempted.current=false;
        setMessage(cleared?'Reward cancelled. The points are available again.':
          'Reward cancelled. The points are available again. Remove the reward discount from the cart before continuing.');
        return;
      }
      if(current()) {setReservation(row);setMessage(row.state==='committed' || row.state==='restored'?
        'This reward already belongs to a verified sale. Use the normal POS refund flow.':
        'Cancellation needs review. Check and remove the reward discount from the POS cart before continuing. Reserved points need administrator review before reuse.');}
    } catch(error) {if(current())setMessage(error instanceof Error?error.message:'Cancellation could not be confirmed.');}
    finally {if(mounted.current)setBusy(false);}
  }
  if(!current())return <s-text>Customer or staff changed. Reopen rewards.</s-text>;
  const holding=reservation?.state==='ready' || reservation?.state==='reserved';
  const open=!!status && status.issuance_enabled!==false && !status.needs_review;
  // A new reward needs enough available points; one already held only needs the cart to still qualify.
  const usable=(reward:Terms)=>open && (holding?chosen?.points===reward.points:status!.available_points>=reward.points) &&
    canRedeem(api,reward.minimum_cents);
  const offered=status?(reservation?(holding && chosen?[chosen]:[]):status.rewards):[];
  return <s-stack direction="block" gap="base" paddingBlock="base">
    <s-text type="strong">Redeem rewards</s-text>
    {status && <>
      {status.rewards.map(reward=><s-text key={reward.points}>{reward.points} points = {dollars(reward.discount_cents)} off {dollars(reward.minimum_cents)}+</s-text>)}
      <s-text>Minimums are merchandise before tax, including custom sales.</s-text>
      <s-text>{status.available_points} available · {status.reserved_points} reserved</s-text>
      {!reservation && !offered.some(usable) && <s-text>Add this customer to an eligible cart with no existing discounts and enough available points.</s-text>}
    </>}
    <s-text>{message}</s-text>
    {!status && <s-button disabled={busy} onClick={()=>setReload(n=>n+1)}>Refresh rewards</s-button>}
    {offered.map(reward=><s-button key={reward.points} disabled={!usable(reward) || busy || applicationAttempted.current} onClick={()=>redeem(reward)}>Use {reward.points} points for {dollars(reward.discount_cents)} off</s-button>)}
    {reservation && <s-button disabled={busy} onClick={check}>Check redemption</s-button>}
    {reservation && (['ready','reserved'].includes(reservation.state) || reservation.can_cancel===true) && <s-button disabled={busy} onClick={cancel}>Cancel this reward</s-button>}
  </s-stack>;
}
