// Browser-only synthetic host; never imported by either native entry point.
import {render} from 'preact';
import {useState} from 'preact/hooks';
import {LoyaltyView, type ContextApi} from './LoyaltyView';

// Plain browser accessibility shims for Shopify custom elements. These do not
// emulate native rendering, authorization or Shopify's POS runtime.
class DemoButton extends HTMLElement {
  connectedCallback() {
    this.setAttribute('role','button');this.tabIndex=0;
    this.addEventListener('keydown',e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();this.click();}});
  }
}
customElements.define('s-button',DemoButton);
function Preview() {
  const [customer,setCustomer]=useState(101);
  const [pin,setPin]=useState(7);
  const [mode,setMode]=useState<'block'|'history'>('block');
  const [offline,setOffline]=useState(false);
  // A fresh object remounts the context exactly as a native target would.
  const api={connectivity:{current:{value:{internetConnected:offline?'Disconnected':'Connected'},subscribe(){return ()=>{};}}},customer:{id:customer},session:{currentSession:{shopId:1,shopDomain:'synthetic-pos.myshopify.com',userId:42,locationId:1},staffMember:{value:{id:pin},subscribe(){return ()=>{};}},async getSessionToken(){
    if(offline)throw Error('Synthetic offline');
    const response=await fetch('/demo/token',{method:'POST',headers:{'X-Loyalty-Demo':'synthetic'},credentials:'omit',cache:'no-store'});
    if(!response.ok)return undefined;
    return (await response.json()).token as string;
  }},action:{presentModal(){setMode('history');}}} as unknown as ContextApi;
  return <main>
    <header><span class="tag">LOCAL SYNTHETIC DEMO</span><h1>Customer points in POS</h1><p>Actual loyalty view and read endpoint with synthetic data. This browser preview is not native Shopify POS.</p></header>
    <aside aria-label="Demo controls">
      <label>Sample customer<select value={customer} onChange={e=>setCustomer(Number(e.currentTarget.value))}>
        <option value="101">Purchase · 20 points</option><option value="102">Refund · 19 points</option>
        <option value="103">Pending update · no posted points</option><option value="104">Review · 12 points</option>
        <option value="105">History · 15 purchases</option>
      </select></label>
      <label>Simulated PIN context<select value={pin} onChange={e=>setPin(Number(e.currentTarget.value))}><option value="7">Staff A</option><option value="8">Staff B</option></select></label>
      <label class="check"><input type="checkbox" checked={offline} onChange={e=>setOffline(e.currentTarget.checked)}/>Simulate offline</label>
      <p>PIN changes clear the view. PIN identity is not verified and does not authorize access under this demo’s all-staff policy.</p>
    </aside>
    <div class="preview" aria-label="Loyalty view">
      {mode==='history' && <button class="back" onClick={()=>setMode('block')}>Back to customer</button>}
      <LoyaltyView api={api} mode={mode}/>
    </div>
    <footer>Read only. Corrections stay in Ops. Production access and all feature flags remain off.</footer>
  </main>;
}
render(<Preview/>,document.getElementById('preview')!);
