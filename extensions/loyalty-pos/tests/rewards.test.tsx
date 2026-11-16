import {render,screen,fireEvent,waitFor,cleanup} from '@testing-library/preact';
import {afterEach,expect,test,vi} from 'vitest';
import {RewardView,canRedeem,appliedReward} from '../src/RewardView';
import {CartRewards} from '../src/CartRewards';
import type {ContextApi} from '../src/LoyaltyView';
function fixture() {
  const subscribe=()=>()=>{};
  const api={customer:{id:101},session:{currentSession:{shopId:1,userId:42,locationId:2},
    staffMember:{value:{id:7},subscribe},getSessionToken:async()=>'test'},
    connectivity:{current:{value:{internetConnected:'Connected'},subscribe}},
    cart:{current:{value:{customer:{id:101},subtotal:'100.00',cartDiscounts:[] as {amount:number;discountDescription:string}[],
      lineItems:[{uuid:'line',variantId:11,price:100,quantity:1,discounts:[]}]},subscribe},addCartCodeDiscount:vi.fn(async(code:string)=>{api.cart.current.value.cartDiscounts=[{amount:10,discountDescription:code}];})}};
  return api as unknown as ContextApi;
}
const status={customer_id:'101',available_points:500,reserved_points:0,needs_review:false,
  reward:{points:500,discount_cents:1000,minimum_cents:10000}};
const reply=(body:object)=>new Response(JSON.stringify('redemption_id' in body?{eligible_variant_ids:['gid://shopify/ProductVariant/11'],...body}:body));
function stubFetch(fetcher:any) {
  vi.stubGlobal('fetch',(url:any,options:any)=>{
    if(options?.body && JSON.parse(options.body).action==='eligibility')return Promise.resolve(reply({eligible_variant_ids:['gid://shopify/ProductVariant/11'],taxes_included:false}));
    return fetcher(url,options);
  });
}
afterEach(()=>{cleanup();vi.unstubAllGlobals();});
test('cart entry requires an attached customer before fetching rewards',()=>{
  const api=fixture();api.cart!.current.value.customer=undefined;
  const fetcher=vi.fn();stubFetch(fetcher);
  render(<CartRewards api={api as unknown as Parameters<typeof CartRewards>[0]['api']}/>);
  expect(screen.getByText(/Add a customer to this cart/)).toBeTruthy();
  expect(fetcher).not.toHaveBeenCalled();
});
test('cart entry reads the cart customer and rejects a switch during reservation',async()=>{
  const api=fixture();api.customer.id=999;
  let resolve!:(value:Response)=>void;
  const fetcher=vi.fn().mockResolvedValueOnce(reply(status)).mockImplementationOnce(()=>new Promise(r=>{resolve=r;}));
  stubFetch(fetcher);
  render(<CartRewards api={api as unknown as Parameters<typeof CartRewards>[0]['api']}/>);
  fireEvent.click(await screen.findByText('Use 500 points for $10.00 off'));
  await waitFor(()=>expect(fetcher).toHaveBeenCalledTimes(2));
  expect(fetcher.mock.calls[0][0]).toContain('/customers/101/rewards');
  api.cart!.current.value.customer!.id=202;
  resolve(reply({redemption_id:1,state:'ready',code:'DEGEN-'+'A'.repeat(32)}));
  await new Promise(r=>setTimeout(r,0));
  expect(api.cart!.addCartCodeDiscount).not.toHaveBeenCalled();
});
test('requires same cart customer, minimum, and no existing discounts',()=>{
  const api=fixture();expect(canRedeem(api,10000)).toBe(true);
  api.cart!.current.value.customer!.id=999;expect(canRedeem(api,10000)).toBe(false);
  api.cart!.current.value.customer!.id=101;api.cart!.current.value.subtotal='99.99';
  expect(canRedeem(api,10000)).toBe(false);
  api.cart!.current.value.subtotal='100.00';
  api.cart!.current.value.lineItems[0].discounts.push({amount:1,discountDescription:'sale'});
  expect(canRedeem(api,10000)).toBe(false);
});
test('gift-card purchases do not count toward the merchandise minimum',()=>{
  const api=fixture();const cart=api.cart!.current.value;
  cart.lineItems.push({...cart.lineItems[0],uuid:'gift',isGiftCard:true,price:100,quantity:1});
  cart.subtotal='199.99';expect(canRedeem(api,10000)).toBe(false);
  cart.subtotal='200.00';expect(canRedeem(api,10000)).toBe(true);
  cart.lineItems[1].price=undefined;expect(canRedeem(api,10000)).toBe(false);
});
test('reserves before applying single-use code and never sends price or points',async()=>{
  const api=fixture();const fetcher=vi.fn().mockResolvedValueOnce(reply(status)).mockResolvedValueOnce(reply({
    redemption_id:1,state:'ready',code:'DEGEN-'+'A'.repeat(32)}));stubFetch(fetcher);
  render(<RewardView api={api}/>);
  fireEvent.click(await screen.findByText('Use 500 points for $10.00 off'));
  await waitFor(()=>expect(api.cart!.addCartCodeDiscount).toHaveBeenCalledWith('DEGEN-'+'A'.repeat(32)));
  expect(Object.keys(JSON.parse(fetcher.mock.calls[1][1].body)).sort()).toEqual(['action','request_key','variant_ids']);
  expect(await screen.findByText(/Reward applied/)).toBeTruthy();
  expect(screen.getByText('0 available · 500 reserved')).toBeTruthy();
});
test('redeem works when the runtime has no crypto.randomUUID',async()=>{
  const {newRequestKey}=await import('../src/RewardView');
  vi.stubGlobal('crypto',{getRandomValues:(bytes:Uint8Array)=>bytes.fill(171)});
  expect(newRequestKey()).toBe('ab'.repeat(16));
  vi.stubGlobal('crypto',undefined);
  expect(newRequestKey()).toMatch(/^[0-9a-f]{32}$/);
});
test('cart changed during reservation never receives the code',async()=>{
  const api=fixture();let resolve!:(value:Response)=>void;
  const fetcher=vi.fn().mockResolvedValueOnce(reply(status)).mockImplementationOnce(()=>new Promise(r=>{resolve=r;}));
  stubFetch(fetcher);render(<RewardView api={api}/>);
  fireEvent.click(await screen.findByText('Use 500 points for $10.00 off'));
  await waitFor(()=>expect(fetcher).toHaveBeenCalledTimes(2));
  api.cart!.current.value.customer!.id=999;
  resolve(reply({redemption_id:1,state:'ready',code:'DEGEN-'+'A'.repeat(32)}));
  await screen.findByText(/Cart changed/);
  expect(api.cart!.addCartCodeDiscount).not.toHaveBeenCalled();
});
test('uncertain request retries with same idempotency key',async()=>{
  const api=fixture();const fetcher=vi.fn().mockResolvedValueOnce(reply(status))
    .mockRejectedValueOnce(Error('Network failed')).mockResolvedValueOnce(reply({redemption_id:1,state:'ready',code:'DEGEN-'+'A'.repeat(32)}));
  stubFetch(fetcher);render(<RewardView api={api}/>);
  fireEvent.click(await screen.findByText('Use 500 points for $10.00 off'));
  await screen.findByText('Network failed');
  fireEvent.click(screen.getByText('Use 500 points for $10.00 off'));
  await waitFor(()=>expect(api.cart!.addCartCodeDiscount).toHaveBeenCalledOnce());
  expect(fetcher.mock.calls[1][1].body).toBe(fetcher.mock.calls[2][1].body);
});

test('waits for delayed cart confirmation before announcing success',async()=>{
  const api=fixture();
  vi.mocked(api.cart!.addCartCodeDiscount).mockImplementation(async code=>{
    setTimeout(()=>{api.cart!.current.value.cartDiscounts=[{amount:10,discountDescription:code}];},200);
  });
  stubFetch(vi.fn().mockResolvedValueOnce(reply(status)).mockResolvedValueOnce(reply({
    redemption_id:1,state:'ready',code:'DEGEN-'+'A'.repeat(32)})));
  render(<RewardView api={api}/>);
  fireEvent.click(await screen.findByText('Use 500 points for $10.00 off'));
  await waitFor(()=>expect(api.cart!.addCartCodeDiscount).toHaveBeenCalledOnce());
  expect(screen.queryByText(/Reward applied/)).toBeNull();
  expect(await screen.findByText(/Reward applied/)).toBeTruthy();
});
test('cart changes during application cannot announce success',async()=>{
  const api=fixture();
  vi.mocked(api.cart!.addCartCodeDiscount).mockImplementation(async()=>{api.cart!.current.value.customer!.id=999;});
  stubFetch(vi.fn().mockResolvedValueOnce(reply(status)).mockResolvedValueOnce(reply({
    redemption_id:1,state:'ready',code:'DEGEN-'+'A'.repeat(32)})));
  render(<RewardView api={api}/>);
  fireEvent.click(await screen.findByText('Use 500 points for $10.00 off'));
  expect(await screen.findByText(/Discount could not be verified/)).toBeTruthy();
  expect(screen.queryByText(/Reward applied/)).toBeNull();
});
test('paused issuance keeps an existing reward visible for recovery',async()=>{
  const api=fixture();
  stubFetch(vi.fn().mockResolvedValueOnce(reply({...status,issuance_enabled:false,
    pending:{redemption_id:1,state:'ready',request_key:'existing_request_001'}})));
  render(<RewardView api={api}/>);
  expect(await screen.findByText('Check redemption')).toBeTruthy();
  expect(screen.getByText('Use 500 points for $10.00 off').hasAttribute('disabled')).toBe(true);
});

test('incorrect applied discount never produces a success message',async()=>{
  const api=fixture();
  vi.mocked(api.cart!.addCartCodeDiscount).mockImplementation(async code=>{
    api.cart!.current.value.cartDiscounts=[{amount:20,discountDescription:code}];
  });
  stubFetch(vi.fn().mockResolvedValueOnce(reply(status)).mockResolvedValueOnce(reply({
    redemption_id:1,state:'ready',code:'DEGEN-'+'A'.repeat(32)})));
  render(<RewardView api={api}/>);
  fireEvent.click(await screen.findByText('Use 500 points for $10.00 off'));
  // The message appears after a 3-second verification window; leave headroom on slow machines.
  expect(await screen.findByText(/Discount could not be verified/,{}, {timeout:8000})).toBeTruthy();
  expect(screen.queryByText(/Reward applied/)).toBeNull();
},12000);
test('another-register hold explains where to recover it',async()=>{
  stubFetch(vi.fn().mockResolvedValueOnce(reply({...status,reserved_points:500,available_points:0,pending:null})));
  render(<RewardView api={fixture()}/>);
  expect(await screen.findByText(/Points are reserved on another register/)).toBeTruthy();
});

test('custom-sale carts reject services even when merchandise reaches the minimum',()=>{
  const api=fixture();const cart=api.cart!.current.value;
  cart.lineItems[0].price=99.99;
  cart.lineItems.push({...cart.lineItems[0],uuid:'service',variantId:12,price:100});
  cart.lineItems.push({...cart.lineItems[0],uuid:'custom',variantId:undefined,price:100});
  cart.subtotal='299.99';
  expect(canRedeem(api,10000,['gid://shopify/ProductVariant/11'])).toBe(false);
  cart.lineItems[0].price=100;cart.subtotal='300.00';
  expect(canRedeem(api,10000,['gid://shopify/ProductVariant/11'])).toBe(false);
});

test('an ineligible preflight never reserves points or applies a code',async()=>{
  const api=fixture();const cart=api.cart!.current.value;
  cart.lineItems[0].price=50;
  cart.lineItems.push({...cart.lineItems[0],uuid:'service',variantId:12,price:50});
  const fetcher=vi.fn().mockResolvedValueOnce(reply(status)).mockResolvedValueOnce(reply({
    eligible_variant_ids:['gid://shopify/ProductVariant/11'],taxes_included:false}));
  vi.stubGlobal('fetch',fetcher);render(<RewardView api={api}/>);
  fireEvent.click(await screen.findByText('Use 500 points for $10.00 off'));
  expect(await screen.findByText(/At least \$100\.00 of eligible physical merchandise/)).toBeTruthy();
  expect(fetcher).toHaveBeenCalledTimes(2);
  expect(JSON.parse(fetcher.mock.calls[1][1].body).action).toBe('eligibility');
  expect(api.cart!.addCartCodeDiscount).not.toHaveBeenCalled();
});

test('a cart change during preflight cannot reserve points',async()=>{
  const api=fixture();let resolve!:(value:Response)=>void;
  const fetcher=vi.fn().mockResolvedValueOnce(reply(status)).mockImplementationOnce(()=>new Promise(r=>{resolve=r;}));
  vi.stubGlobal('fetch',fetcher);render(<RewardView api={api}/>);
  fireEvent.click(await screen.findByText('Use 500 points for $10.00 off'));
  await waitFor(()=>expect(fetcher).toHaveBeenCalledTimes(2));
  api.cart!.current.value.customer!.id=202;
  resolve(reply({eligible_variant_ids:['gid://shopify/ProductVariant/11'],taxes_included:false}));
  expect(await screen.findByText(/Cart changed. Check the cart/)).toBeTruthy();
  expect(fetcher).toHaveBeenCalledTimes(2);
  expect(api.cart!.addCartCodeDiscount).not.toHaveBeenCalled();
});

test('changed server eligibility between preflight and reserve cannot apply',async()=>{
  const api=fixture();stubFetch(vi.fn().mockResolvedValueOnce(reply(status)).mockResolvedValueOnce(reply({
    redemption_id:1,state:'ready',code:'DEGEN-'+'A'.repeat(32),eligible_variant_ids:['gid://shopify/ProductVariant/12']})));
  render(<RewardView api={api}/>);
  fireEvent.click(await screen.findByText('Use 500 points for $10.00 off'));
  expect(await screen.findByText(/Eligibility changed/)).toBeTruthy();
  expect(api.cart!.addCartCodeDiscount).not.toHaveBeenCalled();
});

test('hung token times out and late resolution cannot fetch',async()=>{
  const api=fixture();let resolve!:(value:string)=>void;
  api.session.getSessionToken=()=>new Promise(r=>{resolve=r;});
  const fetcher=vi.fn();stubFetch(fetcher);vi.useFakeTimers();
  try {
    render(<RewardView api={api}/>);
    await vi.advanceTimersByTimeAsync(45001);
    expect(screen.getByText(/Reward request timed out/)).toBeTruthy();
    resolve('late-token');await vi.advanceTimersByTimeAsync(1);
    expect(fetcher).not.toHaveBeenCalled();
  } finally {vi.useRealTimers();}
});

test('hung discount releases busy state without a second application',async()=>{
  const api=fixture();let resolve!:()=>void;
  vi.mocked(api.cart!.addCartCodeDiscount).mockImplementation(()=>new Promise<void>(r=>{resolve=r;}));
  stubFetch(vi.fn().mockResolvedValueOnce(reply(status)).mockResolvedValue(reply({
    redemption_id:1,state:'ready',can_cancel:true,code:'DEGEN-'+'A'.repeat(32)})));
  render(<RewardView api={api}/>);
  const button=await screen.findByText('Use 500 points for $10.00 off');vi.useFakeTimers();
  try {
    fireEvent.click(button);await vi.advanceTimersByTimeAsync(3001);
    expect(screen.getByText(/Discount application timed out/)).toBeTruthy();
    expect(screen.getByText('Check redemption').hasAttribute('disabled')).toBe(false);
    expect(screen.getByText('Cancel this reward').hasAttribute('disabled')).toBe(false);
    fireEvent.click(button);expect(api.cart!.addCartCodeDiscount).toHaveBeenCalledOnce();
    resolve();await vi.advanceTimersByTimeAsync(1);
    expect(screen.queryByText(/Reward applied/)).toBeNull();
    expect(api.cart!.addCartCodeDiscount).toHaveBeenCalledOnce();
  } finally {vi.useRealTimers();}
});

test('custom sales qualify alone or alongside catalog merchandise, but not below minimum',()=>{
  const api=fixture();const cart=api.cart!.current.value;
  cart.lineItems[0].variantId=undefined;
  expect(canRedeem(api,10000,[])).toBe(true);
  cart.subtotal='99.99';cart.lineItems[0].price=99.99;
  expect(canRedeem(api,10000,[])).toBe(false);
  cart.lineItems[0].price=50;cart.subtotal='100.00';
  cart.lineItems.push({...cart.lineItems[0],uuid:'catalog',variantId:11});
  expect(canRedeem(api,10000,['gid://shopify/ProductVariant/11'])).toBe(true);
  cart.lineItems.push({...cart.lineItems[0],uuid:'gift',isGiftCard:true});
  cart.subtotal='150.00';
  expect(canRedeem(api,10000,['gid://shopify/ProductVariant/11'])).toBe(false);
});

test('custom-only reward waits for the cashier and verifies explicit custom scope',async()=>{
  const api=fixture();api.cart!.current.value.lineItems[0].variantId=undefined;
  const code='DEGEN-'+'B'.repeat(32);
  const fetcher=vi.fn().mockResolvedValueOnce(reply(status))
    .mockResolvedValueOnce(reply({eligible_variant_ids:[],custom_sales:true,taxes_included:false}))
    .mockResolvedValueOnce(reply({redemption_id:1,state:'ready',code,eligible_variant_ids:[],custom_sales:true}));
  vi.stubGlobal('fetch',fetcher);render(<RewardView api={api}/>);
  const button=await screen.findByText('Use 500 points for $10.00 off');
  expect(fetcher).toHaveBeenCalledTimes(1);
  expect(api.cart!.addCartCodeDiscount).not.toHaveBeenCalled();
  fireEvent.click(button);
  expect(await screen.findByText(/Reward applied/)).toBeTruthy();
  expect(api.cart!.addCartCodeDiscount).toHaveBeenCalledExactlyOnceWith(code);
  expect(JSON.parse(fetcher.mock.calls[1][1].body)).toEqual({action:'eligibility',variant_ids:[],custom_sales:true});
  expect(JSON.parse(fetcher.mock.calls[2][1].body)).toMatchObject({action:'reserve',variant_ids:[],custom_sales:true});
});
