import {useEffect, useMemo, useState} from 'preact/hooks';
import type {Api} from '@shopify/ui-extensions/pos.cart.line-item-details.action.render';
import type {ContextApi} from './LoyaltyView';
import {RewardView} from './RewardView';

export function CartRewards({api}:{api:Api}) {
  const [customer,setCustomer]=useState(api.cart.current.value.customer?.id);
  useEffect(()=>api.cart.current.subscribe(cart=>setCustomer(cart.customer?.id)),[api]);
  const context=useMemo<ContextApi>(()=>({
    get customer(){return {id:api.cart.current.value.customer?.id ?? 0};},
    cart:api.cart,session:api.session,connectivity:api.connectivity,
  }),[api]);
  return <s-page heading="Redeem rewards">
    {!customer ? <s-text>Add a customer to this cart, then reopen rewards.</s-text> :
      <><s-text>This reward applies to the whole eligible cart.</s-text>
        <RewardView key={customer} api={context}/></>}
  </s-page>;
}
