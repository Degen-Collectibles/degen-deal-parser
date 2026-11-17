import '@shopify/ui-extensions/preact';
import {render} from 'preact';
import type {Api} from '@shopify/ui-extensions/pos.cart.line-item-details.action.menu-item.render';
export default function extension() {
  const api=shopify as Api;
  render(<s-button onClick={()=>api.action.presentModal()}>Redeem rewards</s-button>,document.body);
}
