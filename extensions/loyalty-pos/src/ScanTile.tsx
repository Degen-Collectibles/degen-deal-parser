import '@shopify/ui-extensions/preact';
import {render} from 'preact';
import type {Api} from '@shopify/ui-extensions/pos.home.tile.render';
export default function extension() {
  const api=shopify as Api;
  render(<s-tile heading="Scan rewards card" subheading="Add the customer to the cart" onClick={()=>api.action.presentModal()}/>,document.body);
}
