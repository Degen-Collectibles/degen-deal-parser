import '@shopify/ui-extensions/preact';
import {render} from 'preact';
import type {Api} from '@shopify/ui-extensions/pos.cart.line-item-details.action.render';
import {CartRewards} from './CartRewards';
export default function extension() {
  render(<CartRewards api={shopify as Api}/>,document.body);
}
