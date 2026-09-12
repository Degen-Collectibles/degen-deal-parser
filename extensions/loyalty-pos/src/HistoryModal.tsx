import '@shopify/ui-extensions/preact';
import {render} from 'preact';
import type {Api} from '@shopify/ui-extensions/pos.customer-details.action.render';
import {LoyaltyView} from './LoyaltyView';
export default function extension() {
  render(<LoyaltyView api={shopify as Api} mode="history"/>, document.body);
}
