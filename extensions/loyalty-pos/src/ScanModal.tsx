import '@shopify/ui-extensions/preact';
import {render} from 'preact';
import type {Api} from '@shopify/ui-extensions/pos.home.modal.render';
import {MemberScan} from './MemberScan';
export default function extension() {
  render(<MemberScan api={shopify as Api}/>,document.body);
}
