import '@shopify/ui-extensions';

//@ts-ignore
declare module './src/CustomerBlock.tsx' {
  const shopify: import('@shopify/ui-extensions/pos.customer-details.block.render').Api;
  const globalThis: { shopify: typeof shopify };
}

//@ts-ignore
declare module './src/HistoryModal.tsx' {
  const shopify: import('@shopify/ui-extensions/pos.customer-details.action.render').Api;
  const globalThis: { shopify: typeof shopify };
}

//@ts-ignore
declare module './src/CartRewardAction.tsx' {
  const shopify: import('@shopify/ui-extensions/pos.cart.line-item-details.action.menu-item.render').Api;
  const globalThis: { shopify: typeof shopify };
}

//@ts-ignore
declare module './src/CartRewardModal.tsx' {
  const shopify: import('@shopify/ui-extensions/pos.cart.line-item-details.action.render').Api;
  const globalThis: { shopify: typeof shopify };
}

//@ts-ignore
declare module './src/LoyaltyView.tsx' {
  const shopify:
    | import('@shopify/ui-extensions/pos.customer-details.block.render').Api
    | import('@shopify/ui-extensions/pos.customer-details.action.render').Api
    | import('@shopify/ui-extensions/pos.cart.line-item-details.action.render').Api;
  const globalThis: { shopify: typeof shopify };
}

//@ts-ignore
declare module './src/RewardView.tsx' {
  const shopify:
    | import('@shopify/ui-extensions/pos.customer-details.block.render').Api
    | import('@shopify/ui-extensions/pos.customer-details.action.render').Api
    | import('@shopify/ui-extensions/pos.cart.line-item-details.action.render').Api;
  const globalThis: { shopify: typeof shopify };
}

//@ts-ignore
declare module './src/CartRewards.tsx' {
  const shopify: import('@shopify/ui-extensions/pos.cart.line-item-details.action.render').Api;
  const globalThis: { shopify: typeof shopify };
}

//@ts-ignore
declare module './src/ScanTile.tsx' {
  const shopify: import('@shopify/ui-extensions/pos.home.tile.render').Api;
  const globalThis: { shopify: typeof shopify };
}

//@ts-ignore
declare module './src/ScanModal.tsx' {
  const shopify: import('@shopify/ui-extensions/pos.home.modal.render').Api;
  const globalThis: { shopify: typeof shopify };
}

//@ts-ignore
declare module './src/MemberScan.tsx' {
  const shopify: import('@shopify/ui-extensions/pos.home.modal.render').Api;
  const globalThis: { shopify: typeof shopify };
}
