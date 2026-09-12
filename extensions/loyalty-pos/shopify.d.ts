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
declare module './src/LoyaltyView.tsx' {
  const shopify:
    | import('@shopify/ui-extensions/pos.customer-details.block.render').Api
    | import('@shopify/ui-extensions/pos.customer-details.action.render').Api;
  const globalThis: { shopify: typeof shopify };
}
