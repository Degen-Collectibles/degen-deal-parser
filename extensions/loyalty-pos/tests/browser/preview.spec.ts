import {test,expect} from '@playwright/test';
test('synthetic component flow, customer/PIN switching and mobile layout',async({page,baseURL})=>{
  const evidence=test.info().outputPath('loyalty-pos-presentation');
  const errors:string[]=[];page.on('pageerror',error=>errors.push(error.message));
  await page.route('**/*',route=>new URL(route.request().url()).origin===new URL(baseURL!).origin?route.continue():route.abort());
  await page.goto('/');await expect(page.getByText('20 points',{exact:true})).toBeVisible();
  await expect(page.getByText(/not native Shopify POS/)).toBeVisible();
  await expect(page.locator('s-pos-block s-button')).toHaveCount(1);
  await expect(page.getByText('Refresh',{exact:true})).toHaveCount(0);
  await page.locator('s-pos-block').screenshot({path:evidence+'/block-desktop.png'});
  await page.getByText('View history',{exact:true}).click();await expect(page.getByText('Purchase points',{exact:true})).toBeVisible();
  await page.getByLabel('Sample customer').selectOption('102');
  await expect(page.getByText('19 points',{exact:true})).toBeVisible();await expect(page.getByText('Refund adjustment',{exact:true})).toBeVisible();
  await page.screenshot({path:evidence+'/desktop.png',fullPage:true});
  await page.getByLabel('Sample customer').selectOption('103');await expect(page.getByText('No posted points yet.')).toBeVisible();await expect(page.getByText(/Updates pending/)).toBeVisible();
  await page.getByLabel('Sample customer').selectOption('104');await expect(page.getByText(/Order needs review/)).toBeVisible();
  await expect(page.getByText('Ask an Ops administrator to review this customer’s order.')).toBeVisible();
  await page.getByLabel('Simulate offline').check();await expect(page.getByText('Loyalty is unavailable. Try again when connected.')).toBeVisible();await expect(page.getByText('12 points',{exact:true})).toHaveCount(0);
  await page.getByLabel('Sample customer').selectOption('102');
  await expect(page.getByText('19 points',{exact:true})).toHaveCount(0);
  await page.getByLabel('Simulate offline').uncheck();await page.getByLabel('Simulated PIN context').selectOption('8');await expect(page.getByText('19 points',{exact:true})).toBeVisible();
  await expect(page.getByText('12 points',{exact:true})).toHaveCount(0);
  await page.getByLabel('Sample customer').selectOption('105');await expect(page.getByText('15 points',{exact:true})).toBeVisible();await expect(page.getByText('Purchase points',{exact:true})).toHaveCount(10);
  await page.getByText('Older activity',{exact:true}).click();await expect(page.getByText('Purchase points',{exact:true})).toHaveCount(5);
  await page.setViewportSize({width:390,height:844});await page.getByLabel('Sample customer').selectOption('102');await expect(page.getByText('19 points',{exact:true})).toBeVisible();
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth)).toBe(true);
  await page.screenshot({path:evidence+'/mobile.png',fullPage:true});
  await page.getByText('Back to customer',{exact:true}).click();
  await expect(page.locator('s-pos-block').getByText('19 points',{exact:true})).toBeVisible();
  await page.locator('s-pos-block').screenshot({path:evidence+'/block-mobile.png'});
  expect(errors).toEqual([]);
});

test('loopback preview rejects foreign origins and hosts',async({request})=>{
  expect((await request.post('/demo/token')).status()).toBe(403);
  expect((await request.post('/demo/token',{headers:{Origin:'https://example.invalid','X-Loyalty-Demo':'synthetic'}})).status()).toBe(403);
  expect((await request.get('/',{headers:{Host:'example.invalid'}})).status()).toBe(403);
});
