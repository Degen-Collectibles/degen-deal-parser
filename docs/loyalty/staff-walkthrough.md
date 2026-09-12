# Loyalty staff quickstart and local walkthrough

## Cashier quickstart — after an approved launch

Production loyalty is still OFF. These are the intended employee steps; the current preview uses synthetic points.

1. In Shopify POS, find the customer's **existing profile using email OR phone** and attach it to the sale. Do not create a second profile or a separate loyalty account. If the existing profile cannot be identified, ask the responsible Ops administrator for help.
2. Open that customer's details and find **Loyalty points**. Tap **View history** for purchases and refund adjustments. POS supplies the customer automatically: no manual Shopify ID lookup, extra Ops login or extra loyalty PIN is needed.
3. Complete the eligible POS purchase normally. Points are posted asynchronously after Shopify evidence is checked. Open history and use **Refresh** when needed; do not promise that an unfinished or just-synced sale is already credited. “As of” is the ledger-read time, not proof that every recent order has been checked.

All cashiers may view balances/history under Jeffrey's approved app/shop policy. Corrections stay in authorized Ops controls. The normal Shopify POS login/PIN workflow is unchanged; loyalty does not verify a PIN person's identity or use it as an authorization boundary. Email/phone are only used inside Shopify to locate the existing profile; loyalty does not copy contacts or deny later points because a contact field disappears.

| What staff sees | What to do |
|---|---|
| Balance and activity | Read whole points. Purchase rows add points; refund adjustments change the previous award. There is no redemption or reward discount in this release. |
| No posted points yet | No points have been posted for this profile. Check that the correct existing customer is attached; do not create a new account or promise historical credit. |
| Updates pending | Allow the Shopify check to finish, then Refresh in history. If still pending, refer the order to Ops. |
| Order needs review | History directs staff to an Ops administrator. Give them the affected order/customer context through the existing internal workflow; staff do not edit points or troubleshoot JSON. |
| Updates paused | Existing posted points are visible, but new changes are paused. Ask Ops about the pause; do not promise a new award. This is expected in the disabled synthetic preview. |
| Unavailable / access unavailable | A failed read is not a zero balance. Reconnect and Refresh; if access still fails, ask an administrator to check app access. Do not change accounts to bypass the error. |
| Activity changed / wait a minute | Refresh to start current history, or wait the requested minute before retrying. |

Eligible USD merchandise earns one point per dollar after discounts, rounded on the cumulative net order amount with halves up. Taxes, shipping, tips, fees and gift-card issuance do not earn; qualifying merchandise paid by gift card can earn. A refund changes the order's net entitlement, so two small refunds can together cause a one-point adjustment. Shopify online is excluded at launch; TikTok remains permanently excluded. Only purchases after the approved launch boundary qualify. A missed customer attachment has a seven-day follow-up window; attach the existing customer promptly and send older/uncertain timing to Ops for evidence review. Changing an order's customer must never be used to move points.

## Current preview and verification

Open **http://127.0.0.1:8767/** from Windows for the existing synthetic POS component preview. Select **Refund · 19 points**, then **View history**: the actual read endpoint/components show **19 points**, **−1 Refund adjustment** and **+20 Purchase points**. The demo-only picker, offline and PIN-context controls are not employee screens and are not present in the production extension.

September 11 automated checks passed: customer/session isolation (including changes during authentication), clearing on offline/error, reconnect into a newly selected customer, refreshed authorization, history pagination and mobile layout. **32 component tests and 2 actual POS browser tests passed**; two additional Ops browser checks passed. Full evidence and engine-specific results are in [release-review.md](release-review.md). User-provided native screenshot separately confirms the DEV customer's 19-point balance, −1/+20 history and header Refresh. Native phone offline/reconnect/sleep/lock and customer-switch lifecycle remain unverified; no repeated phone walkthrough is requested here. The parent owns the temporary native preview; this task did not restart it.

## Optional Ops training demo — synthetic data only


From PowerShell, start the feature worktree's local-only launcher:

```powershell
Set-Location 'C:\Users\jeffr\Degen-consolidation\shopify-pos-loyalty-20260910'
& 'C:\Users\jeffr\OneDrive\Apps\Documents\Degen App\.venv\Scripts\python.exe' scripts/loyalty_demo.py
```

From the same worktree in WSL:

```bash
'/mnt/c/Users/jeffr/OneDrive/Apps/Documents/Degen App/.venv/Scripts/python.exe' scripts/loyalty_demo.py
```

Open **http://127.0.0.1:8766/demo**. If that port is occupied, add `--port 8769` and use the printed URL. The launcher binds only `127.0.0.1`, has no reload worker, and creates a fresh scratch database and synthetic home on every launch. It never loads `.env`, starts the production app lifespan, mounts provider webhooks or calls Shopify. All displayed IDs, dates, policy values, staff and balances are demo fixtures. No production activation settings are changed.

Select **Cashier** and **Earned points**, then **Open example**. The embedded panel is the actual `/loyalty` page, using real session checks, explicit role grants and CSRF. Demo login exists only in this launcher; there is no demo-login route in the production app.

| Example | What to check |
|---|---|
| Earned points | Customer 101, internal order ID 1001: $19.99 eligible merchandise and 20 points. |
| Cumulative refund | Customer 102, internal order ID 1002: two $0.25 merchandise refunds. Entitlement stays 20 after the first and becomes 19 after the second. History records +20 and −1; eligible merchandise is $19.49. |
| Pending attachment | Internal order ID 1003 starts with no customer and no points. **Simulate existing customer attachment** supplies synthetic canonical evidence for existing demo customer 103 within seven days; the actual engine awards 20 points. In real use, staff attach the existing customer in native POS using email or phone. |
| Exchange review | Customer 104 has a prior 20-point award and a later unsupported exchange. Review is visible and the prior points remain unchanged. |
| Attachment evidence review | Customer 105 was first observed too late to prove attachment timing. In Admin mode, **Verify attachment timing** accepts the synthetic timestamp printed in the demo description and a case reference such as `DEMO-case-105`. This exercises the actual audited verification form. |

Switch to **Admin** and **Earned points**. Open **Correct points**, enter target `19` and case reference `DEMO-case-101`, then submit. The real compensating entry and AuditLog are committed to the scratch database; the order enters manual hold. Cashier mode does not expose this action, and forged requests are denied by the actual permission check.

For replay, enter internal **entitlement** ID `1` in the administrator reconciliation form and queue a Shopify check. Then select **Process queued demo checks** above the panel. This demo-only control supplies the retained synthetic canonical order to the real processor; it does not call Shopify. The correction hold remains intact. The same control processes an attachment-verification job after the earlier example.

The separate Ops lookup accepts Shopify **internal order ID**, not the merchant-visible order number such as `#1001`. The small IDs in this demo are deliberately synthetic; the example picker avoids typing them. Balances, not contacts, are shown. Unrelated sidebar sections are unavailable in this isolated preview.

Use a narrow browser window to check the mobile cards. **Ctrl+C** stops the server; restarting creates fresh examples. The scratch path is printed for inspection/disposal of this synthetic run only. Do not deploy this launcher or connect its seed/process controls to a real database.
