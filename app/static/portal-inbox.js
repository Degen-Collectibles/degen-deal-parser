/* Employee Inbox (/team/inbox) — progressive enhancement only.
 *
 * Announcements and updates are form POSTs to /team/inbox/open and need no
 * JavaScript. This file:
 *   - marks a document read in the background when its link is opened (the
 *     link itself opens in a new tab, so the page can't POST-and-redirect),
 *     then clears its unread dot;
 *   - drives the "Browser alerts" row (moved here from the old
 *     /team/notifications page): shows the permission state and asks for
 *     permission on "Turn on".
 */
(function () {
  "use strict";

  var root = document.querySelector("[data-inbox]");
  if (!root) return;

  var readUrl = root.getAttribute("data-read-url") || "/team/inbox/read";
  var csrf = root.getAttribute("data-csrf") || "";

  function markRowRead(row) {
    row.classList.remove("is-unread");
    row.removeAttribute("data-inbox-unread");
    var dot = row.querySelector(".pt-inbox-dot");
    if (dot) dot.textContent = "";
  }

  function postRead(kind, key) {
    if (!window.fetch || !window.FormData) return;
    var body = new window.FormData();
    body.append("kind", kind);
    body.append("key", key);
    body.append("csrf_token", csrf);
    try {
      window.fetch(readUrl, {
        method: "POST",
        credentials: "same-origin",
        headers: { "X-CSRF-Token": csrf, "Accept": "application/json" },
        body: body,
        keepalive: true
      }).catch(function () {});
    } catch (err) {
      // Best effort: the document still opens.
    }
  }

  root.addEventListener("click", function (event) {
    var link = event.target.closest ? event.target.closest("a[data-inbox-kind]") : null;
    if (!link || !link.hasAttribute("data-inbox-unread")) return;
    postRead(link.getAttribute("data-inbox-kind"), link.getAttribute("data-inbox-key"));
    markRowRead(link);
  });

  // ---- Browser alerts row ----
  var pushRow = root.querySelector("[data-push-row]");
  if (!pushRow) return;
  var pushStatus = pushRow.querySelector("[data-push-status]");
  var pushBtn = pushRow.querySelector("[data-push-enable]");

  function renderPush() {
    if (!("Notification" in window)) {
      if (pushStatus) pushStatus.textContent = "Not available in this browser.";
      if (pushBtn) pushBtn.hidden = true;
      return;
    }
    var state = window.Notification.permission;
    if (pushStatus) {
      pushStatus.textContent =
        state === "granted" ? "On for this device while the portal is open." :
        state === "denied" ? "Blocked in this browser's site settings." :
        "Off on this device.";
    }
    if (pushBtn) pushBtn.hidden = state !== "default";
  }

  if (pushBtn) {
    pushBtn.addEventListener("click", function () {
      if (!("Notification" in window)) return;
      var asked = window.Notification.requestPermission(function () { renderPush(); });
      if (asked && asked.then) {
        asked.then(function () {
          renderPush();
          if (window.Notification.permission === "granted") {
            try {
              new window.Notification("Degen Team alerts are on", {
                body: "You'll get alerts on this device while the portal is open."
              });
            } catch (err) {
              // Some mobile browsers only allow notifications from a service worker.
            }
          }
        });
      }
    });
  }
  renderPush();
})();
