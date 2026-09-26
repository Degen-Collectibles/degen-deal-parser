/* Employee Inbox (/team/inbox) — progressive enhancement only.
 *
 * Announcements and updates are form POSTs to /team/inbox/open and need no
 * JavaScript. This file:
 *   - marks a document read in the background when its link is opened (the
 *     link itself opens in a new tab, so the page can't POST-and-redirect),
 *     then clears its unread dot and updates every unread count on the page
 *     (eyebrow, "Mark all read", sidebar count, bottom-nav More badge) from
 *     the new total the server returns;
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

  function setCount(el, n, label) {
    if (!el) return;
    if (n > 0) {
      el.textContent = label;
      if (el.hasAttribute("aria-label")) el.setAttribute("aria-label", n + " unread");
    } else if (el.parentNode) {
      el.parentNode.removeChild(el);
    }
  }

  function renderUnread(total) {
    var n = Math.max(0, parseInt(total, 10) || 0);
    var eyebrow = document.querySelector("[data-inbox-eyebrow]");
    if (eyebrow) eyebrow.textContent = n ? n + " unread" : "All caught up";
    setCount(document.querySelector("#pt-sidebar a[href='/team/inbox'] .pt-count"), n, String(n));
    var more = document.querySelector("#pt-mobile-bottom-nav a[href='/team/more']");
    if (more) {
      setCount(more.querySelector(".pt-mbn-badge"), n, n < 100 ? String(n) : "99+");
      more.setAttribute("aria-label", n ? "More, " + n + " unread" : "More");
    }
    // The list only holds the current filter, so its unread rows are that
    // filter's count; hide "Mark all read" once none are left.
    var readAll = document.querySelector(".pt-inbox-readall");
    if (readAll && !root.querySelector(".pt-inbox-list .is-unread")) readAll.hidden = true;
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
      }).then(function (resp) {
        return resp.ok ? resp.json() : null;
      }).then(function (data) {
        if (data && typeof data.unread === "number") renderUnread(data.unread);
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
