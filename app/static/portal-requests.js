/* Employee Requests page (/team/requests) — progressive enhancement only.
 *
 * The server renders every form as a bottom sheet and opens the one named in
 * the URL (?new=timeoff, ?edit=supply&id=3), so the page works without this
 * file. With it:
 *   - "Time off" / "Supplies" / "Edit" open their sheet in place,
 *   - the backdrop, "Close" and Escape close it,
 *   - time-off dates are checked as you pick them (end >= start, not past),
 *   - the "you're scheduled" warning refreshes from /team/requests/overlap,
 *   - "Cancel request" asks for confirmation first.
 */
(function () {
  "use strict";

  var root = document.querySelector("[data-requests]");
  if (!root) return;

  var backdrop = document.querySelector("[data-sheet-bg]");
  var overlapUrl = root.getAttribute("data-overlap-url") || "";
  var closeHref = root.getAttribute("data-close-href") || "/team/requests";
  var current = null;
  var opener = null;

  function openSheet(sheet, trigger) {
    if (!sheet) return false;
    if (current && current !== sheet) closeSheet(false);
    current = sheet;
    opener = trigger || null;
    sheet.hidden = false;
    // Next frame so the slide-up transition runs from the closed position.
    window.requestAnimationFrame(function () {
      sheet.classList.add("is-open");
      if (backdrop) backdrop.classList.add("is-open");
    });
    document.body.classList.add("pt-sheet-lock");
    var first = sheet.querySelector("input:not([type=hidden]), textarea, select");
    if (first) {
      window.setTimeout(function () {
        try { first.focus({ preventScroll: true }); } catch (e) { first.focus(); }
      }, 60);
    }
    return true;
  }

  function closeSheet(restoreFocus) {
    var sheet = current;
    if (!sheet) return;
    current = null;
    sheet.classList.remove("is-open");
    if (backdrop) backdrop.classList.remove("is-open");
    document.body.classList.remove("pt-sheet-lock");
    window.setTimeout(function () {
      if (!sheet.classList.contains("is-open")) sheet.hidden = true;
    }, 360);
    // A server-opened sheet (?new=…) leaves its query in the URL; drop it so
    // a reload doesn't pop the form back up.
    if (window.history && window.history.replaceState && /[?&](new|edit)=/.test(window.location.search)) {
      window.history.replaceState(null, "", closeHref);
    }
    if (restoreFocus !== false && opener && opener.focus) opener.focus();
    opener = null;
  }

  document.addEventListener("click", function (event) {
    var trigger = event.target.closest("[data-sheet-open]");
    if (trigger) {
      var sheet = document.getElementById(trigger.getAttribute("data-sheet-open"));
      if (openSheet(sheet, trigger)) event.preventDefault();
      return;
    }
    if (event.target.closest("[data-sheet-close]") || event.target.closest("[data-sheet-bg]")) {
      if (current) {
        event.preventDefault();
        closeSheet(true);
      }
    }
  });

  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape" && current) closeSheet(true);
  });

  // Adopt a sheet the server rendered open.
  var serverOpen = document.querySelector("[data-sheet].is-open");
  if (serverOpen) {
    current = serverOpen;
    document.body.classList.add("pt-sheet-lock");
  }

  // Confirm before cancelling a request.
  document.querySelectorAll("form[data-confirm]").forEach(function (form) {
    form.addEventListener("submit", function (event) {
      if (!window.confirm(form.getAttribute("data-confirm"))) event.preventDefault();
    });
  });

  // ---- Time-off date checks + overlap warning ----
  function renderOverlap(form, data) {
    var box = form.querySelector("[data-overlap]");
    if (!box) return;
    var list = box.querySelector("[data-overlap-list]");
    var summary = box.querySelector("[data-overlap-summary]");
    var shifts = (data && data.ok && data.shifts) || [];
    if (!shifts.length) {
      box.hidden = true;
      return;
    }
    summary.textContent = data.summary || "";
    list.textContent = "";
    shifts.forEach(function (shift) {
      var li = document.createElement("li");
      li.textContent = shift.text;
      list.appendChild(li);
    });
    box.hidden = false;
  }

  function wireTimeoff(form) {
    var start = form.querySelector("[data-timeoff-start]");
    var end = form.querySelector("[data-timeoff-end]");
    var errorBox = form.querySelector("[data-date-error]");
    if (!start || !end) return;
    var today = start.getAttribute("min") || "";
    var seq = 0;

    function problem() {
      if (!start.value || !end.value) return "";
      if (today && start.value < today) return "The first day off can't be in the past.";
      if (end.value < start.value) return "The last day off must be on or after the first day.";
      return "";
    }

    function check() {
      // ISO dates compare correctly as strings.
      end.min = start.value && start.value > today ? start.value : today;
      var msg = problem();
      end.setCustomValidity(msg);
      if (errorBox) {
        errorBox.textContent = msg;
        errorBox.hidden = !msg;
      }
      return !msg;
    }

    function refreshOverlap() {
      if (!overlapUrl || !window.fetch) return;
      if (!start.value || !check()) {
        renderOverlap(form, null);
        return;
      }
      var mine = ++seq;
      var qs = "?start=" + encodeURIComponent(start.value) +
        "&end=" + encodeURIComponent(end.value || start.value);
      window.fetch(overlapUrl + qs, {
        credentials: "same-origin",
        headers: { "Accept": "application/json" }
      }).then(function (res) {
        return res.ok ? res.json() : null;
      }).then(function (data) {
        if (mine === seq) renderOverlap(form, data);
      }).catch(function () { /* the server re-checks nothing here; warning only */ });
    }

    start.addEventListener("change", function () {
      // Picking a first day with no last day yet: assume a single day.
      if (start.value && (!end.value || end.value < start.value)) end.value = start.value;
      check();
      refreshOverlap();
    });
    end.addEventListener("change", function () {
      check();
      refreshOverlap();
    });
    form.addEventListener("submit", function (event) {
      if (!check()) {
        event.preventDefault();
        end.reportValidity();
      }
    });
    check();
  }

  document.querySelectorAll("form[data-timeoff-form]").forEach(wireTimeoff);
})();
