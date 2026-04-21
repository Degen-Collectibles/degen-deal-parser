/* linear-drawer.js — mobile slide-in drawer for the linear sidebar */
(function () {
    var OPEN_CLASS = 'linear-drawer-open';
    var FOCUSABLE = 'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';
    /* IDs that must stay interactive while drawer is open — the sidebar, its
       topbar trigger, and the backdrop (which need to receive taps to close).
       Historically _linear_sidebar.html is included INSIDE the template's
       outer .page/.lx-page wrapper on most pages, so naively setting inert
       on .page would also disable the sidebar/close-btn/backdrop — leaving
       the drawer visible but completely unresponsive (hamburger, close and
       backdrop all stop receiving taps). */
    var KEEP_LIVE_IDS = ['linear-sidebar', 'linear-mobile-topbar', 'linear-drawer-backdrop'];
    var prevFocus = null;
    var keydownHandler = null;
    var inertedEls = [];

    function isAncestorOfKeepLive(el) {
        for (var i = 0; i < KEEP_LIVE_IDS.length; i++) {
            var node = document.getElementById(KEEP_LIVE_IDS[i]);
            if (node && (node === el || el.contains(node))) return true;
        }
        return false;
    }

    function collectInertTargets() {
        /* For each .lx-page / .page root: if it wraps the sidebar/topbar/
           backdrop, inert only the siblings that do NOT wrap them. Otherwise
           inert the root itself. */
        var roots = document.querySelectorAll('.lx-page, .page');
        var targets = [];
        roots.forEach(function (root) {
            if (isAncestorOfKeepLive(root)) {
                for (var i = 0; i < root.children.length; i++) {
                    var child = root.children[i];
                    if (KEEP_LIVE_IDS.indexOf(child.id) !== -1) continue;
                    if (isAncestorOfKeepLive(child)) continue;
                    targets.push(child);
                }
            } else {
                targets.push(root);
            }
        });
        return targets;
    }

    function trapTab(e, sidebar) {
        var nodes = sidebar.querySelectorAll(FOCUSABLE);
        if (!nodes.length) return;
        var first = nodes[0];
        var last = nodes[nodes.length - 1];
        var active = document.activeElement;
        if (e.shiftKey) {
            if (active === first || !sidebar.contains(active)) { last.focus(); e.preventDefault(); }
        } else {
            if (active === last) { first.focus(); e.preventDefault(); }
        }
    }

    function openDrawer() {
        document.body.classList.add(OPEN_CLASS);
        var hamburger = document.getElementById('linear-hamburger');
        if (hamburger) hamburger.setAttribute('aria-expanded', 'true');
        var sidebar = document.getElementById('linear-sidebar');
        prevFocus = document.activeElement;

        inertedEls = collectInertTargets();
        inertedEls.forEach(function (el) {
            el.setAttribute('inert', '');
            el.setAttribute('aria-hidden', 'true');
        });

        if (sidebar) {
            var firstFocusable = sidebar.querySelector(FOCUSABLE);
            if (firstFocusable) firstFocusable.focus();

            keydownHandler = function (e) {
                if (e.key === 'Tab') trapTab(e, sidebar);
            };
            document.addEventListener('keydown', keydownHandler);
        }
    }

    function closeDrawer() {
        document.body.classList.remove(OPEN_CLASS);
        var hamburger = document.getElementById('linear-hamburger');
        if (hamburger) hamburger.setAttribute('aria-expanded', 'false');

        inertedEls.forEach(function (el) {
            el.removeAttribute('inert');
            el.removeAttribute('aria-hidden');
        });
        inertedEls = [];

        if (keydownHandler) {
            document.removeEventListener('keydown', keydownHandler);
            keydownHandler = null;
        }

        if (prevFocus) {
            prevFocus.focus();
            prevFocus = null;
        }
    }

    function init() {
        var hamburger = document.getElementById('linear-hamburger');
        if (hamburger) hamburger.addEventListener('click', openDrawer);

        var backdrop = document.getElementById('linear-drawer-backdrop');
        if (backdrop) backdrop.addEventListener('click', closeDrawer);

        var closeBtn = document.getElementById('linear-drawer-close');
        if (closeBtn) closeBtn.addEventListener('click', closeDrawer);

        document.addEventListener('keydown', function (e) {
            if (e.key === 'Escape' && document.body.classList.contains(OPEN_CLASS)) {
                closeDrawer();
            }
        });

        /* Close drawer on any nav link tap */
        var items = document.querySelectorAll('.linear-sidebar-item');
        for (var i = 0; i < items.length; i++) {
            items[i].addEventListener('click', closeDrawer);
        }
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
