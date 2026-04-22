/* portal-drawer.js — employee portal mobile drawer
 *
 * Mirrors linear-drawer.js but namespaced to pt-* classes so the team
 * portal and ops site can't collide.  Opens/closes the sidebar when the
 * hamburger is tapped, traps focus inside the drawer while open, and
 * closes on:
 *   - tap on backdrop
 *   - tap on any nav link inside the drawer
 *   - Escape key
 *   - viewport resize above the mobile breakpoint
 */
(function () {
    var OPEN_CLASS = 'pt-drawer-open';
    var FOCUSABLE = 'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';
    var prevFocus = null;
    var keydownHandler = null;

    function openDrawer() {
        document.body.classList.add(OPEN_CLASS);
        var hamburger = document.getElementById('pt-hamburger');
        if (hamburger) hamburger.setAttribute('aria-expanded', 'true');

        var sidebar = document.getElementById('pt-sidebar');
        prevFocus = document.activeElement;

        if (sidebar) {
            var first = sidebar.querySelector(FOCUSABLE);
            if (first) first.focus();

            keydownHandler = function (e) {
                if (e.key !== 'Tab') return;
                var nodes = sidebar.querySelectorAll(FOCUSABLE);
                if (!nodes.length) return;
                var firstEl = nodes[0];
                var lastEl = nodes[nodes.length - 1];
                var active = document.activeElement;
                if (e.shiftKey) {
                    if (active === firstEl || !sidebar.contains(active)) {
                        lastEl.focus();
                        e.preventDefault();
                    }
                } else {
                    if (active === lastEl) {
                        firstEl.focus();
                        e.preventDefault();
                    }
                }
            };
            document.addEventListener('keydown', keydownHandler);
        }
    }

    function closeDrawer() {
        document.body.classList.remove(OPEN_CLASS);
        var hamburger = document.getElementById('pt-hamburger');
        if (hamburger) hamburger.setAttribute('aria-expanded', 'false');

        if (keydownHandler) {
            document.removeEventListener('keydown', keydownHandler);
            keydownHandler = null;
        }
        if (prevFocus) {
            try { prevFocus.focus(); } catch (_e) {}
            prevFocus = null;
        }
    }

    function init() {
        var hamburger = document.getElementById('pt-hamburger');
        if (hamburger) hamburger.addEventListener('click', openDrawer);

        var backdrop = document.getElementById('pt-drawer-backdrop');
        if (backdrop) backdrop.addEventListener('click', closeDrawer);

        var closeBtn = document.getElementById('pt-drawer-close');
        if (closeBtn) closeBtn.addEventListener('click', closeDrawer);

        document.addEventListener('keydown', function (e) {
            if (e.key === 'Escape' && document.body.classList.contains(OPEN_CLASS)) {
                closeDrawer();
            }
        });

        /* Close on any sidebar nav link tap so the drawer doesn't linger */
        var links = document.querySelectorAll('.pt-side .pt-link');
        for (var i = 0; i < links.length; i++) {
            links[i].addEventListener('click', closeDrawer);
        }

        /* If the user rotates / resizes past the mobile breakpoint with the
           drawer open, close it so the desktop layout re-pins cleanly. */
        var mq = window.matchMedia('(min-width: 861px)');
        var onChange = function (ev) {
            if (ev.matches && document.body.classList.contains(OPEN_CLASS)) {
                closeDrawer();
            }
        };
        if (mq.addEventListener) mq.addEventListener('change', onChange);
        else if (mq.addListener) mq.addListener(onChange);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
