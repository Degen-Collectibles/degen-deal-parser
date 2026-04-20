/* linear-drawer.js — mobile slide-in drawer for the linear sidebar */
(function () {
    var OPEN_CLASS = 'linear-drawer-open';
    var prevFocus = null;

    function openDrawer() {
        document.body.classList.add(OPEN_CLASS);
        var hamburger = document.getElementById('linear-hamburger');
        if (hamburger) hamburger.setAttribute('aria-expanded', 'true');
        var sidebar = document.getElementById('linear-sidebar');
        if (sidebar) {
            var firstFocusable = sidebar.querySelector('a[href], button:not([disabled])');
            if (firstFocusable) {
                prevFocus = document.activeElement;
                firstFocusable.focus();
            }
        }
    }

    function closeDrawer() {
        document.body.classList.remove(OPEN_CLASS);
        var hamburger = document.getElementById('linear-hamburger');
        if (hamburger) hamburger.setAttribute('aria-expanded', 'false');
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
