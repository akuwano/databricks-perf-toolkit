// Theme toggle
const themeToggle = document.getElementById('themeToggle');
const html = document.documentElement;

// Check saved preference or system preference
const savedTheme = localStorage.getItem('theme');
const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;

if (savedTheme) {
    html.dataset.theme = savedTheme;
} else if (!prefersDark) {
    html.dataset.theme = 'light';
}

updateThemeIcon();

themeToggle.addEventListener('click', () => {
    const newTheme = html.dataset.theme === 'dark' ? 'light' : 'dark';
    html.dataset.theme = newTheme;
    localStorage.setItem('theme', newTheme);
    updateThemeIcon();
});

function updateThemeIcon() {
    const icon = themeToggle.querySelector('i');
    const isDark = html.dataset.theme === 'dark';
    icon.className = isDark ? 'bi bi-moon-fill' : 'bi bi-sun-fill';

    // Switch highlight.js theme
    const hljsLight = document.getElementById('hljs-theme-light');
    const hljsDark = document.getElementById('hljs-theme-dark');
    if (hljsLight) hljsLight.disabled = isDark;
    if (hljsDark) hljsDark.disabled = !isDark;

    // Re-initialize mermaid theme
    if (window.mermaid) {
        mermaid.initialize({
            startOnLoad: false,
            theme: isDark ? 'dark' : 'default',
            flowchart: { useMaxWidth: true, htmlLabels: true },
            securityLevel: 'strict',
        });
    }
}

// Language toggle
document.querySelectorAll('.lang-btn').forEach(btn => {
    btn.addEventListener('click', async () => {
        const lang = btn.dataset.lang;
        try {
            await fetch('/api/v1/lang', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ lang: lang })
            });
            // Reload page to apply language change
            window.location.reload();
        } catch (e) {
            console.error('Failed to set language:', e);
        }
    });
});

// Accordion functionality
document.querySelectorAll('.accordion-button').forEach(btn => {
    btn.addEventListener('click', (e) => {
        e.preventDefault();
        e.stopPropagation();
        const collapse = btn.closest('.accordion-item').querySelector('.accordion-collapse');
        if (collapse) {
            collapse.classList.toggle('show');
            btn.classList.toggle('collapsed');
        }
    });
});
