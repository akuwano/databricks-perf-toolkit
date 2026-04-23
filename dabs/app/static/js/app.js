/**
 * DBSQL Profiler Analyzer - Client-side JavaScript
 */

// Utility functions
const Utils = {
    formatBytes: (bytes) => {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    },

    formatTime: (ms) => {
        if (ms < 1000) return `${ms} ms`;
        if (ms < 60000) return `${(ms / 1000).toFixed(2)} sec`;
        const minutes = Math.floor(ms / 60000);
        const seconds = ((ms % 60000) / 1000).toFixed(1);
        return `${minutes} min ${seconds} sec`;
    },

    formatPercent: (ratio) => {
        return (ratio * 100).toFixed(1) + '%';
    }
};

// API client
const API = {
    async analyze(formData) {
        const response = await fetch('/api/v1/analyze', {
            method: 'POST',
            body: formData
        });
        return response.json();
    },

    async getAnalysis(id) {
        const response = await fetch(`/api/v1/analyze/${id}`);
        return response.json();
    },

    downloadUrl(id, format) {
        return `/api/v1/analyze/${id}/download?format=${format}`;
    }
};

/**
 * Wrap h2 sections in collapsible <details> elements.
 * Sections default open/closed based on title keywords.
 */
const OPEN_SECTIONS = [
    'Executive Summary', 'エグゼクティブサマリー',
    'Action Plan', 'アクションプラン',
    'Root Cause', '根本原因',
    'Recommendation', '推奨',
    'Hot Operator', 'ホットオペレータ',
];

function wrapSectionsInDetails(container) {
    if (!container) return;
    const h2s = Array.from(container.querySelectorAll('h2'));
    h2s.forEach(h2 => {
        const title = h2.textContent || '';
        const shouldOpen = OPEN_SECTIONS.some(k => title.includes(k));

        // Collect siblings until next h2, h1, or hr
        const content = [];
        let next = h2.nextSibling;
        while (next && !(next.nodeType === 1 && (next.tagName === 'H2' || next.tagName === 'H1' || next.tagName === 'HR'))) {
            content.push(next);
            next = next.nextSibling;
        }

        // Remove preceding <hr>
        const prevHr = h2.previousElementSibling;
        if (prevHr && prevHr.tagName === 'HR') {
            prevHr.remove();
        }

        // Build <details>
        const details = document.createElement('details');
        if (shouldOpen) details.setAttribute('open', '');
        const summary = document.createElement('summary');
        summary.appendChild(h2.cloneNode(true));
        details.appendChild(summary);
        const body = document.createElement('div');
        body.className = 'details-body';
        content.forEach(n => body.appendChild(n));
        details.appendChild(body);
        h2.replaceWith(details);
    });
}

// Export for use in templates
window.Utils = Utils;
window.API = API;
window.wrapSectionsInDetails = wrapSectionsInDetails;
