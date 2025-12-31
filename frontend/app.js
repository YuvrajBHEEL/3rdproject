/**
 * eFile Sathi - Government Document AI System
 * Frontend JavaScript with Enhanced Features
 */

// API Base URL
const API_BASE = '';

// Global state
let currentDocument = {
    docId: null,
    text: '',
    summaries: {},
    category: null
};

// Analytics tracking
let analyticsData = {
    searchCount: 0,
    rtiCount: 0
};

// Upload queue for multi-file
let uploadQueue = [];

// ========================================
// Utility Functions
// ========================================

function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// ========================================
// DOM Ready
// ========================================

document.addEventListener('DOMContentLoaded', () => {
    initializeTheme();
    initializeNavigation();
    initializeUpload();
    initializeResultTabs();
    initializeSummaryOptions();
    initializeSearch();
    initializeRTI();
    loadStats();
    initializeAnalytics();
});

// ========================================
// Theme Toggle (Dark Mode)
// ========================================

function initializeTheme() {
    const themeToggle = document.getElementById('themeToggle');
    const savedTheme = localStorage.getItem('theme') || 'light';

    document.documentElement.setAttribute('data-theme', savedTheme);
    updateThemeIcon(savedTheme);

    if (themeToggle) {
        themeToggle.addEventListener('click', () => {
            const currentTheme = document.documentElement.getAttribute('data-theme');
            const newTheme = currentTheme === 'dark' ? 'light' : 'dark';

            document.documentElement.setAttribute('data-theme', newTheme);
            localStorage.setItem('theme', newTheme);
            updateThemeIcon(newTheme);
        });
    }
}

function updateThemeIcon(theme) {
    const themeIcon = document.querySelector('.theme-icon');
    if (themeIcon) {
        // Sun icon for dark mode, moon icon for light mode
        if (theme === 'dark') {
            themeIcon.innerHTML = '<span class="icon" id="themeIconSvg"><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/></svg></span>';
        } else {
            themeIcon.innerHTML = '<span class="icon" id="themeIconSvg"><svg viewBox="0 0 24 24"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg></span>';
        }
    }
}

// ========================================
// Toast Notifications
// ========================================

function showToast(type, title, message, duration = 4000) {
    const container = document.getElementById('toastContainer');
    if (!container) return;

    const toast = document.createElement('div');
    toast.className = `toast ${type}`;

    const icons = {
        success: '<span class="icon icon-success"><svg viewBox="0 0 24 24"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg></span>',
        error: '<span class="icon icon-error"><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg></span>',
        warning: '<span class="icon icon-warning"><svg viewBox="0 0 24 24"><path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg></span>',
        info: '<span class="icon icon-info"><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg></span>'
    };

    toast.innerHTML = `
        <span class="toast-icon">${icons[type] || icons.info}</span>
        <div class="toast-content">
            <div class="toast-title">${title}</div>
            <div class="toast-message">${message}</div>
        </div>
        <button class="toast-close" onclick="this.parentElement.remove()">×</button>
    `;

    container.appendChild(toast);

    // Auto remove
    setTimeout(() => {
        toast.classList.add('toast-out');
        setTimeout(() => toast.remove(), 300);
    }, duration);
}

// ========================================
// Navigation
// ========================================

function initializeNavigation() {
    const navItems = document.querySelectorAll('.nav-item[data-section]');

    // Map of section IDs
    const sections = {
        'home': document.getElementById('home-section'),
        'upload': document.getElementById('upload-section'),
        'documents': document.getElementById('documents-section'),
        'search': document.getElementById('search-section'),
        'rti': document.getElementById('rti-section'),
        'comparison': document.getElementById('comparison-section'),
        'workflow': document.getElementById('workflow-section'),
        'grievance': document.getElementById('grievance-section'),
        'compliance': document.getElementById('compliance-section'),
        'analytics': document.getElementById('analytics-section')
    };

    navItems.forEach(item => {
        item.addEventListener('click', (e) => {
            e.preventDefault();
            const sectionKey = item.dataset.section;

            // 1. Update nav active state
            navItems.forEach(nav => nav.classList.remove('active'));
            item.classList.add('active');

            // 2. Hide ALL sections first
            Object.values(sections).forEach(section => {
                if (section) section.style.display = 'none';
            });

            // 3. Show the selected section
            const targetSection = sections[sectionKey];
            if (targetSection) {
                targetSection.style.display = 'block';

                // Special handling: Home section also shows upload section
                if (sectionKey === 'home') {
                    const uploadSection = sections['upload'];
                    if (uploadSection) uploadSection.style.display = 'block';
                }

                // Special handling: Scroll to top
                window.scrollTo(0, 0);

                // Load analytics if needed
                if (sectionKey === 'analytics') {
                    loadAnalyticsData();
                }

                // Load documents library if needed
                if (sectionKey === 'documents') {
                    loadDocuments();
                }
            } else {
                console.error(`Section not found: ${sectionKey}`);
            }
        });
    });
}

// ========================================
// Multi-File Upload
// ========================================

function initializeUpload() {
    const uploadZone = document.getElementById('uploadZone');
    const fileInput = document.getElementById('fileInput');

    if (!uploadZone || !fileInput) return;

    // Click to upload
    uploadZone.addEventListener('click', () => fileInput.click());

    // File selected (multiple)
    fileInput.addEventListener('change', (e) => {
        if (e.target.files.length > 0) {
            handleMultipleFiles(Array.from(e.target.files));
        }
    });

    // Drag and drop
    uploadZone.addEventListener('dragover', (e) => {
        e.preventDefault();
        uploadZone.classList.add('dragover');
    });

    uploadZone.addEventListener('dragleave', () => {
        uploadZone.classList.remove('dragover');
    });

    uploadZone.addEventListener('drop', (e) => {
        e.preventDefault();
        uploadZone.classList.remove('dragover');
        if (e.dataTransfer.files.length > 0) {
            handleMultipleFiles(Array.from(e.dataTransfer.files));
        }
    });
}

async function handleMultipleFiles(files) {
    const validTypes = ['.pdf', '.png', '.jpg', '.jpeg', '.tiff'];

    // Filter valid files
    const validFiles = files.filter(file => {
        const ext = '.' + file.name.split('.').pop().toLowerCase();
        return validTypes.includes(ext);
    });

    if (validFiles.length === 0) {
        showToast('error', 'Invalid Files', `Only ${validTypes.join(', ')} are supported`);
        return;
    }

    // Show upload queue
    const queueContainer = document.getElementById('uploadQueue');
    const queueList = document.getElementById('queueList');
    const queueCount = document.getElementById('queueCount');

    if (queueContainer) {
        queueContainer.style.display = 'block';
    }

    uploadQueue = validFiles.map((file, index) => ({
        id: index,
        file: file,
        name: file.name,
        size: formatFileSize(file.size),
        status: 'pending'
    }));

    renderUploadQueue();

    // Process files sequentially
    for (let i = 0; i < uploadQueue.length; i++) {
        uploadQueue[i].status = 'processing';
        renderUploadQueue();

        try {
            await handleFileUpload(uploadQueue[i].file);
            uploadQueue[i].status = 'done';
        } catch (error) {
            uploadQueue[i].status = 'error';
            console.error('Upload error:', error);
        }

        renderUploadQueue();
    }

    showToast('success', 'Upload Complete', `Processed ${validFiles.length} file(s)`);
}

function renderUploadQueue() {
    const queueList = document.getElementById('queueList');
    const queueCount = document.getElementById('queueCount');

    if (!queueList) return;

    if (queueCount) {
        queueCount.textContent = uploadQueue.length;
    }

    const getIcon = (name) => {
        const ext = name.split('.').pop().toLowerCase();
        if (ext === 'pdf') return '<span class="icon"><svg viewBox="0 0 24 24"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/></svg></span>';
        if (['png', 'jpg', 'jpeg'].includes(ext)) return '<span class="icon"><svg viewBox="0 0 24 24"><rect x="3" y="3" width="18" height="18" rx="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21 15 16 10 5 21"/></svg></span>';
        return '<span class="icon"><svg viewBox="0 0 24 24"><path d="M22 19a2 2 0 01-2 2H4a2 2 0 01-2-2V5a2 2 0 012-2h5l2 3h9a2 2 0 012 2z"/></svg></span>';
    };

    queueList.innerHTML = uploadQueue.map(item => `
        <div class="upload-queue-item">
            <span class="file-icon">${getIcon(item.name)}</span>
            <div class="file-info">
                <div class="file-name">${item.name}</div>
                <div class="file-size">${item.size}</div>
                ${item.status === 'processing' ? '<div class="upload-progress"><div class="upload-progress-bar" style="width: 60%"></div></div>' : ''}
            </div>
            <span class="file-status ${item.status}">${item.status}</span>
        </div>
    `).join('');
}

function formatFileSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

async function handleFileUpload(file) {
    const uploadStatus = document.getElementById('uploadStatus');
    const resultsPanel = document.getElementById('resultsPanel');

    // Show loading
    showStatus('loading', `<span class="spinner"></span> Processing ${file.name}...`);

    const formData = new FormData();
    formData.append('file', file);

    const response = await fetch(`${API_BASE}/upload-ocr`, {
        method: 'POST',
        body: formData
    });

    if (!response.ok) {
        throw new Error('Upload failed');
    }

    const data = await response.json();

    // Store document data
    currentDocument.docId = data.doc_id;
    currentDocument.text = data.text;

    // Show success
    showStatus('success', `✓ Processed successfully: ${data.metadata.word_count} words, ${data.metadata.page_count} pages`);

    // Check for similar/duplicate documents
    if (data.similar_documents && data.similar_documents.length > 0) {
        const dupList = data.similar_documents.map(d =>
            `• ${d.filename} (${d.similarity}% similar)`
        ).join('\n');

        showToast('warning', '⚠️ Similar Documents Found',
            `This document appears similar to existing documents:\n${dupList}`, 8000);
    }

    // Display results
    displayOCRResults(data);

    // Show results panel and download buttons
    if (resultsPanel) {
        resultsPanel.style.display = 'block';
    }

    const downloadButtons = document.getElementById('downloadButtons');
    if (downloadButtons) {
        downloadButtons.style.display = 'flex';
    }

    // Generate summaries and actions
    await generateSummaries();
    await extractActions();

    // Update stats
    loadStats();

    return data;
}

function showStatus(type, message) {
    const uploadStatus = document.getElementById('uploadStatus');
    if (uploadStatus) {
        uploadStatus.className = `upload-status ${type}`;
        uploadStatus.innerHTML = message;
    }
}

// ========================================
// Download Functions
// ========================================

function downloadText() {
    if (!currentDocument.text) {
        showToast('warning', 'No Content', 'Please upload a document first');
        return;
    }

    const blob = new Blob([currentDocument.text], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `extracted_text_${currentDocument.docId || 'document'}.txt`;
    a.click();
    URL.revokeObjectURL(url);

    showToast('success', 'Downloaded', 'Text file saved successfully');
}

async function downloadPDF() {
    if (!currentDocument.text) {
        showToast('warning', 'No Content', 'Please upload a document first');
        return;
    }

    try {
        const response = await fetch(`${API_BASE}/export/pdf/${currentDocument.docId}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                text: currentDocument.text,
                doc_id: currentDocument.docId
            })
        });

        if (response.ok) {
            const blob = await response.blob();
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `document_${currentDocument.docId}.pdf`;
            a.click();
            URL.revokeObjectURL(url);
            showToast('success', 'Downloaded', 'PDF saved successfully');
        } else {
            // Fallback: create simple text download
            downloadText();
            showToast('info', 'Fallback', 'Downloaded as text (PDF not available)');
        }
    } catch (error) {
        downloadText();
        showToast('info', 'Fallback', 'Downloaded as text file');
    }
}

function copyText() {
    if (!currentDocument.text) {
        showToast('warning', 'No Content', 'Please upload a document first');
        return;
    }

    navigator.clipboard.writeText(currentDocument.text).then(() => {
        showToast('success', 'Copied', 'Text copied to clipboard');
    }).catch(() => {
        showToast('error', 'Failed', 'Could not copy to clipboard');
    });
}

// ========================================
// OCR Display
// ========================================

function displayOCRResults(data) {
    const textStats = document.getElementById('textStats');
    const extractedText = document.getElementById('extractedText');

    if (textStats) {
        let categoryHtml = '';
        if (data.category) {
            currentDocument.category = data.category;
            categoryHtml = `<span class="doc-category ${data.category.toLowerCase()}">${data.category}</span>`;
        }

        textStats.innerHTML = `
            <span><strong>Pages:</strong> ${data.metadata.page_count}</span>
            <span><strong>Words:</strong> ${data.metadata.word_count}</span>
            <span><strong>Confidence:</strong> ${Math.round(data.confidence.overall_confidence)}%</span>
            <span><strong>Language:</strong> ${data.metadata.language.toUpperCase()}</span>
            ${data.metadata.has_handwriting ? '<span><strong><span class="icon icon-warning"><svg viewBox="0 0 24 24"><path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg></span> Handwriting detected</strong></span>' : ''}
            ${categoryHtml}
        `;
    }

    if (extractedText) {
        extractedText.textContent = data.text;
    }
}

// ========================================
// Result Tabs
// ========================================

function initializeResultTabs() {
    const tabs = document.querySelectorAll('.result-tab');
    const contents = document.querySelectorAll('.result-content');

    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            const targetId = tab.dataset.tab + '-content';

            // Update tabs
            tabs.forEach(t => t.classList.remove('active'));
            tab.classList.add('active');

            // Update content
            contents.forEach(content => {
                content.classList.remove('active');
                if (content.id === targetId) {
                    content.classList.add('active');
                }
            });
        });
    });
}

// ========================================
// Summarization
// ========================================

function initializeSummaryOptions() {
    const radios = document.querySelectorAll('input[name="summaryLevel"]');
    radios.forEach(radio => {
        radio.addEventListener('change', () => {
            if (currentDocument.summaries[radio.value]) {
                displaySummary(radio.value);
            }
        });
    });
}

async function generateSummaries() {
    if (!currentDocument.text) return;

    const summaryText = document.getElementById('summaryText');
    if (summaryText) {
        summaryText.innerHTML = '<span class="spinner"></span> Generating summaries...';
    }

    try {
        const response = await fetch(`${API_BASE}/summarize-all`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ text: currentDocument.text })
        });

        const data = await response.json();
        currentDocument.summaries = data.summaries;

        // Display selected summary
        const selected = document.querySelector('input[name="summaryLevel"]:checked');
        displaySummary(selected ? selected.value : 'secretary');

    } catch (error) {
        console.error('Summarization error:', error);
        if (summaryText) {
            summaryText.textContent = 'Error generating summary. Please try again.';
        }
    }
}

function displaySummary(level) {
    const summaryText = document.getElementById('summaryText');
    const summary = currentDocument.summaries[level];

    if (!summaryText) return;

    if (!summary) {
        summaryText.innerHTML = '<p class="placeholder">No summary available.</p>';
        return;
    }

    summaryText.innerHTML = `
        <div style="margin-bottom: 10px;">${summary.content}</div>
        <div style="font-size: 12px; color: #718096; border-top: 1px solid #e0e4e8; padding-top: 10px;">
            Words: ${summary.word_count} | 
            ${summary.action_required ? '<span class="icon icon-warning"><svg viewBox="0 0 24 24"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg></span> Action Required' : 'No immediate action required'}
        </div>
    `;
}

// ========================================
// Action Extraction
// ========================================

async function extractActions() {
    if (!currentDocument.text) return;

    const actionItems = document.getElementById('actionItems');
    if (actionItems) {
        actionItems.innerHTML = '<span class="spinner"></span> Extracting action items...';
    }

    try {
        const response = await fetch(`${API_BASE}/extract`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ text: currentDocument.text })
        });

        const data = await response.json();
        displayActions(data);

    } catch (error) {
        console.error('Extraction error:', error);
        if (actionItems) {
            actionItems.textContent = 'Error extracting actions.';
        }
    }
}

function displayActions(data) {
    const actionItems = document.getElementById('actionItems');

    if (!actionItems) return;

    if (!data.actions || data.actions.length === 0) {
        actionItems.innerHTML = '<p class="placeholder">No action items identified in this document.</p>';
        return;
    }

    actionItems.innerHTML = data.actions.map((action, index) => `
        <div class="action-item ${action.priority}">
            <div class="action-header">
                <span class="action-who">${action.who}</span>
                <span class="action-priority ${action.priority}">${action.priority}</span>
            </div>
            <div class="action-what">${action.what}</div>
            ${action.when ? `<div class="action-when"><span class="icon"><svg viewBox="0 0 24 24"><rect x="3" y="4" width="18" height="18" rx="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></svg></span> ${action.when}</div>` : ''}
            <button class="btn-secondary btn-sm" onclick="showAssignForm(${index})" style="margin-top: 8px;">
                <span class="icon"><svg viewBox="0 0 24 24"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg></span> Assign Task
            </button>
            <div id="assign-form-${index}" class="task-assign-form" style="display: none;">
                <input type="text" placeholder="Officer Name" id="officer-${index}">
                <input type="date" id="deadline-${index}">
                <button class="btn-success btn-sm" onclick="assignTask(${index}, '${action.what.replace(/'/g, "\\'")}')">Assign</button>
            </div>
        </div>
    `).join('');

    // Add financial amounts if any
    if (data.financial_amounts && data.financial_amounts.length > 0) {
        actionItems.innerHTML += `
            <div style="margin-top: 16px; padding: 12px; background: #fff3e0; border-radius: 4px; font-size: 13px;">
                <strong><span class="icon"><svg viewBox="0 0 24 24"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 000 7h5a3.5 3.5 0 010 7H6"/></svg></span> Financial Amounts:</strong>
                <ul style="margin: 8px 0 0 20px;">
                    ${data.financial_amounts.map(a => `<li>${a.full_text}</li>`).join('')}
                </ul>
            </div>
        `;
    }
}

function showAssignForm(index) {
    const form = document.getElementById(`assign-form-${index}`);
    if (form) {
        form.style.display = form.style.display === 'none' ? 'flex' : 'none';
    }
}

function assignTask(index, taskDescription) {
    const officer = document.getElementById(`officer-${index}`);
    const deadline = document.getElementById(`deadline-${index}`);

    if (!officer || !officer.value.trim()) {
        showToast('warning', 'Missing Info', 'Please enter officer name');
        return;
    }

    const form = document.getElementById(`assign-form-${index}`);
    if (form) {
        form.innerHTML = `
            <div class="assigned-badge">
                <span class="icon icon-success"><svg viewBox="0 0 24 24"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg></span> Assigned to ${officer.value} ${deadline.value ? `| Due: ${deadline.value}` : ''}
            </div>
        `;
        form.style.display = 'block';
    }

    showToast('success', 'Task Assigned', `Assigned to ${officer.value}`);
}

// ========================================
// Search
// ========================================

function initializeSearch() {
    const searchBtn = document.getElementById('searchBtn');
    const searchInput = document.getElementById('searchInput');

    if (searchBtn) {
        searchBtn.addEventListener('click', performSearch);
    }

    if (searchInput) {
        searchInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') performSearch();
        });
    }
}

async function performSearch() {
    const searchInput = document.getElementById('searchInput');
    const searchResults = document.getElementById('searchResults');
    const query = searchInput ? searchInput.value.trim() : '';

    if (!query) return;

    if (searchResults) {
        searchResults.innerHTML = '<span class="spinner"></span> Searching...';
    }

    try {
        const response = await fetch(`${API_BASE}/search`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ query, top_k: 10 })
        });

        const data = await response.json();
        displaySearchResults(data);

        // Track analytics
        analyticsData.searchCount++;

    } catch (error) {
        console.error('Search error:', error);
        if (searchResults) {
            searchResults.innerHTML = '<p class="placeholder">Error performing search.</p>';
        }
    }
}

function displaySearchResults(data) {
    const searchResults = document.getElementById('searchResults');

    if (!searchResults) return;

    if (!data.results || data.results.length === 0) {
        searchResults.innerHTML = `
            <p class="placeholder">No documents found for "${data.query}"</p>
            <p style="font-size: 12px; color: #718096;">Try different keywords or upload more documents.</p>
        `;
        return;
    }

    searchResults.innerHTML = data.results.map(result => `
        <div class="search-result">
            <div class="search-result-header">
                <span class="search-result-title">${result.title}</span>
                <span class="search-result-score">${Math.round(result.score * 100)}%</span>
            </div>
            <div class="search-result-text">${highlightText(result.matched_section, data.query)}</div>
        </div>
    `).join('');
}

function highlightText(text, query) {
    const words = query.toLowerCase().split(' ');
    let highlighted = text;

    words.forEach(word => {
        if (word.length > 2) {
            const regex = new RegExp(`(${word})`, 'gi');
            highlighted = highlighted.replace(regex, '<span class="search-highlight">$1</span>');
        }
    });

    return highlighted;
}

// ========================================
// RTI Generation
// ========================================

function initializeRTI() {
    const generateBtn = document.getElementById('generateRti');
    if (generateBtn) {
        generateBtn.addEventListener('click', generateRTIResponse);
    }
}

async function generateRTIResponse() {
    const rtiQuery = document.getElementById('rtiQuery');
    const rtiApplicant = document.getElementById('rtiApplicant');
    const rtiType = document.getElementById('rtiType');
    const rtiOutput = document.getElementById('rtiOutput');

    const query = rtiQuery ? rtiQuery.value.trim() : '';
    const applicant = rtiApplicant ? rtiApplicant.value.trim() : 'Applicant';
    const responseType = rtiType ? rtiType.value : 'standard';

    if (!query) {
        showToast('warning', 'Missing Query', 'Please enter an RTI query');
        return;
    }

    if (rtiOutput) {
        rtiOutput.innerHTML = '<span class="spinner"></span> Generating RTI response...';
    }

    try {
        const response = await fetch(`${API_BASE}/rti/generate`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                query,
                applicant_name: applicant,
                response_type: responseType
            })
        });

        const data = await response.json();
        displayRTIResponse(data);

        // Track analytics
        analyticsData.rtiCount++;

    } catch (error) {
        console.error('RTI error:', error);
        showToast('error', 'Error', 'Failed to generate RTI response');
        if (rtiOutput) {
            rtiOutput.innerHTML = '<p style="color: #dc3545;">Error generating RTI response.</p>';
        }
    }
}

function displayRTIResponse(data) {
    const rtiOutput = document.getElementById('rtiOutput');

    if (!rtiOutput) return;

    rtiOutput.innerHTML = `<pre style="white-space: pre-wrap; font-size: 12px; margin: 0;">${data.letter}</pre>`;

    if (data.redacted_items && data.redacted_items.length > 0) {
        rtiOutput.innerHTML += `
            <div style="margin-top: 16px; padding: 8px; background: #ffebee; border-radius: 4px; font-size: 12px;">
                <strong><span class="icon"><svg viewBox="0 0 24 24"><rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0110 0v4"/></svg></span> Redacted:</strong> ${data.redacted_items.join(', ')}
            </div>
        `;
    }
}

function printRTI() {
    window.print();
}

function downloadRTIPDF() {
    const rtiOutput = document.getElementById('rtiOutput');
    if (!rtiOutput || rtiOutput.textContent.includes('placeholder')) {
        showToast('warning', 'No Content', 'Generate an RTI response first');
        return;
    }

    // Create downloadable text file as fallback
    const text = rtiOutput.textContent;
    const blob = new Blob([text], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'RTI_Response.txt';
    a.click();
    URL.revokeObjectURL(url);

    showToast('success', 'Downloaded', 'RTI response saved');
}

// ========================================
// Analytics Dashboard
// ========================================

let categoryChart = null;
let timelineChart = null;

function initializeAnalytics() {
    // Charts will be initialized when the analytics section is shown
}

// ========================================
// Document Library Functions
// ========================================

async function loadDocuments() {
    const grid = document.getElementById('documentsGrid');
    const countBadge = document.getElementById('libraryCount');

    if (!grid) return;

    // Show loading
    grid.innerHTML = `
        <div class="loading-placeholder">
            <span class="loading-spinner">⏳</span>
            <p>Loading documents...</p>
        </div>
    `;

    try {
        const response = await fetch(`${API_BASE}/documents/list`);
        const data = await response.json();

        if (data.success && data.documents.length > 0) {
            countBadge.textContent = `${data.count} documents`;

            grid.innerHTML = data.documents.map(doc => `
                <div class="document-card" onclick="viewDocument('${doc.doc_id}')">
                    <div class="document-card-header">
                        <span class="document-icon">📄</span>
                        <div>
                            <div class="document-title">${escapeHtml(doc.title)}</div>
                            <div class="document-id">ID: ${doc.doc_id}</div>
                        </div>
                    </div>
                    <div class="document-preview">${escapeHtml(doc.text_preview || 'No preview available')}</div>
                    <div class="document-meta">
                        <span class="document-meta-item">📝 ${doc.word_count} words</span>
                        ${doc.file_size ? `<span class="document-meta-item">📦 ${formatFileSize(doc.file_size)}</span>` : ''}
                    </div>
                </div>
            `).join('');
        } else {
            grid.innerHTML = `
                <div class="empty-state">
                    <span class="empty-state-icon">📂</span>
                    <p>No documents uploaded yet</p>
                    <p>Upload a document to get started!</p>
                </div>
            `;
            countBadge.textContent = '0 documents';
        }
    } catch (error) {
        console.error('Failed to load documents:', error);
        grid.innerHTML = `
            <div class="empty-state">
                <span class="empty-state-icon">⚠️</span>
                <p>Failed to load documents</p>
                <p>Please try again later</p>
            </div>
        `;
    }
}

// Store current document data for view switching
let currentModalDoc = null;
let currentModalSummary = null;

function viewDocument(docId) {
    // Create and show a document viewer modal
    fetch(`${API_BASE}/documents/${docId}`)
        .then(res => res.json())
        .then(data => {
            if (data.success && data.document) {
                const doc = data.document;
                currentModalDoc = doc;
                currentModalSummary = null; // Reset summary

                // Create modal overlay
                let modal = document.getElementById('documentViewerModal');
                if (!modal) {
                    modal = document.createElement('div');
                    modal.id = 'documentViewerModal';
                    modal.className = 'document-modal-overlay';
                    modal.innerHTML = `
                        <div class="document-modal-content">
                            <div class="document-modal-header">
                                <h3 id="modalDocTitle"></h3>
                                <button class="modal-close-btn" onclick="closeDocumentModal()">✕</button>
                            </div>
                            <div class="document-modal-tabs">
                                <button class="modal-tab active" id="tabFull" onclick="switchDocView('full')">📄 Full Format</button>
                                <button class="modal-tab" id="tabSummary" onclick="switchDocView('summary')">📝 Summarized</button>
                            </div>
                            <div class="document-modal-meta" id="modalDocMeta"></div>
                            <div class="document-modal-body" id="modalDocBody"></div>
                        </div>
                    `;
                    document.body.appendChild(modal);

                    // Add modal styles if not present
                    if (!document.getElementById('modalStyles')) {
                        const styles = document.createElement('style');
                        styles.id = 'modalStyles';
                        styles.textContent = `
                            .document-modal-overlay {
                                position: fixed;
                                top: 0; left: 0; right: 0; bottom: 0;
                                background: rgba(0,0,0,0.7);
                                display: flex;
                                align-items: center;
                                justify-content: center;
                                z-index: 10000;
                                padding: 20px;
                            }
                            .document-modal-content {
                                background: white;
                                border-radius: 8px;
                                max-width: 900px;
                                width: 100%;
                                max-height: 90vh;
                                display: flex;
                                flex-direction: column;
                                box-shadow: 0 20px 60px rgba(0,0,0,0.3);
                            }
                            .document-modal-header {
                                display: flex;
                                justify-content: space-between;
                                align-items: center;
                                padding: 16px 24px;
                                border-bottom: 1px solid #eee;
                                background: #f8f9fa;
                                border-radius: 8px 8px 0 0;
                            }
                            .document-modal-header h3 {
                                margin: 0;
                                color: #003366;
                            }
                            .modal-close-btn {
                                background: none;
                                border: none;
                                font-size: 24px;
                                cursor: pointer;
                                color: #666;
                            }
                            .modal-close-btn:hover { color: #dc3545; }
                            .document-modal-tabs {
                                display: flex;
                                gap: 0;
                                padding: 0 24px;
                                background: #f8f9fa;
                                border-bottom: 2px solid #e0e4e8;
                            }
                            .modal-tab {
                                padding: 12px 24px;
                                background: none;
                                border: none;
                                cursor: pointer;
                                font-size: 14px;
                                font-weight: 500;
                                color: #666;
                                border-bottom: 3px solid transparent;
                                margin-bottom: -2px;
                                transition: all 0.2s;
                            }
                            .modal-tab:hover {
                                color: #003366;
                                background: rgba(0,51,102,0.05);
                            }
                            .modal-tab.active {
                                color: #003366;
                                border-bottom-color: #003366;
                                background: white;
                            }
                            .document-modal-meta {
                                padding: 12px 24px;
                                background: #f0f4f8;
                                font-size: 14px;
                                color: #666;
                            }
                            .document-modal-body {
                                padding: 16px;
                                overflow-y: auto;
                                flex: 1;
                                line-height: 1.6;
                                font-size: 14px;
                            }
                            .summary-loading {
                                display: flex;
                                align-items: center;
                                gap: 10px;
                                color: #666;
                                font-style: italic;
                            }
                            .summary-section { margin-bottom: 16px; }
                            .summary-section h4 { 
                                color: #003366; 
                                margin: 0 0 8px 0;
                                font-size: 14px;
                                text-transform: uppercase;
                                letter-spacing: 0.5px;
                            }
                            .summary-section p { margin: 0; line-height: 1.6; }
                            [data-theme="dark"] .document-modal-content { background: #1a1a2e; }
                            [data-theme="dark"] .document-modal-header { background: #252540; border-color: #333; }
                            [data-theme="dark"] .document-modal-header h3 { color: #fff; }
                            [data-theme="dark"] .document-modal-tabs { background: #252540; border-color: #333; }
                            [data-theme="dark"] .modal-tab { color: #aaa; }
                            [data-theme="dark"] .modal-tab.active { color: #fff; background: #1a1a2e; border-bottom-color: #4da6ff; }
                            [data-theme="dark"] .document-modal-meta { background: #252540; color: #aaa; }
                            [data-theme="dark"] .document-modal-body { color: #ddd; }
                        `;
                        document.head.appendChild(styles);
                    }
                }

                // Reset tabs
                document.getElementById('tabFull').classList.add('active');
                document.getElementById('tabSummary').classList.remove('active');

                // Populate modal content
                document.getElementById('modalDocTitle').textContent = doc.title || doc.filename;
                document.getElementById('modalDocMeta').innerHTML = `
                    <span>📄 ID: ${docId}</span> • 
                    <span>📝 ${doc.word_count} words</span> • 
                    <span>📦 ${formatFileSize(doc.file_size || 0)}</span>
                `;
                document.getElementById('modalDocBody').textContent = doc.full_text || doc.text_preview || 'No content available';

                // Show modal
                modal.style.display = 'flex';

                // Close on backdrop click
                modal.onclick = function (e) {
                    if (e.target === modal) closeDocumentModal();
                };
            }
        })
        .catch(err => {
            console.error('Failed to load document:', err);
            alert('Failed to load document details');
        });
}

function switchDocView(view) {
    const tabFull = document.getElementById('tabFull');
    const tabSummary = document.getElementById('tabSummary');
    const body = document.getElementById('modalDocBody');

    if (view === 'full') {
        tabFull.classList.add('active');
        tabSummary.classList.remove('active');
        body.textContent = currentModalDoc.full_text || currentModalDoc.text_preview || 'No content available';
    } else if (view === 'summary') {
        tabFull.classList.remove('active');
        tabSummary.classList.add('active');

        // If summary already fetched, show it
        if (currentModalSummary) {
            renderSummary(currentModalSummary);
        } else {
            // Fetch summary
            body.innerHTML = '<div class="summary-loading">⏳ Generating summary...</div>';

            fetch(`${API_BASE}/summarize-all`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ text: currentModalDoc.full_text || currentModalDoc.text_preview })
            })
                .then(res => res.json())
                .then(data => {
                    if (data.success && data.summaries) {
                        currentModalSummary = data.summaries;
                        renderSummary(data.summaries);
                    } else {
                        body.innerHTML = '<p>⚠️ Could not generate summary. Please try again.</p>';
                    }
                })
                .catch(err => {
                    console.error('Summary error:', err);
                    body.innerHTML = '<p>⚠️ Failed to generate summary.</p>';
                });
        }
    }
}

function renderSummary(summaries) {
    const body = document.getElementById('modalDocBody');

    // Get the director level summary as the general summary (balanced detail)
    let generalSummary = summaries.director?.content ||
        summaries.officer?.content ||
        summaries.secretary?.content ||
        'Summary not available';

    // Trim extra whitespace
    generalSummary = generalSummary.trim().replace(/\n{3,}/g, '\n\n');

    body.innerHTML = `
        <div class="summary-container">
            <div class="summary-header">
                <h4>📝 Document Summary</h4>
            </div>
            <div class="summary-content">
                ${escapeHtml(generalSummary)}
            </div>
        </div>
        <style>
            .summary-container {
                background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
                border-radius: 6px;
                padding: 16px;
                border-left: 3px solid #003366;
            }
            .summary-header {
                margin-bottom: 8px;
            }
            .summary-header h4 {
                margin: 0;
                color: #003366;
                font-size: 16px;
            }
            .summary-content {
                line-height: 1.6;
                color: #333;
                font-size: 14px;
            }
            [data-theme="dark"] .summary-container {
                background: linear-gradient(135deg, #252540 0%, #1a1a2e 100%);
            }
            [data-theme="dark"] .summary-header h4 { color: #4da6ff; }
            [data-theme="dark"] .summary-content { color: #ddd; }
        </style>
    `;
}

function closeDocumentModal() {
    const modal = document.getElementById('documentViewerModal');
    if (modal) modal.style.display = 'none';
    currentModalDoc = null;
    currentModalSummary = null;
}

function formatFileSize(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

// Update navigation to load documents when section is shown
const originalShowSection = typeof showSection === 'function' ? showSection : null;

async function loadAnalyticsData() {
    try {
        const response = await fetch(`${API_BASE}/health`);
        const data = await response.json();

        // Update stat cards
        const docCount = document.getElementById('analyticsDocCount');
        const blockCount = document.getElementById('analyticsBlockCount');
        const searchCount = document.getElementById('analyticsSearchCount');
        const rtiCount = document.getElementById('analyticsRTICount');

        if (docCount) {
            const match = data.modules.search.match(/(\d+)/);
            docCount.textContent = match ? match[1] : '0';
        }

        if (blockCount) {
            blockCount.textContent = data.modules.blockchain.total_blocks || '0';
        }

        if (searchCount) {
            searchCount.textContent = analyticsData.searchCount;
        }

        if (rtiCount) {
            rtiCount.textContent = analyticsData.rtiCount;
        }

        // Initialize charts
        initializeCharts();

    } catch (error) {
        console.error('Analytics error:', error);
    }
}

function initializeCharts() {
    // Category Chart
    const categoryCtx = document.getElementById('categoryChart');
    if (categoryCtx && typeof Chart !== 'undefined') {
        if (categoryChart) categoryChart.destroy();

        categoryChart = new Chart(categoryCtx, {
            type: 'doughnut',
            data: {
                labels: ['Circular', 'Order', 'Memo', 'Budget', 'Notification', 'Other'],
                datasets: [{
                    data: [12, 8, 15, 6, 10, 5],
                    backgroundColor: [
                        '#1565c0',
                        '#c62828',
                        '#7b1fa2',
                        '#2e7d32',
                        '#00838f',
                        '#757575'
                    ]
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    legend: {
                        position: 'right'
                    }
                }
            }
        });
    }

    // Timeline Chart
    const timelineCtx = document.getElementById('timelineChart');
    if (timelineCtx && typeof Chart !== 'undefined') {
        if (timelineChart) timelineChart.destroy();

        const labels = [];
        const docData = [];

        for (let i = 6; i >= 0; i--) {
            const date = new Date();
            date.setDate(date.getDate() - i);
            labels.push(date.toLocaleDateString('en-IN', { weekday: 'short' }));
            docData.push(Math.floor(Math.random() * 10) + 1);
        }

        timelineChart = new Chart(timelineCtx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Documents Processed',
                    data: docData,
                    backgroundColor: '#003366',
                    borderRadius: 4
                }]
            },
            options: {
                responsive: true,
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            stepSize: 2
                        }
                    }
                },
                plugins: {
                    legend: {
                        display: false
                    }
                }
            }
        });
    }
}

// ========================================
// Stats
// ========================================

async function loadStats() {
    try {
        const response = await fetch(`${API_BASE}/health`);
        const data = await response.json();

        const docCount = document.getElementById('docCount');
        const blockCount = document.getElementById('blockCount');

        if (docCount) {
            const match = data.modules.search.match(/(\d+)/);
            docCount.textContent = match ? match[1] : '0';
        }

        if (blockCount) {
            blockCount.textContent = data.modules.blockchain.total_blocks || '1';
        }

    } catch (error) {
        console.error('Stats error:', error);
    }
}

// Make functions available globally
window.downloadText = downloadText;
window.downloadPDF = downloadPDF;
window.copyText = copyText;
window.printRTI = printRTI;
window.downloadRTIPDF = downloadRTIPDF;
window.showAssignForm = showAssignForm;
window.assignTask = assignTask;


// ========================================
// NEW: Navigation Update for New Sections
// ========================================

// Override navigation to include all new sections
document.addEventListener('DOMContentLoaded', () => {
    // Extended sections including new features
    const allSections = {
        'home': document.getElementById('home-section'),
        'upload': document.getElementById('upload-section'),
        'search': document.getElementById('search-section'),
        'rti': document.getElementById('rti-section'),
        'comparison': document.getElementById('comparison-section'),
        'workflow': document.getElementById('workflow-section'),
        'grievance': document.getElementById('grievance-section'),
        'compliance': document.getElementById('compliance-section'),
        'analytics': document.getElementById('analytics-section')
    };

    const navItems = document.querySelectorAll('.nav-item[data-section]');

    navItems.forEach(item => {
        item.addEventListener('click', (e) => {
            e.preventDefault();
            const section = item.dataset.section;

            // Update nav active state
            navItems.forEach(nav => nav.classList.remove('active'));
            item.classList.add('active');

            // Hide all sections except home and upload (which are always visible)
            Object.keys(allSections).forEach(key => {
                if (allSections[key] && key !== 'home' && key !== 'upload') {
                    allSections[key].style.display = 'none';
                }
            });

            // Show selected section
            if (allSections[section]) {
                allSections[section].style.display = 'block';
            }

            // Load section-specific data
            if (section === 'analytics') loadAnalyticsData();
            if (section === 'grievance') loadGrievances();
        });
    });

    // Initialize new features
    initializeChatbot();
    initializeVoiceControl();
    initializeComparison();
    initializeCompliance();
    initializeWorkflow();
    initializeGrievance();
});


// ========================================
// NEW: Chatbot Widget
// ========================================

function initializeChatbot() {
    const toggle = document.getElementById('chatbotToggle');
    const panel = document.getElementById('chatbotPanel');
    const close = document.getElementById('chatbotClose');
    const input = document.getElementById('chatInput');
    const sendBtn = document.getElementById('chatSendBtn');

    if (toggle) {
        toggle.addEventListener('click', () => {
            panel.classList.toggle('active');
            toggle.innerHTML = panel.classList.contains('active') ? '✕' : '<span class="icon"><svg viewBox="0 0 24 24"><path d="M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z"/></svg></span>';
            if (panel.classList.contains('active')) {
                input?.focus();
            }
        });
    }

    if (close) {
        close.addEventListener('click', () => {
            panel.classList.remove('active');
            toggle.innerHTML = '<span class="icon"><svg viewBox="0 0 24 24"><path d="M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z"/></svg></span>';
        });
    }

    if (sendBtn) {
        sendBtn.addEventListener('click', sendChatMessage);
    }

    if (input) {
        input.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') sendChatMessage();
        });
    }
}

async function sendChatMessage() {
    const input = document.getElementById('chatInput');
    const messages = document.getElementById('chatMessages');
    const message = input?.value.trim();

    if (!message) return;

    // Add user message
    addChatMessage(message, 'user');
    input.value = '';

    // Show typing indicator
    const typingHtml = '<div class="typing-indicator" id="typingIndicator"><span></span><span></span><span></span></div>';
    messages.insertAdjacentHTML('beforeend', typingHtml);
    messages.scrollTop = messages.scrollHeight;

    try {
        const response = await fetch(`${API_BASE}/chat`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                message: message,
                document_text: currentDocument.text || '',
                doc_id: currentDocument.docId || ''
            })
        });

        const data = await response.json();

        // Remove typing indicator
        document.getElementById('typingIndicator')?.remove();

        // Add bot response
        addChatMessage(data.message || 'Sorry, I could not process your request.', 'bot');

    } catch (error) {
        document.getElementById('typingIndicator')?.remove();
        addChatMessage('Sorry, an error occurred. Please try again.', 'bot');
    }
}

function addChatMessage(text, type) {
    const messages = document.getElementById('chatMessages');
    if (!messages) return;

    const messageDiv = document.createElement('div');
    messageDiv.className = `chat-message ${type}`;
    messageDiv.textContent = text;
    messages.appendChild(messageDiv);
    messages.scrollTop = messages.scrollHeight;
}


// ========================================
// NEW: Voice Control
// ========================================

let isListening = false;
let recognition = null;

function initializeVoiceControl() {
    const voiceBtn = document.getElementById('voiceBtn');

    if (!voiceBtn) return;

    // Check for browser support
    if ('webkitSpeechRecognition' in window || 'SpeechRecognition' in window) {
        const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
        recognition = new SpeechRecognition();
        recognition.continuous = false;
        recognition.interimResults = false;
        recognition.lang = 'hi-IN'; // Hindi + English

        recognition.onresult = (event) => {
            const transcript = event.results[0][0].transcript;
            showToast('info', 'Voice Input', transcript);

            // Send to chatbot
            const input = document.getElementById('chatInput');
            if (input) {
                input.value = transcript;
                sendChatMessage();
            }
        };

        recognition.onend = () => {
            isListening = false;
            voiceBtn.classList.remove('listening');
            voiceBtn.innerHTML = '<span class="icon"><svg viewBox="0 0 24 24"><path d="M12 1a3 3 0 00-3 3v8a3 3 0 006 0V4a3 3 0 00-3-3z"/><path d="M19 10v2a7 7 0 01-14 0v-2"/><line x1="12" y1="19" x2="12" y2="23"/><line x1="8" y1="23" x2="16" y2="23"/></svg></span>';
        };

        recognition.onerror = (event) => {
            isListening = false;
            voiceBtn.classList.remove('listening');
            voiceBtn.innerHTML = '<span class="icon"><svg viewBox="0 0 24 24"><path d="M12 1a3 3 0 00-3 3v8a3 3 0 006 0V4a3 3 0 00-3-3z"/><path d="M19 10v2a7 7 0 01-14 0v-2"/><line x1="12" y1="19" x2="12" y2="23"/><line x1="8" y1="23" x2="16" y2="23"/></svg></span>';
            showToast('error', 'Voice Error', 'Could not recognize speech');
        };
    }

    voiceBtn.addEventListener('click', () => {
        if (!recognition) {
            showToast('warning', 'Not Supported', 'Voice input not supported in this browser');
            return;
        }

        if (isListening) {
            recognition.stop();
        } else {
            recognition.start();
            isListening = true;
            voiceBtn.classList.add('listening');
            voiceBtn.textContent = '🔴';
            showToast('info', 'Listening...', 'Speak now in Hindi or English');
        }
    });
}


// ========================================
// NEW: Document Comparison
// ========================================

let compareDoc1Text = '';
let compareDoc2Text = '';

function initializeComparison() {
    const zone1 = document.getElementById('compareDoc1Zone');
    const zone2 = document.getElementById('compareDoc2Zone');
    const file1 = document.getElementById('compareDoc1');
    const file2 = document.getElementById('compareDoc2');
    const compareBtn = document.getElementById('compareDocsBtn');

    if (zone1) zone1.addEventListener('click', () => file1?.click());
    if (zone2) zone2.addEventListener('click', () => file2?.click());

    if (file1) {
        file1.addEventListener('change', async (e) => {
            if (e.target.files[0]) {
                const text = await uploadAndExtractText(e.target.files[0]);
                compareDoc1Text = text;
                zone1.innerHTML = `<div class="upload-icon"><span class="icon icon-success"><svg viewBox="0 0 24 24"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg></span></div><p class="upload-text">Document 1 loaded</p>`;
                showToast('success', 'Loaded', 'First document ready');
            }
        });
    }

    if (file2) {
        file2.addEventListener('change', async (e) => {
            if (e.target.files[0]) {
                const text = await uploadAndExtractText(e.target.files[0]);
                compareDoc2Text = text;
                zone2.innerHTML = `<div class="upload-icon"><span class="icon icon-success"><svg viewBox="0 0 24 24"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg></span></div><p class="upload-text">Document 2 loaded</p>`;
                showToast('success', 'Loaded', 'Second document ready');
            }
        });
    }

    if (compareBtn) {
        compareBtn.addEventListener('click', compareDocuments);
    }
}

async function uploadAndExtractText(file) {
    const formData = new FormData();
    formData.append('file', file);

    try {
        const response = await fetch(`${API_BASE}/upload-ocr`, {
            method: 'POST',
            body: formData
        });
        const data = await response.json();
        return data.text || '';
    } catch (error) {
        showToast('error', 'Error', 'Failed to extract text');
        return '';
    }
}

async function compareDocuments() {
    if (!compareDoc1Text || !compareDoc2Text) {
        showToast('warning', 'Missing Documents', 'Please upload both documents first');
        return;
    }

    showToast('info', 'Comparing...', 'Analyzing document differences');

    try {
        const response = await fetch(`${API_BASE}/compare`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                doc1_text: compareDoc1Text,
                doc2_text: compareDoc2Text
            })
        });

        const data = await response.json();
        displayComparisonResults(data);

    } catch (error) {
        showToast('error', 'Error', 'Comparison failed');
    }
}

function displayComparisonResults(data) {
    const resultsPanel = document.getElementById('comparisonResults');
    const doc1Content = document.getElementById('comparisonDoc1Content');
    const doc2Content = document.getElementById('comparisonDoc2Content');

    if (!resultsPanel) return;

    resultsPanel.style.display = 'grid';

    // Display diff for doc1
    if (doc1Content) {
        doc1Content.innerHTML = data.doc1_diff.map(line =>
            `<div class="diff-line ${line.type}">${line.text}</div>`
        ).join('');
    }

    // Display diff for doc2
    if (doc2Content) {
        doc2Content.innerHTML = data.doc2_diff.map(line =>
            `<div class="diff-line ${line.type}">${line.text}</div>`
        ).join('');
    }

    showToast('success', 'Comparison Complete',
        `Similarity: ${data.similarity_score}% | ${data.additions} additions, ${data.deletions} deletions`);
}


// ========================================
// NEW: Compliance Checker
// ========================================

function initializeCompliance() {
    const zone = document.getElementById('complianceUploadZone');
    const fileInput = document.getElementById('complianceFileInput');

    if (zone) zone.addEventListener('click', () => fileInput?.click());

    if (fileInput) {
        fileInput.addEventListener('change', async (e) => {
            if (e.target.files[0]) {
                await checkDocumentCompliance(e.target.files[0]);
            }
        });
    }
}

async function checkDocumentCompliance(file) {
    showToast('info', 'Checking...', 'Analyzing document compliance');

    const text = await uploadAndExtractText(file);
    if (!text) return;

    try {
        const response = await fetch(`${API_BASE}/compliance/check`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ text })
        });

        const data = await response.json();
        displayComplianceResults(data);

    } catch (error) {
        showToast('error', 'Error', 'Compliance check failed');
    }
}

function displayComplianceResults(data) {
    const resultsDiv = document.getElementById('complianceResults');
    const progressCircle = document.getElementById('complianceProgress');
    const valueDiv = document.getElementById('complianceValue');
    const checklist = document.getElementById('complianceChecklist');
    const esignStatus = document.getElementById('esignStatus');
    const meter = document.getElementById('complianceMeter');

    if (!resultsDiv) return;

    resultsDiv.style.display = 'block';

    // Update score circle
    const circumference = 2 * Math.PI * 34; // r=34
    const offset = circumference - (data.score / 100 * circumference);

    if (progressCircle) {
        progressCircle.style.strokeDasharray = `${circumference}`;
        progressCircle.style.strokeDashoffset = offset;
    }

    if (valueDiv) {
        valueDiv.textContent = `${Math.round(data.score)}%`;
    }

    // Update meter class based on score
    if (meter) {
        meter.classList.remove('low', 'medium');
        if (data.score < 50) meter.classList.add('low');
        else if (data.score < 75) meter.classList.add('medium');
    }

    // Update checklist
    if (checklist) {
        checklist.innerHTML = data.checks.map(check => `
            <div class="compliance-item">
                <span class="${check.passed ? 'check' : 'cross'}">${check.passed ? '✓' : '✗'}</span>
                <span>${check.message}</span>
            </div>
        `).join('');
    }

    // Update e-sign status
    if (esignStatus) {
        if (data.has_digital_signature) {
            esignStatus.className = 'esign-badge verified';
            esignStatus.innerHTML = '<span class="icon"><svg viewBox="0 0 24 24"><rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0110 0v4"/></svg></span> Digital Signature: Verified';
        } else {
            esignStatus.className = 'esign-badge pending';
            esignStatus.innerHTML = '<span class="icon"><svg viewBox="0 0 24 24"><rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0110 0v4"/></svg></span> Digital Signature: Not Found';
        }
    }

    showToast('success', 'Compliance Check Complete', `Score: ${Math.round(data.score)}% (Grade ${data.grade})`);

    // Show confetti if high score
    if (data.score >= 90) {
        showConfetti();
    }
}


// ========================================
// NEW: Workflow Tracker
// ========================================

function initializeWorkflow() {
    const trackBtn = document.getElementById('trackWorkflowBtn');
    const docIdInput = document.getElementById('workflowDocId');

    if (trackBtn) {
        trackBtn.addEventListener('click', trackWorkflow);
    }

    if (docIdInput) {
        docIdInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') trackWorkflow();
        });
    }
}

async function trackWorkflow() {
    const docIdInput = document.getElementById('workflowDocId');
    const docId = docIdInput?.value.trim() || 'DOC-2024-0001';

    try {
        const response = await fetch(`${API_BASE}/workflow/${docId}`);

        if (!response.ok) {
            showToast('warning', 'Not Found', `No workflow found for ${docId}`);
            return;
        }

        const data = await response.json();
        displayWorkflow(data);

    } catch (error) {
        showToast('error', 'Error', 'Failed to fetch workflow');
    }
}

function displayWorkflow(data) {
    const display = document.getElementById('workflowDisplay');
    if (!display) return;

    const steps = ['submitted', 'under_review', 'pending_approval', 'approved', 'archived'];
    const icons = [
        '<span class="icon"><svg viewBox="0 0 24 24"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg></span>',
        '<span class="icon"><svg viewBox="0 0 24 24"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg></span>',
        '<span class="icon"><svg viewBox="0 0 24 24"><path d="M17 3a2.828 2.828 0 114 4L7.5 20.5 2 22l1.5-5.5L17 3z"/></svg></span>',
        '<span class="icon"><svg viewBox="0 0 24 24"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg></span>',
        '<span class="icon"><svg viewBox="0 0 24 24"><path d="M22 19a2 2 0 01-2 2H4a2 2 0 01-2-2V5a2 2 0 012-2h5l2 3h9a2 2 0 012 2z"/></svg></span>'
    ];
    const labels = ['Submitted', 'Under Review', 'Pending Approval', 'Approved', 'Archived'];

    const currentIndex = steps.indexOf(data.current_status);

    display.innerHTML = `
        <h4 style="margin-bottom: 16px; color: var(--primary-blue);"><span class="icon"><svg viewBox="0 0 24 24"><path d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2"/><rect x="9" y="3" width="6" height="4" rx="1"/></svg></span> ${data.title}</h4>
        <p style="margin-bottom: 8px; font-size: 13px;">
            <strong>Progress:</strong> ${data.progress}% | 
            <strong>Days in process:</strong> ${data.days_in_process}
            ${data.days_remaining !== null ? ` | <strong>Days remaining:</strong> ${data.days_remaining}` : ''}
        </p>
        <div class="workflow-timeline">
            ${steps.map((step, i) => `
                <div class="workflow-step ${i < currentIndex ? 'completed' : i === currentIndex ? 'current' : 'pending'}">
                    <div class="workflow-icon">${icons[i]}</div>
                    <div class="workflow-label">${labels[i]}</div>
                </div>
            `).join('')}
        </div>
    `;

    showToast('info', 'Workflow Loaded', `Status: ${data.current_status.replace('_', ' ')}`);
}


// ========================================
// NEW: Grievance Tracking
// ========================================

function initializeGrievance() {
    const submitBtn = document.getElementById('submitGrievanceBtn');
    if (submitBtn) {
        submitBtn.addEventListener('click', submitGrievance);
    }
}

async function submitGrievance() {
    const subject = document.getElementById('grievanceSubject')?.value.trim();
    const details = document.getElementById('grievanceDetails')?.value.trim();
    const priority = document.getElementById('grievancePriority')?.value || 'normal';

    if (!subject || !details) {
        showToast('warning', 'Missing Info', 'Please enter subject and details');
        return;
    }

    try {
        const response = await fetch(`${API_BASE}/grievance/register`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ subject, details, priority })
        });

        const data = await response.json();

        showToast('success', 'Grievance Registered', `ID: ${data.id}`);
        showConfetti();

        // Clear form
        document.getElementById('grievanceSubject').value = '';
        document.getElementById('grievanceDetails').value = '';

        // Reload list
        loadGrievances();

    } catch (error) {
        showToast('error', 'Error', 'Failed to register grievance');
    }
}

async function loadGrievances() {
    try {
        const response = await fetch(`${API_BASE}/grievance/list`);
        const data = await response.json();

        const list = document.getElementById('grievanceList');
        if (!list || !data.grievances) return;

        list.innerHTML = data.grievances.slice(0, 5).map(g => `
            <div class="grievance-card ${g.priority === 'urgent' ? 'urgent' : ''} ${g.status === 'resolved' ? 'resolved' : ''}">
                <div class="grievance-header">
                    <span class="grievance-id">${g.id}</span>
                    <span class="grievance-status ${g.status}">${g.status}</span>
                </div>
                <div class="grievance-subject">${g.subject}</div>
                <div class="grievance-date"><span class="icon"><svg viewBox="0 0 24 24"><rect x="3" y="4" width="18" height="18" rx="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></svg></span> Submitted: ${formatDate(g.submitted_date)} | <span class="icon"><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg></span> Due: ${formatDate(g.due_date)}</div>
            </div>
        `).join('');

    } catch (error) {
        console.error('Failed to load grievances:', error);
    }
}

function formatDate(isoString) {
    const date = new Date(isoString);
    return date.toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: 'numeric' });
}


// ========================================
// NEW: Confetti Animation
// ========================================

function showConfetti() {
    const container = document.createElement('div');
    container.className = 'confetti-container';
    document.body.appendChild(container);

    const colors = ['#ff9933', '#138808', '#003366', '#f26522', '#ffc107'];

    for (let i = 0; i < 50; i++) {
        const confetti = document.createElement('div');
        confetti.className = 'confetti';
        confetti.style.left = Math.random() * 100 + '%';
        confetti.style.backgroundColor = colors[Math.floor(Math.random() * colors.length)];
        confetti.style.animationDelay = Math.random() * 2 + 's';
        confetti.style.transform = `rotate(${Math.random() * 360}deg)`;
        container.appendChild(confetti);
    }

    setTimeout(() => container.remove(), 4000);
}


// ========================================
// Expose new functions globally
// ========================================

window.trackWorkflow = trackWorkflow;
window.loadGrievances = loadGrievances;
window.submitGrievance = submitGrievance;
window.showConfetti = showConfetti;
