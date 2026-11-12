(() => {
    'use strict';

    const AGE_LABELS = ['18–24', '25–34', '35–44', '45–54', '55+'];
    const number = new Intl.NumberFormat('en-US');
    const dollars = new Intl.NumberFormat('en-US', {style: 'currency', currency: 'USD', maximumFractionDigits: 2});
    const compact = new Intl.NumberFormat('en-US', {notation: 'compact', maximumFractionDigits: 1});
    const compactDollars = new Intl.NumberFormat('en-US', {style: 'currency', currency: 'USD', notation: 'compact', maximumFractionDigits: 1});
    const font = '-apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif';
    const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    const $ = id => document.getElementById(id);
    const finite = value => Number.isFinite(Number(value)) ? Math.max(0, Number(value)) : 0;
    const escapeHTML = value => String(value).replace(/[&<>"']/g, char => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[char]));
    const setText = (id, value) => { $(id).textContent = value; };
    const tooltipOptions = {
        backgroundColor: '#fff', borderColor: '#e0e9e3', borderWidth: 1,
        padding: [14, 16], extraCssText: 'box-shadow:0 8px 30px rgba(25,60,43,.12);border-radius:10px;',
        textStyle: {fontFamily: font, fontSize: 11, color: '#526d60'}, confine: true,
    };

    let state = {
        schema_version: 2, updated_at: null, replay: null,
        connection: {kafka: 'connecting', message: 'Connecting to the purchase stream…'},
        totals: {purchases: 0, revenue: 0, average_purchase: 0, active_states: 0},
        age_groups: [], states: [], trend: [],
    };
    let mapMetric = 'purchases';
    let trendMinutes = 5;
    let mapReady = false;
    let snapshotReady = false;
    let receivedLiveUpdate = false;
    let transportStatus = 'connecting';
    let fetchError = '';
    const charts = {};

    function formatMoney(value) { return dollars.format(finite(value)); }
    function normalize(payload) {
        if (!payload || payload.schema_version !== 2) throw new Error('Unsupported dashboard data format.');
        const totals = payload.totals || {};
        return {
            schema_version: 2,
            updated_at: payload.updated_at || null,
            replay: payload.replay && typeof payload.replay === 'object' ? {...payload.replay} : null,
            connection: payload.connection || {kafka: 'connecting', message: ''},
            totals: {
                purchases: finite(totals.purchases), revenue: finite(totals.revenue),
                average_purchase: finite(totals.average_purchase), active_states: finite(totals.active_states),
            },
            age_groups: Array.isArray(payload.age_groups) ? payload.age_groups : [],
            states: Array.isArray(payload.states) ? payload.states.map(item => ({
                code: String(item.code || ''), name: String(item.name || ''), count: finite(item.count), revenue: finite(item.revenue),
            })) : [],
            trend: Array.isArray(payload.trend) ? payload.trend.map(item => ({
                timestamp: item.timestamp, purchases: finite(item.purchases), revenue: finite(item.revenue),
            })).filter(item => Number.isFinite(Date.parse(item.timestamp))) : [],
        };
    }

    function replayLabel() {
        const current = state.replay ? Number(state.replay.number) : 0;
        return Number.isSafeInteger(current) && current > 0 ? 'Round ' + number.format(current) : 'Current round';
    }

    function updateConnection() {
        const kafka = state.connection.kafka;
        let label = 'Connecting to data';
        let kind = 'is-connecting';
        if (kafka === 'error') {
            label = 'Stream unavailable'; kind = 'is-error';
        } else if (transportStatus === 'disconnected') {
            label = 'Reconnecting to live updates';
        } else if (snapshotReady && kafka === 'connected') {
            label = transportStatus === 'connected' ? 'Live data connected' : 'Syncing live updates';
            kind = transportStatus === 'connected' ? 'is-connected' : 'is-connecting';
        } else if (snapshotReady && kafka === 'connecting') {
            label = 'Connecting to Kafka';
        }
        $('connection-pill').className = 'connection-pill ' + kind;
        setText('connection-label', label);
        let notice = '';
        if (fetchError && !snapshotReady) notice = fetchError;
        else if (kafka === 'error') notice = state.connection.message || 'The purchase stream is unavailable. Waiting to reconnect.';
        else if (transportStatus === 'disconnected') notice = 'Live updates are temporarily disconnected. Reconnecting automatically…';
        else if (!snapshotReady) notice = 'Connecting to the purchase stream…';
        else if (kafka === 'connecting') notice = state.connection.message || 'Connecting to Kafka. Your dashboard will update automatically.';
        else if (!state.replay) notice = 'Connected and waiting for the first replay round.';
        else if (state.totals.purchases === 0) notice = replayLabel() + ' is ready. Waiting for purchase events.';
        $('stream-notice').hidden = !notice;
        $('stream-notice').classList.toggle('is-error', kafka === 'error' || Boolean(fetchError && !snapshotReady));
        setText('stream-notice-text', notice);
    }

    function updateSummary() {
        setText('total-purchases', number.format(state.totals.purchases));
        setText('total-revenue', formatMoney(state.totals.revenue));
        setText('average-purchase', formatMoney(state.totals.average_purchase));
        setText('active-states', number.format(state.totals.active_states));
        const updated = Date.parse(state.updated_at);
        setText('updated-at', !state.replay
            ? 'Waiting for the first replay round'
            : replayLabel() + ' · ' + (Number.isFinite(updated)
                ? 'Last update ' + new Date(updated).toLocaleTimeString('en-US', {hour: 'numeric', minute: '2-digit', second: '2-digit'})
                : 'Waiting for purchase events'));
    }

    function renderAge() {
        const counts = AGE_LABELS.map(label => {
            const group = state.age_groups.find(item => item.label === label);
            return group ? finite(group.count) : 0;
        });
        const total = counts.reduce((sum, count) => sum + count, 0);
        const colors = ['#a9cabb', '#5d9a83', '#277d63', '#78aa92', '#c1d8ca'];
        charts.age.setOption({
            animation: !reducedMotion, animationDurationUpdate: 350,
            textStyle: {fontFamily: font},
            grid: {left: 68, right: 60, top: 24, bottom: 38},
            tooltip: {...tooltipOptions, trigger: 'item', formatter: item => '<div class="echarts-tooltip"><div class="tooltip-title">Age ' + escapeHTML(item.name) + '</div><div class="tooltip-row"><span>Purchases</span><strong>' + number.format(item.value) + '</strong></div><div class="tooltip-note">' + (total ? (item.value / total * 100).toFixed(1) : '0') + '% of age-group purchases</div></div>'},
            xAxis: {type: 'value', min: 0, minInterval: 1, splitNumber: 3, axisLine: {show: false}, axisTick: {show: false}, axisLabel: {color: '#a0aeaa', fontSize: 9, margin: 13, formatter: value => compact.format(value)}, splitLine: {lineStyle: {color: '#edf2ee', type: 'dashed'}}},
            yAxis: {type: 'category', inverse: true, data: AGE_LABELS, axisLine: {show: false}, axisTick: {show: false}, axisLabel: {color: '#647e71', fontSize: 10, margin: 16}},
            series: [{type: 'bar', name: 'Purchases', barWidth: 17, showBackground: true, backgroundStyle: {color: '#f5f8f5', borderRadius: 4}, itemStyle: {borderRadius: [0, 4, 4, 0]}, label: {show: true, position: 'right', distance: 10, color: '#527362', fontSize: 10, formatter: item => compact.format(item.value)}, data: counts.map((value, index) => ({value, itemStyle: {color: colors[index]}}))}],
        });
        if (total) {
            const peak = counts.indexOf(Math.max(...counts));
            $('age-insight').innerHTML = '<span class="insight-dot" aria-hidden="true"></span><span><strong>' + AGE_LABELS[peak] + '</strong> leads with <strong>' + (counts[peak] / total * 100).toFixed(1) + '%</strong> of age-group purchases.</span>';
        } else {
            $('age-insight').innerHTML = '<span class="insight-dot" aria-hidden="true"></span><span>Age groups will appear as purchases arrive.</span>';
        }
        $('age-chart').setAttribute('aria-label', 'Purchase events by age group: ' + AGE_LABELS.map((label, index) => label + ': ' + number.format(counts[index])).join('; '));
    }

    function renderMap() {
        const revenue = mapMetric === 'revenue';
        const maximum = Math.max(0, ...state.states.map(item => revenue ? item.revenue : item.count));
        setText('map-legend-max', revenue ? compactDollars.format(maximum) : compact.format(maximum));
        $('map-chart').setAttribute('aria-label', 'United States map showing ' + (revenue ? 'revenue' : 'purchase counts') + ' by state. Alaska and Hawaii shown as insets.');
        if (!mapReady) return;
        charts.map.setOption({
            animation: !reducedMotion, animationDurationUpdate: 400,
            textStyle: {fontFamily: font},
            tooltip: {...tooltipOptions, trigger: 'item', formatter: item => {
                const values = item.data || {};
                return '<div class="echarts-tooltip"><div class="tooltip-title">' + escapeHTML(item.name) + '</div><div class="tooltip-row"><span>Purchases</span><strong>' + number.format(finite(values.count)) + '</strong></div><div class="tooltip-row"><span>Revenue</span><strong>' + formatMoney(values.revenue) + '</strong></div></div>';
            }},
            visualMap: {show: false, min: 0, max: Math.max(maximum, 1), inRange: {color: ['#e5f1eb', '#b7d9c8', '#80b89e', '#418d70', '#126b59']}},
            series: [{type: 'map', name: revenue ? 'Revenue' : 'Purchases', map: 'USA', roam: false, selectedMode: false, layoutCenter: ['50%', '49%'], layoutSize: Math.min($('map-chart').clientWidth * 0.96, $('map-chart').clientHeight * 1.9), label: {show: false}, itemStyle: {areaColor: '#e9eeeb', borderColor: '#fff', borderWidth: 1.1}, emphasis: {label: {show: false}, itemStyle: {areaColor: '#e4b08e', borderColor: '#fff', borderWidth: 1.5}}, data: state.states.map(item => ({name: item.name, value: revenue ? item.revenue : item.count, count: item.count, revenue: item.revenue}))}],
        });
    }

    function renderRanking() {
        const revenue = mapMetric === 'revenue';
        const rows = [...state.states].filter(item => item.count > 0 || item.revenue > 0).sort((a, b) => (revenue ? b.revenue - a.revenue : b.count - a.count) || a.name.localeCompare(b.name)).slice(0, 5);
        setText('ranking-description', 'Top states ranked by ' + (revenue ? 'revenue' : 'purchases'));
        $('ranking-empty').hidden = rows.length > 0;
        $('state-ranking').innerHTML = rows.map((item, index) => '<tr><td><div class="state-label"><span class="state-rank">' + String(index + 1).padStart(2, '0') + '</span><span class="state-code">' + escapeHTML(item.code) + '</span><span class="state-name">' + escapeHTML(item.name) + '</span></div></td><td class="' + (revenue ? '' : 'primary-value') + '">' + number.format(item.count) + '</td><td class="' + (revenue ? 'primary-value' : '') + '">' + formatMoney(item.revenue) + '</td></tr>').join('');
    }

    function renderTrend() {
        const now = Date.now();
        const start = now - trendMinutes * 60 * 1000;
        const buckets = state.trend.filter(item => {
            const timestamp = Date.parse(item.timestamp);
            return timestamp >= start && timestamp <= now + 5000;
        }).sort((a, b) => Date.parse(a.timestamp) - Date.parse(b.timestamp));
        const total = buckets.reduce((sum, item) => sum + item.purchases, 0);
        setText('trend-window-total', number.format(total));
        setText('trend-window-label', trendMinutes === 1 ? 'minute' : '5 minutes');
        charts.trend.setOption({
            animation: !reducedMotion, animationDurationUpdate: 300,
            textStyle: {fontFamily: font},
            grid: {left: 49, right: 24, top: 23, bottom: 35},
            tooltip: {...tooltipOptions, trigger: 'axis', axisPointer: {type: 'line', lineStyle: {color: '#9cbaab', type: 'dashed'}}, formatter: items => {
                if (!items.length) return '';
                const item = items[0].data;
                const time = new Date(item.value[0]).toLocaleTimeString('en-US', {hour: 'numeric', minute: '2-digit', second: '2-digit'});
                return '<div class="echarts-tooltip"><div class="tooltip-title">' + time + '</div><div class="tooltip-row"><span>Purchases</span><strong>' + number.format(item.value[1]) + '</strong></div><div class="tooltip-row"><span>Revenue</span><strong>' + formatMoney(item.revenue) + '</strong></div><div class="tooltip-note">5-second interval</div></div>';
            }},
            xAxis: {type: 'time', min: start, max: now, splitNumber: 4, axisLine: {lineStyle: {color: '#e8eeea'}}, axisTick: {show: false}, splitLine: {show: false}, axisLabel: {color: '#98a9a0', fontSize: 9, margin: 12, hideOverlap: true, formatter: timestamp => new Date(timestamp).toLocaleTimeString('en-US', {hour12: false, minute: '2-digit', second: '2-digit'})}},
            yAxis: {type: 'value', min: 0, minInterval: 1, splitNumber: 3, max: values => Math.max(1, Math.ceil(values.max * 1.15)), axisLine: {show: false}, axisTick: {show: false}, axisLabel: {color: '#98a9a0', fontSize: 9, margin: 12, formatter: value => compact.format(value)}, splitLine: {lineStyle: {color: '#edf2ee', type: 'dashed'}}},
            series: [{name: 'Purchases', type: 'line', showSymbol: false, symbol: 'circle', symbolSize: 6, smooth: 0.2, lineStyle: {color: '#287f64', width: 2.1}, itemStyle: {color: '#287f64', borderColor: '#fff', borderWidth: 2}, areaStyle: {color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [{offset: 0, color: 'rgba(69,151,112,.17)'}, {offset: 1, color: 'rgba(69,151,112,.015)'}])}, data: buckets.map(item => ({value: [Date.parse(item.timestamp), item.purchases], revenue: item.revenue}))}],
        });
        $('trend-chart').setAttribute('aria-label', 'Purchase events per five-second interval over the past ' + trendMinutes + (trendMinutes === 1 ? ' minute. ' : ' minutes. ') + number.format(total) + ' purchases in this window.');
    }

    function applySnapshot(payload) {
        try {
            const nextState = normalize(payload);
            const previousRound = state.replay ? state.replay.id : null;
            const nextRound = nextState.replay ? nextState.replay.id : null;
            if (previousRound !== nextRound) {
                // Remove the previous round immediately, including hovered values.
                Object.values(charts).forEach(chart => chart.clear());
            }
            state = nextState;
            snapshotReady = true;
            fetchError = '';
            updateSummary();
            updateConnection();
            renderAge();
            renderMap();
            renderRanking();
            renderTrend();
            return true;
        } catch (error) {
            fetchError = 'The dashboard received an unsupported update. Waiting for valid data.';
            updateConnection();
            console.error(error);
            return false;
        }
    }

    async function fetchJSON(url) {
        const controller = new AbortController();
        const timeout = window.setTimeout(() => controller.abort(), 15000);
        try {
            const response = await fetch(url, {signal: controller.signal, headers: {Accept: 'application/json'}});
            if (!response.ok) throw new Error('Request failed: ' + response.status);
            return await response.json();
        } finally { window.clearTimeout(timeout); }
    }

    async function loadMap() {
        try {
            const geoJSON = await fetchJSON(document.body.dataset.mapUrl || '/static/data/us-states.geojson');
            echarts.registerMap('USA', geoJSON, {Alaska: {left: -131, top: 25, width: 15}, Hawaii: {left: -110, top: 28, width: 5}});
            mapReady = true;
            $('map-chart-message').hidden = true;
            renderMap();
        } catch (error) {
            $('map-chart-message').innerHTML = '<span>The U.S. map could not be loaded.</span><span>State rankings and totals remain available.</span>';
            console.error(error);
        }
    }

    function connectLiveUpdates() {
        if (typeof window.io !== 'function') {
            transportStatus = 'disconnected';
            updateConnection();
            return;
        }
        const socket = io();
        socket.on('connect', () => { transportStatus = 'connected'; updateConnection(); });
        socket.on('disconnect', () => { transportStatus = 'disconnected'; updateConnection(); });
        socket.on('connect_error', () => { transportStatus = 'disconnected'; updateConnection(); });
        socket.on('dashboard:update', payload => {
            if (applySnapshot(payload)) receivedLiveUpdate = true;
        });
    }

    if (typeof window.echarts === 'undefined') {
        setText('stream-notice-text', 'The chart library could not be loaded. Please reload the page.');
        $('stream-notice').classList.add('is-error');
        setText('connection-label', 'Dashboard unavailable');
        $('connection-pill').className = 'connection-pill is-error';
        $('map-chart-message').innerHTML = '<span>Charts are currently unavailable.</span>';
        return;
    }

    charts.map = echarts.init($('map-chart'));
    charts.age = echarts.init($('age-chart'));
    charts.trend = echarts.init($('trend-chart'));
    renderAge(); renderTrend();

    document.querySelectorAll('[data-map-metric]').forEach(button => {
        button.addEventListener('click', () => {
            mapMetric = button.dataset.mapMetric;
            document.querySelectorAll('[data-map-metric]').forEach(control => {
                const selected = control === button;
                control.classList.toggle('is-active', selected);
                control.setAttribute('aria-pressed', String(selected));
            });
            renderMap(); renderRanking();
        });
    });
    document.querySelectorAll('[data-trend-window]').forEach(button => {
        button.addEventListener('click', () => {
            trendMinutes = Number(button.dataset.trendWindow);
            document.querySelectorAll('[data-trend-window]').forEach(control => {
                const selected = control === button;
                control.classList.toggle('is-active', selected);
                control.setAttribute('aria-pressed', String(selected));
            });
            renderTrend();
        });
    });

    const resize = () => {
        Object.values(charts).forEach(chart => chart.resize());
        if (mapReady) renderMap();
    };
    if (typeof ResizeObserver !== 'undefined') {
        const observer = new ResizeObserver(resize);
        document.querySelectorAll('.panel').forEach(panel => observer.observe(panel));
    } else { window.addEventListener('resize', resize); }
    window.setInterval(renderTrend, 5000);
    fetchJSON('/api/dashboard').then(payload => { if (!receivedLiveUpdate) applySnapshot(payload); }).catch(error => {
        if (!snapshotReady) {
            fetchError = 'Unable to load the dashboard snapshot. Waiting for live updates to reconnect…';
            updateConnection();
        }
        console.error(error);
    });
    loadMap();
    connectLiveUpdates();
})();
