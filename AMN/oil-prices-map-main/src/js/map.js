/**
 * AMN Map Logic v3.0 (Professional Edition)
 */

let map, vectorSource, routeSource, overlay;
let activeStation = null;
let routePoints = [];

const FUEL_COLORS = {
    'Бензин': '#ff4d4d', 'ДтЛ': '#3399ff', 'ДтЕ': '#ff9933',
    'ДтЗ': '#33ccff', 'ДтА': '#00cc99', 'СУГ': '#9966ff'
};

// --- ИНТЕРФЕЙС ---
function toggleMenu(show) {
    document.getElementById('uiPanel').style.display = show ? 'flex' : 'none';
    document.getElementById('restoreBtn').style.display = show ? 'none' : 'flex';
}

function getMarkerStyle(m) {
    const fuelCat = m.categories[0] || 'Бензин';
    const color = FUEL_COLORS[fuelCat] || '#999';
    
    // Иконка: 🏛 для SPIMEX, 💧 для LUKOIL
    let iconChar = (m.company === 'spimex') ? '🏛' : '💧';
    let labelColor = (m.company_segment === 'lukoil_term') ? '#ffeb3b' : 'white';
    
    return new ol.style.Style({
        image: new ol.style.Icon({
            src: 'data:image/svg+xml;charset=utf-8,' + encodeURIComponent(`
                <svg width="34" height="44" viewBox="0 0 34 44" xmlns="http://www.w3.org/2000/svg">
                    <path d="M17 0C7.6 0 0 7.6 0 17c0 11.9 17 27 17 27s17-15.1 17-27c0-9.4-7.6-17-17-17z" fill="${color}" stroke="white" stroke-width="2"/>
                    <text x="17" y="24" font-size="15" text-anchor="middle" fill="${labelColor}" font-family="Arial">${iconChar}</text>
                </svg>`.trim()),
            anchor: [0.5, 1]
        })
    });
}

// --- ИНИЦИАЛИЗАЦИЯ ---
function initMap() {
    const osm = new ol.layer.Tile({ source: new ol.source.OSM(), properties: { name: 'osm' } });
    const sat = new ol.layer.Tile({
        source: new ol.source.XYZ({
            url: 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
            crossOrigin: 'anonymous'
        }),
        visible: false, properties: { name: 'satellite' }
    });

    vectorSource = new ol.source.Vector();
    routeSource = new ol.source.Vector();
    
    AMN_DATA.markers.forEach(m => {
        const f = new ol.Feature({ geometry: new ol.geom.Point(ol.proj.fromLonLat([m.lon, m.lat])), data: m });
        f.setStyle(getMarkerStyle(m));
        vectorSource.addFeature(f);
    });

    map = new ol.Map({
        target: 'map',
        layers: [osm, sat, new ol.layer.Vector({ source: routeSource }), new ol.layer.Vector({ source: vectorSource })],
        view: new ol.View({ center: ol.proj.fromLonLat([AMN_DATA.config.center_lon, AMN_DATA.config.center_lat]), zoom: AMN_DATA.config.zoom_start })
    });

    overlay = new ol.Overlay({ element: document.getElementById('popup'), autoPan: true });
    map.addOverlay(overlay);

    map.on('singleclick', async function(evt) {
        if (evt.originalEvent.altKey) { handleFinderClick(evt.coordinate); return; }
        const feature = map.forEachFeatureAtPixel(evt.pixel, f => f);
        if (feature && feature.get('data')) {
            showPopup(feature.get('data'), evt.coordinate);
        } else if (activeStation) {
            addRoutePoint(evt.coordinate);
        }
    });
    
    renderOtpTable();
    applyFilters();
}

// --- ПОПАП ---
function showPopup(d, coords) {
    const prices = AMN_DATA.otpPrices;
    let otp = 0;
    for (let k in prices) { if (d.name.toLowerCase().includes(k)) otp = prices[k].nalyv + prices[k].storage; }

    const rows = d.fuels.map(f => `
        <tr style="border-bottom:1px solid #eee;">
            <td style="padding:6px 0; font-size:12px;">
                <span style="display:inline-block; width:8px; height:8px; border-radius:50%; background:${FUEL_COLORS[f.cat] || '#999'}; margin-right:5px;"></span>
                <b>${f.name}</b> <span style="font-size:9px; color:#999; background:#f0f0f0; padding:1px 3px; border-radius:3px;">${f.date}</span>
            </td>
            <td style="text-align:right; font-weight:bold; color:#0056b3;">${(f.price + otp).toLocaleString()} ₽</td>
        </tr>`).join('');

    document.getElementById('popup-content').innerHTML = `
        <div style="font-size:9px; color:#999; text-transform:uppercase;">${d.company_segment || d.company}</div>
        <div style="font-weight:bold; font-size:15px; margin-bottom:10px; border-bottom:1px solid #eee; padding-bottom:5px;">${d.name}</div>
        <table style="width:100%; border-collapse:collapse;">${rows}</table>
        <button class="mono-btn" style="margin-top:12px; font-size:10px;" onclick="startLogistics('${d.code}')">🚚 РАССЧИТАТЬ ЛОГИСТИКУ</button>
    `;
    overlay.setPosition(coords);
}

// --- РОУТИНГ ---
async function buildRouteManually() {
    if (!activeStation || routePoints.length === 0) return;
    const rawPoints = [[activeStation.lon, activeStation.lat], ...routePoints];
    document.getElementById('routeStatus').innerHTML = "⌛ Считаю дорогу...";
    try {
        const coordsStr = rawPoints.map(p => `${p[0]},${p[1]}`).join(';');
        const url = `https://router.project-osrm.org/route/v1/driving/${coordsStr}?overview=full&geometries=geojson`;
        const resp = await fetch(url);
        const json = await resp.json();
        if (json.routes && json.routes.length) {
            const format = new ol.format.GeoJSON();
            const feat = format.readFeature(json.routes[0].geometry, { dataProjection: 'EPSG:4326', featureProjection: 'EPSG:3857' });
            feat.setStyle(new ol.style.Style({ stroke: new ol.style.Stroke({ color: '#2563eb', width: 5 }) }));
            routeSource.clear();
            routeSource.addFeature(feat);
            const km = json.routes[0].distance / 1000;
            const cost = (km * document.getElementById('tariff').value) / document.getElementById('tonnage').value;
            document.getElementById('routeStatus').innerHTML = `🛣 <b>${km.toFixed(1)} км</b> | +<b>${Math.round(cost)} ₽/т</b>`;
        }
    } catch(e) { document.getElementById('routeStatus').innerHTML = "❌ Ошибка OSRM"; }
}

function startLogistics(code) {
    const f = vectorSource.getFeatures().find(f => f.get('data').code === code);
    if (!f) return;
    activeStation = f.get('data');
    routePoints = []; routeSource.clear();
    document.getElementById('routeStatus').innerHTML = `📍 <b>${activeStation.name}</b><br>Ставьте точки доставки на карте`;
    overlay.setPosition(undefined);
}

function addRoutePoint(coord) {
    routePoints.push(ol.proj.toLonLat(coord));
    const dot = new ol.Feature({ geometry: new ol.geom.Point(coord) });
    dot.setStyle(new ol.style.Style({ image: new ol.style.Circle({ radius: 5, fill: new ol.style.Fill({color:'#f59e0b'}), stroke: new ol.style.Stroke({color:'#fff', width:2}) }) }));
    routeSource.addFeature(dot);
}

// --- ВСПОМОГАТЕЛЬНОЕ ---
function renderOtpTable() {
    let html = '';
    for(let k in AMN_DATA.otpPrices) {
        const p = AMN_DATA.otpPrices[k];
        const safeK = k.replace(/ /g, '_');
        html += `<tr><td>${k}</td>
            <td><input type="number" id="on_${safeK}" value="${p.nalyv}" oninput="recalcOtp('${k}')" class="mono-input" style="padding:4px;"></td>
            <td><input type="number" id="os_${safeK}" value="${p.storage}" oninput="recalcOtp('${k}')" class="mono-input" style="padding:4px;"></td>
            <td id="sum_${safeK}" style="font-weight:700; color:#e67e22;">${(p.nalyv + p.storage).toFixed(2)}</td></tr>`;
    }
    document.getElementById('otpTableBody').innerHTML = html;
}

window.recalcOtp = function(k) {
    const safeK = k.replace(/ /g, '_');
    const n = parseFloat(document.getElementById('on_'+safeK).value) || 0;
    const s = parseFloat(document.getElementById('os_'+safeK).value) || 0;
    document.getElementById('sum_'+safeK).textContent = (n + s).toFixed(2);
}

function saveOtp() { document.getElementById('otpModal').style.display='none'; alert("Надбавки применены"); }

function changeMapLayer() {
    const val = document.getElementById('mapProvider').value;
    map.getLayers().forEach(l => { if (l.get('properties')?.name) l.setVisible(l.get('properties').name === val); });
}

function applyFilters() {
    const sp = document.getElementById('flt_spimex').checked;
    const l_s = document.getElementById('flt_luk_spot').checked;
    const l_t = document.getElementById('flt_luk_term').checked;
    const fuels = { 'Бензин': document.getElementById('flt_benz').checked, 'ДтЛ': document.getElementById('flt_dtl').checked, 'ДтЕ': document.getElementById('flt_dte').checked, 'ДтЗ': document.getElementById('flt_dtz').checked, 'ДтА': document.getElementById('flt_dta').checked, 'СУГ': document.getElementById('flt_sug').checked };
    vectorSource.getFeatures().forEach(f => {
        const d = f.get('data'); let v = false;
        if (d.company === 'spimex' && sp) v = true;
        if ((d.company_segment === 'lukoil_spot' || d.company_segment === 'lukoil_other') && l_s) v = true;
        if (d.company_segment === 'lukoil_term' && l_t) v = true;
        if (v) v = d.categories.some(c => fuels[c]);
        f.setStyle(v ? getMarkerStyle(d) : null);
    });
}

function doSearch() {
    const q = document.getElementById('searchInput').value.toLowerCase();
    const f = vectorSource.getFeatures().find(f => f.get('data').name.toLowerCase().includes(q));
    if (f) map.getView().animate({ center: f.getGeometry().getCoordinates(), zoom: 12 });
}

async function handleFinderClick(coord) {
    const center = ol.proj.toLonLat(coord);
    const radius = parseFloat(document.getElementById('finderRadius').value);
    const fuelType = document.getElementById('finderFuel').value;
    const candidates = [];
    vectorSource.getFeatures().forEach(f => {
        if (f.getStyle() === null) return;
        const d = f.get('data');
        const dist = Math.sqrt(Math.pow(d.lon - center[0], 2) + Math.pow(d.lat - center[1], 2)) * 111;
        if (dist > radius) return;
        const fuel = d.fuels.find(x => x.cat === fuelType);
        if (fuel) candidates.push({ name: d.name, price: fuel.price, dist: dist });
    });
    candidates.sort((a,b) => a.price - b.price);
    document.getElementById('finderResult').innerHTML = candidates.slice(0,3).map(c => `<div style="background:#f0fff4; padding:8px; border-radius:10px; margin-top:5px; border:1px solid #c6f6d5;"><b>${c.name}</b><br>Цена: ${c.price} ₽ (${c.dist.toFixed(0)} км)</div>`).join('') || "Ничего не найдено";
}

document.addEventListener("DOMContentLoaded", initMap);
