let map, vectorSource, clusterSource, routeSource, overlay;
let activeStation = null;
let routePoints = [];

const FUEL_COLORS = {
    'Бензин': '#ff4d4d', 'ДтЛ': '#3399ff', 'ДтЕ': '#ff9933',
    'ДтЗ': '#33ccff', 'ДтА': '#00cc99', 'СУГ': '#9966ff'
};

const FINDER_FUEL_SPECS = [
    { id:'Бензин',    cat:'Бензин', keywords:[] },
    { id:'Аи-95',     cat:'Бензин', keywords:['аи-95','95-к5','премиум'] },
    { id:'Аи-92',     cat:'Бензин', keywords:['аи-92','92-к5','регуляр'] },
    { id:'ДтЛсортС',  cat:'ДтЛ',    keywords:['сорт c','сорт с','минус 5'] },
    { id:'ДтЛ',       cat:'ДтЛ',    keywords:[] },
    { id:'ДтЕсортE',  cat:'ДтЕ',    keywords:['сорт e','сорт е','минус 15'] },
    { id:'ДтЕсортF',  cat:'ДтЕ',    keywords:['сорт f','минус 20'] },
    { id:'ДтЗм32',    cat:'ДтЗ',    keywords:['минус 32','класс 2'] },
    { id:'ДтЗм38',    cat:'ДтЗ',    keywords:['минус 38','класс 3'] },
    { id:'ДтАм44',    cat:'ДтА',    keywords:['минус 44','вид 4'] },
    { id:'ДтАм52',    cat:'ДтА',    keywords:['минус 52'] },
    { id:'СУГ',       cat:'СУГ',    keywords:[] },
];

function toggleMenu(show) {
    document.getElementById('uiPanel').style.display = show ? 'flex' : 'none';
    document.getElementById('restoreBtn').style.display = show ? 'none' : 'flex';
}

function makeDraggable(elmnt, header) {
    let pos1 = 0, pos2 = 0, pos3 = 0, pos4 = 0;
    header.onmousedown = dragMouseDown;
    function dragMouseDown(e) {
        e = e || window.event; pos3 = e.clientX; pos4 = e.clientY;
        document.onmouseup = closeDragElement; document.onmousemove = elementDrag;
    }
    function elementDrag(e) {
        e = e || window.event; e.preventDefault();
        pos1 = pos3 - e.clientX; pos2 = pos4 - e.clientY; pos3 = e.clientX; pos4 = e.clientY;
        elmnt.style.top = (elmnt.offsetTop - pos2) + "px"; elmnt.style.left = (elmnt.offsetLeft - pos1) + "px";
    }
    function closeDragElement() { document.onmouseup = null; document.onmousemove = null; }
}

function getOtpSurcharge(name) {
    const n = (name || '').toLowerCase();
    for (let k in AMN_DATA.otpPrices) {
        if (n.includes(k)) { return (AMN_DATA.otpPrices[k].nalyv || 0) + (AMN_DATA.otpPrices[k].storage || 0); }
    }
    return 0;
}

function getClusterStyle(feature) {
    const features = feature.get('features');
    const size = features.length;

    if (size === 1) {
        const m = features[0].get('data');
        let color = '#999'; let iconSvg = '';
        if (m.company === 'spimex') {
            color = '#808080';
            iconSvg = `<svg width="34" height="44" viewBox="0 0 34 44" xmlns="http://www.w3.org/2000/svg"><path d="M17 0C7.6 0 0 7.6 0 17c0 11.9 17 27 17 27s17-15.1 17-27c0-9.4-7.6-17-17-17z" fill="${color}" stroke="white" stroke-width="2"/><text x="17" y="24" font-size="14" text-anchor="middle" fill="white" font-family="Arial">🏛</text></svg>`;
        } else {
            if (m.company_segment === 'lukoil_term') { color = '#8B4513'; } else { color = '#e11d48'; }
            iconSvg = `<svg width="34" height="44" viewBox="0 0 34 44" xmlns="http://www.w3.org/2000/svg"><path d="M17 0C7.6 0 0 7.6 0 17c0 11.9 17 27 17 27s17-15.1 17-27c0-9.4-7.6-17-17-17z" fill="${color}" stroke="white" stroke-width="2"/><text x="17" y="24" font-size="10" font-weight="bold" text-anchor="middle" fill="white" font-family="Arial">ЛУК</text></svg>`;
        }
        
        let minPrice = 0;
        if (m.fuels && m.fuels.length > 0) { minPrice = Math.min(...m.fuels.map(f => f.price)); }
        const totalMinPrice = minPrice + getOtpSurcharge(m.name);
        const displayPrice = totalMinPrice > 0 ? totalMinPrice.toLocaleString() + ' ₽' : '—';

        return new ol.style.Style({
            image: new ol.style.Icon({ src: 'data:image/svg+xml;charset=utf-8,' + encodeURIComponent(iconSvg.trim()), anchor: [0.5, 1] }),
            text: new ol.style.Text({ text: displayPrice, font: 'bold 11px Arial', offsetY: 10, fill: new ol.style.Fill({ color: '#111' }), backgroundFill: new ol.style.Fill({ color: 'rgba(255,255,255,0.95)' }), backgroundStroke: new ol.style.Stroke({ color: color, width: 2 }), padding: [3, 5, 2, 5] })
        });
    } else {
        return new ol.style.Style({
            image: new ol.style.Circle({ radius: Math.min(25, 12 + size), stroke: new ol.style.Stroke({ color: '#fff', width: 2 }), fill: new ol.style.Fill({ color: '#2563eb' }) }),
            text: new ol.style.Text({ text: size.toString(), font: 'bold 13px Arial', fill: new ol.style.Fill({ color: '#fff' }) })
        });
    }
}

function initMap() {
    const osm = new ol.layer.Tile({ source: new ol.source.OSM(), properties: { name: 'osm' } });
    const sat = new ol.layer.Tile({ source: new ol.source.XYZ({ url: 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', crossOrigin: 'anonymous' }), visible: false, properties: { name: 'satellite' } });

    vectorSource = new ol.source.Vector();
    routeSource = new ol.source.Vector();
    clusterSource = new ol.source.Cluster({ distance: 40, source: vectorSource });

    const clusterLayer = new ol.layer.Vector({ source: clusterSource, style: getClusterStyle });

    map = new ol.Map({
        target: 'map',
        layers: [osm, sat, new ol.layer.Vector({ source: routeSource }), clusterLayer],
        view: new ol.View({ center: ol.proj.fromLonLat([AMN_DATA.config.center_lon, AMN_DATA.config.center_lat]), zoom: AMN_DATA.config.zoom_start })
    });

    overlay = new ol.Overlay({ element: document.getElementById('popup'), autoPan: true, autoPanAnimation: { duration: 250 } });
    map.addOverlay(overlay);
    
    // Делаем оба окна перетаскиваемыми
    makeDraggable(document.getElementById("uiPanel"), document.getElementById("uiHeader"));
    makeDraggable(document.getElementById("routesPanel"), document.getElementById("routesHeader"));

    map.on('singleclick', function(evt) {
        if (evt.originalEvent.altKey) { handleFinderClick(evt.coordinate); return; }
        const feature = map.forEachFeatureAtPixel(evt.pixel, f => f);
        
        if (feature) {
            const clusterFeatures = feature.get('features');
            if (clusterFeatures) {
                if (clusterFeatures.length === 1) { showPopup(clusterFeatures[0].get('data'), evt.coordinate); } 
                else { showClusterListPopup(clusterFeatures, evt.coordinate); }
            }
        } else if (activeStation) {
            addRoutePoint(evt.coordinate);
        } else { overlay.setPosition(undefined); }
    });
    
    map.on('pointermove', function(e) {
        if (e.dragging) return;
        const hit = map.hasFeatureAtPixel(map.getEventPixel(e.originalEvent));
        map.getTargetElement().style.cursor = hit ? 'pointer' : '';
    });
    
    renderOtpTable();
    applyFilters();
}

function showPopup(d, coords) {
    const otp = getOtpSurcharge(d.name);
    const rows = d.fuels.map(f => `<tr style="border-bottom:1px solid #eee;"><td style="padding:6px 0; font-size:12px;"><span style="display:inline-block; width:8px; height:8px; border-radius:50%; background:${FUEL_COLORS[f.cat] || '#999'}; margin-right:5px;"></span><b>${f.name}</b> <span style="font-size:9px; color:#999; background:#f0f0f0; padding:1px 3px; border-radius:3px;">${f.date}</span></td><td style="text-align:right; font-weight:bold; color:#0056b3;">${(f.price + otp).toLocaleString()} ₽</td></tr>`).join('');
    const otpBadge = otp > 0 ? `<div style="font-size:10px; color:#e67e22; margin-bottom:5px;">⚙ Вкл. надбавка ОТП: +${otp} ₽/т</div>` : '';

    document.getElementById('popup-content').innerHTML = `
        <div style="font-size:9px; color:#999; text-transform:uppercase;">${d.company_segment || d.company}</div>
        <div style="font-weight:bold; font-size:15px; margin-bottom:5px;">${d.name}</div>
        ${otpBadge}
        <table style="width:100%; border-collapse:collapse;">${rows}</table>
        <button class="mono-btn" style="margin-top:12px; font-size:10px;" onclick="startLogistics('${d.code}')">🚚 МАРШРУТ ОТСЮДА</button>
    `;
    overlay.setPosition(coords);
}

function showClusterListPopup(features, coords) {
    let listHtml = `<div style="font-weight:bold; font-size:13px; margin-bottom:8px; border-bottom: 2px solid #2563eb; padding-bottom:5px;">Базы в этом районе (${features.length}):</div>`;
    listHtml += `<div style="max-height: 200px; overflow-y: auto; padding-right:5px;">`;
    features.forEach(f => {
        const d = f.get('data');
        let minPrice = 0;
        if (d.fuels && d.fuels.length > 0) { minPrice = Math.min(...d.fuels.map(fuel => fuel.price)); }
        const total = minPrice + getOtpSurcharge(d.name);
        const iconChar = (d.company === 'spimex') ? '🏛' : 'ЛУК';
        listHtml += `<div style="padding: 6px 0; border-bottom: 1px solid #eee; font-size: 12px; display:flex; justify-content: space-between; align-items: center; cursor:pointer;" onclick="focusStation('${d.code}')">
                <div style="flex:1; padding-right: 10px;"><span style="font-size: 10px; color: #666;">${iconChar} ${d.company_segment || d.company}</span><br><b>${d.name}</b></div>
                <div style="color:#0056b3; font-weight:bold; white-space: nowrap;">от ${total > 0 ? total.toLocaleString() : '—'} ₽</div></div>`;
    });
    listHtml += `</div><div style="font-size:10px; color:#999; margin-top:8px; text-align:center;">Нажмите на базу для перехода</div>`;
    document.getElementById('popup-content').innerHTML = listHtml;
    overlay.setPosition(coords);
}

window.focusStation = function(code) {
    const f = vectorSource.getFeatures().find(feat => feat.get('data').code === code);
    if (f) {
        map.getView().animate({ center: f.getGeometry().getCoordinates(), zoom: 12, duration: 500 });
        showPopup(f.get('data'), f.getGeometry().getCoordinates());
    }
};

window.renderStationList = function() {
    const listEl = document.getElementById('stationList');
    const sortType = document.getElementById('listSort').value;
    const searchQ = document.getElementById('listSearch').value.toLowerCase();
    
    let bases = [];
    vectorSource.getFeatures().forEach(f => {
        const d = f.get('data');
        if (searchQ && !d.name.toLowerCase().includes(searchQ)) return;
        let minPrice = Infinity;
        if (d.fuels && d.fuels.length > 0) { minPrice = Math.min(...d.fuels.map(fuel => fuel.price)); }
        const total = minPrice + getOtpSurcharge(d.name);
        bases.push({ data: d, price: total === Infinity ? 0 : total, feature: f });
    });
    
    if (sortType === 'price_asc') bases.sort((a,b) => (a.price || 999999) - (b.price || 999999));
    else if (sortType === 'price_desc') bases.sort((a,b) => b.price - a.price);
    else bases.sort((a,b) => a.data.name.localeCompare(b.data.name));
    
    if (bases.length === 0) { listEl.innerHTML = '<div style="padding:10px; text-align:center; color:#999;">Ничего не найдено</div>'; return; }

    listEl.innerHTML = bases.map(b => {
        let dotColor = '#999';
        if (b.data.company === 'spimex') dotColor = '#808080';
        else if (b.data.company_segment === 'lukoil_term') dotColor = '#8B4513';
        else dotColor = '#e11d48';
        return `<div class="station-list-item" onclick="focusStation('${b.data.code}')"><div style="font-weight: bold; font-size: 11px; margin-bottom: 3px;">${b.data.name}</div><div style="display: flex; justify-content: space-between; align-items: center; font-size: 10px; color: #666;"><span><span class="dot" style="background:${dotColor}; width:8px; height:8px;"></span> ${b.data.company_segment || b.data.company}</span><span style="color: #28a745; font-weight: bold; font-size:12px;">${b.price > 0 ? b.price.toLocaleString() + ' ₽' : '—'}</span></div></div>`
    }).join('');
};

function renderOtpTable() {
    let html = '';
    for(let k in AMN_DATA.otpPrices) {
        const p = AMN_DATA.otpPrices[k]; const safeK = k.replace(/ /g, '_');
        html += `<tr><td>${k}</td><td><input type="number" id="on_${safeK}" value="${p.nalyv}" oninput="recalcOtp('${k}')" class="mono-input" style="padding:4px;"></td><td><input type="number" id="os_${safeK}" value="${p.storage}" oninput="recalcOtp('${k}')" class="mono-input" style="padding:4px;"></td><td id="sum_${safeK}" style="font-weight:700; color:#e67e22;">${(p.nalyv + p.storage).toFixed(2)}</td></tr>`;
    }
    document.getElementById('otpTableBody').innerHTML = html;
}

window.recalcOtp = function(k) {
    const safeK = k.replace(/ /g, '_');
    const n = parseFloat(document.getElementById('on_'+safeK).value) || 0; const s = parseFloat(document.getElementById('os_'+safeK).value) || 0;
    document.getElementById('sum_'+safeK).textContent = (n + s).toFixed(2);
}

function saveOtp() { document.getElementById('otpModal').style.display='none'; clusterSource.refresh(); renderStationList(); }
function changeMapLayer() { const val = document.getElementById('mapProvider').value; map.getLayers().forEach(l => { if (l.get('properties')?.name) l.setVisible(l.get('properties').name === val); }); }

function applyFilters() {
    const sp = document.getElementById('flt_spimex').checked, l_s = document.getElementById('flt_luk_spot').checked, l_t = document.getElementById('flt_luk_term').checked;
    const fuels = { 'Бензин': document.getElementById('flt_benz').checked, 'ДтЛ': document.getElementById('flt_dtl').checked, 'ДтЕ': document.getElementById('flt_dte').checked, 'ДтЗ': document.getElementById('flt_dtz').checked, 'ДтА': document.getElementById('flt_dta').checked, 'СУГ': document.getElementById('flt_sug').checked };
    
    vectorSource.clear(); const activeFeatures = [];
    AMN_DATA.markers.forEach(m => {
        let v = false;
        if (m.company === 'spimex' && sp) v = true;
        if ((m.company_segment === 'lukoil_spot' || m.company_segment === 'lukoil_other') && l_s) v = true;
        if (m.company_segment === 'lukoil_term' && l_t) v = true;
        if (v) v = m.categories.some(c => fuels[c]);
        if (v) activeFeatures.push(new ol.Feature({ geometry: new ol.geom.Point(ol.proj.fromLonLat([m.lon, m.lat])), data: m }));
    });
    vectorSource.addFeatures(activeFeatures); renderStationList();
}

// === ЛОГИСТИКА (РУЧНАЯ) ===
function startLogistics(code) {
    const f = vectorSource.getFeatures().find(f => f.get('data').code === code);
    if (!f) return;
    activeStation = f.get('data');
    routePoints = []; routeSource.clear();
    document.getElementById('routeStatus').innerHTML = `📍 <b>${activeStation.name}</b><br>Кликайте на карту, чтобы поставить точки доставки`;
    overlay.setPosition(undefined);
}

function addRoutePoint(coord) {
    routePoints.push(ol.proj.toLonLat(coord));
    const dot = new ol.Feature({ geometry: new ol.geom.Point(coord) });
    dot.setStyle(new ol.style.Style({ image: new ol.style.Circle({ radius: 5, fill: new ol.style.Fill({color:'#f59e0b'}), stroke: new ol.style.Stroke({color:'#fff', width:2}) }) }));
    routeSource.addFeature(dot);
}

function clearRoute() {
    activeStation = null; routePoints = []; routeSource.clear();
    document.getElementById('routeStatus').innerHTML = "Откройте карточку базы на карте и нажмите 'Маршрут отсюда'";
}

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
            routeSource.clear(); routeSource.addFeature(feat);
            
            rawPoints.forEach((p, i) => {
                const dot = new ol.Feature({ geometry: new ol.geom.Point(ol.proj.fromLonLat(p)) });
                dot.setStyle(new ol.style.Style({ image: new ol.style.Circle({ radius: i===0?6:5, fill: new ol.style.Fill({color: i===0?'#28a745':'#f59e0b'}), stroke: new ol.style.Stroke({color:'#fff', width:2}) }) }));
                routeSource.addFeature(dot);
            });

            const km = json.routes[0].distance / 1000;
            const cost = (km * document.getElementById('tariff').value) / document.getElementById('tonnage').value;
            document.getElementById('routeStatus').innerHTML = `🛣 <b>${km.toFixed(1)} км</b> | Стоимость: +<b>${Math.round(cost).toLocaleString()} ₽/т</b>`;
        }
    } catch(e) { document.getElementById('routeStatus').innerHTML = "❌ Ошибка OSRM"; }
}

// === ЛУЧШАЯ ЦЕНА ТОП-3 (АВТОМАТИЧЕСКАЯ) ===
async function handleFinderClick(coord) {
    routeSource.clear(); 
    overlay.setPosition(undefined);
    
    // Показываем новое окно с результатами
    const routesPanel = document.getElementById('routesPanel');
    const routesContent = document.getElementById('routesContent');
    routesPanel.style.display = 'flex';
    routesContent.innerHTML = "<div style='text-align:center; padding:20px; color:#555;'>⏳ Ищем лучшие базы...</div>";

    const center = ol.proj.toLonLat(coord);
    const radius = parseFloat(document.getElementById('finderRadius').value) || 300;
    const specId = document.getElementById('finderFuel').value;
    const spec = FINDER_FUEL_SPECS.find(s => s.id === specId);
    if (!spec) return;
    
    const candidates = [];
    
    vectorSource.getFeatures().forEach(f => {
        const d = f.get('data');
        const dist = Math.sqrt(Math.pow(d.lon - center[0], 2) + Math.pow(d.lat - center[1], 2)) * 111;
        if (dist > radius) return;

        let bestFuel = null, bestPrice = Infinity;
        for (let fuel of d.fuels) {
            if (fuel.cat !== spec.cat) continue;
            if (spec.keywords.length > 0 && !spec.keywords.some(kw => fuel.name.toLowerCase().includes(kw))) continue;
            if (fuel.price < bestPrice) { bestPrice = fuel.price; bestFuel = fuel; }
        }
        
        if (bestFuel) {
            const otp = getOtpSurcharge(d.name);
            candidates.push({ data: d, fuelName: bestFuel.name, basePrice: bestPrice, otp: otp, distKm: dist });
        }
    });

    if (candidates.length === 0) { routesContent.innerHTML = "<div style='text-align:center; padding:20px; color:#e11d48;'>Ничего не найдено в заданном радиусе</div>"; return; }

    const tariff = parseFloat(document.getElementById('tariff').value) || 170;
    const tonnage = parseFloat(document.getElementById('tonnage').value) || 25;
    
    candidates.forEach(c => c.estTotal = c.basePrice + c.otp + (c.distKm * tariff / tonnage));
    candidates.sort((a,b) => a.estTotal - b.estTotal);
    const top10 = candidates.slice(0, 10); 
    
    routesContent.innerHTML = "<div style='text-align:center; padding:20px; color:#555;'>⏳ Рассчитываем автодороги OSRM...</div>";
    const results = [];

    // Быстрый запрос OSRM для Топ-10
    for (let c of top10) {
        try {
            const url = `https://router.project-osrm.org/route/v1/driving/${center[0]},${center[1]};${c.data.lon},${c.data.lat}?overview=false`;
            const resp = await fetch(url);
            const json = await resp.json();
            
            if (json.routes && json.routes.length > 0) {
                const roadKm = json.routes[0].distance / 1000;
                results.push({
                    name: c.data.name, fuelName: c.fuelName, basePrice: c.basePrice, otp: c.otp,
                    roadKm: roadKm, totalCost: c.basePrice + c.otp + ((roadKm * tariff) / tonnage), code: c.data.code,
                    lon: c.data.lon, lat: c.data.lat
                });
            }
        } catch(e) { console.error("OSRM error", e); }
        await new Promise(r => setTimeout(r, 200)); 
    }

    if (results.length === 0) { routesContent.innerHTML = "<div style='text-align:center; padding:20px; color:#e11d48;'>❌ Ошибка сервера маршрутов</div>"; return; }

    results.sort((a, b) => a.totalCost - b.totalCost);

    // --- ФОРМИРУЕМ КАРТОЧКИ С ДЕТАЛИЗАЦИЕЙ В НОВОМ ОКНЕ ---
    let htmlStr = results.slice(0, 3).map((c, i) => {
        const basePriceTotal = c.basePrice + c.otp;
        const deliveryCost = c.totalCost - basePriceTotal;
        const badgeColor = i === 0 ? '#28a745' : (i === 1 ? '#f59e0b' : '#64748b');

        return `
        <div style="background:#fff; padding:12px; border-radius:10px; margin-bottom:12px; box-shadow:0 4px 6px rgba(0,0,0,0.05); border:1px solid #e2e8f0; border-left: 5px solid ${badgeColor}; cursor:pointer;" onclick="focusStation('${c.code}')">
            <div style="font-weight:bold; font-size:13px; margin-bottom:6px; color:#1e293b;">
                ${['🥇 1 место', '🥈 2 место', '🥉 3 место'][i]} — ${c.name}
            </div>
            <div style="color:#64748b; font-size:11px; margin-bottom:10px; border-bottom: 1px dashed #e2e8f0; padding-bottom: 8px;">
                ${c.fuelName} • 🛣 <b>${c.roadKm.toFixed(1)} км</b>
            </div>
            
            <div style="display:flex; justify-content:space-between; font-size:11px; margin-bottom:6px; color:#475569;">
                <span>Прайс базы ${c.otp > 0 ? '(+ОТП)' : ''}:</span>
                <span style="font-weight:bold;">${basePriceTotal.toLocaleString()} ₽</span>
            </div>
            <div style="display:flex; justify-content:space-between; font-size:11px; margin-bottom:10px; color:#475569;">
                <span>Доставка:</span>
                <span style="font-weight:bold;">+${Math.round(deliveryCost).toLocaleString()} ₽</span>
            </div>
            <div style="display:flex; justify-content:space-between; font-size:14px; color:${badgeColor}; font-weight:bold; background: #f8fafc; padding: 6px; border-radius: 6px;">
                <span>Итого:</span>
                <span>${Math.round(c.totalCost).toLocaleString()} ₽/т</span>
            </div>
        </div>
        `;
    }).join('');

    routesContent.innerHTML = htmlStr;

    // Тяжелый запрос геометрии только для Победителя (Топ-1)
    const best = results[0];
    try {
        const urlGeom = `https://router.project-osrm.org/route/v1/driving/${center[0]},${center[1]};${best.lon},${best.lat}?overview=full&geometries=geojson`;
        const respGeom = await fetch(urlGeom);
        const jsonGeom = await respGeom.json();
        
        if (jsonGeom.routes && jsonGeom.routes.length > 0) {
            const format = new ol.format.GeoJSON();
            const feat = format.readFeature(jsonGeom.routes[0].geometry, { dataProjection: 'EPSG:4326', featureProjection: 'EPSG:3857' });
            feat.setStyle(new ol.style.Style({ stroke: new ol.style.Stroke({ color: '#28a745', width: 4 }) }));
            routeSource.addFeature(feat);

            const startDot = new ol.Feature({ geometry: new ol.geom.Point(coord) });
            startDot.setStyle(new ol.style.Style({ image: new ol.style.Circle({ radius: 6, fill: new ol.style.Fill({color:'#f59e0b'}), stroke: new ol.style.Stroke({color:'#fff', width:2}) }) }));
            routeSource.addFeature(startDot);

            map.getView().fit(feat.getGeometry().getExtent(), { padding: [50, 50, 50, 380], duration: 800, maxZoom: 12 });
        }
    } catch(e) { console.log("Geometry fetch failed", e); }
}

document.addEventListener("DOMContentLoaded", initMap);
