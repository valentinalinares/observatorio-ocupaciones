    const DATA = window.OBSERVATORIO_DATA;

    const SOURCE_LABELS = {
      'ENAHO|anual': 'ENAHO anual',
      'EPEN|departamentos_anual': 'EPEN departamentos anual',
      'EPEN|lima_movil': 'EPEN Lima móvil'
    };

    const SERIES_COLORS = {
      'ENAHO|anual': '#0f766e',
      'EPEN|departamentos_anual': '#b45309',
      'EPEN|lima_movil': '#334155'
    };

    const FILES = [
      'data/processed/empleo_base_estandarizada.csv.gz',
      'data/processed/resumen_ocupacion_anio.csv.gz',
      'data/processed/mercado_laboral_periodo.csv.gz',
      'data/processed/mercado_laboral_mes.csv.gz',
      'data/processed/ocupacion_crosswalk_4d.csv',
      'data/processed/disponibilidad_fuentes.csv',
      'notebooks/01_workflow_bases_empleo.ipynb'
    ];

    const SUMMARY_METRIC_LABELS = {
      population: 'Población ocupada',
      income_mean: 'Ingreso medio',
      income_median: 'Ingreso mediano',
      hours_mean: 'Horas semanales',
      women_share: '% mujeres',
      rural_share: '% rural',
      age_mean: 'Edad media',
      cases: 'Casos muestrales'
    };

    const SUMMARY_METRIC_FORMATTERS = {
      population: value => numberFormat(value, 0),
      income_mean: value => currencyFormat(value),
      income_median: value => currencyFormat(value),
      hours_mean: value => numberFormat(value, 1),
      women_share: value => percentFormat(value),
      rural_share: value => percentFormat(value),
      age_mean: value => `${numberFormat(value, 1)} años`,
      cases: value => numberFormat(value, 0)
    };

    const PANORAMA_METRIC_FORMATTERS = {
      pet_population: value => numberFormat(value, 0),
      pea_population: value => numberFormat(value, 0),
      occupied_population: value => numberFormat(value, 0),
      unemployed_open_population: value => numberFormat(value, 0),
      hidden_unemployment_population: value => numberFormat(value, 0),
      inactive_population: value => numberFormat(value, 0),
      activity_rate_pet: value => percentFormat(value),
      employment_rate_pet: value => percentFormat(value),
      unemployment_rate_pea: value => percentFormat(value),
      income_monthly_weighted_mean: value => currencyFormat(value),
      hours_week_weighted_mean: value => numberFormat(value, 1),
      occupation_count: value => numberFormat(value, 0)
    };

    const PERIOD_END_MONTHS = {
      'Nov-Dic-Ene': 1,
      'Dic-Ene-Feb': 2,
      'Ene-Feb-Mar': 3,
      'Feb-Mar-Abr': 4,
      'Mar-Abr-May': 5,
      'Abr-May-Jun': 6,
      'May-Jun-Jul': 7,
      'Jun-Jul-Ago': 8,
      'Jul-Ago-Set': 9,
      'Ago-Set-Oct': 10,
      'Set-Oct-Nov': 11,
      'Oct-Nov-Dic': 12
    };

    function sourceKey(row) {
      return `${row.source}|${row.survey_variant}`;
    }

    function sourceLabel(row) {
      return SOURCE_LABELS[sourceKey(row)] || `${row.source} · ${row.survey_variant}`;
    }

    function numberFormat(value, digits = 0) {
      if (value === null || value === undefined || Number.isNaN(value)) return 'Sin dato';
      return new Intl.NumberFormat('es-PE', { maximumFractionDigits: digits, minimumFractionDigits: digits }).format(value);
    }

    function percentFormat(value, digits = 1) {
      if (value === null || value === undefined || Number.isNaN(value)) return 'Sin dato';
      return `${numberFormat(value * 100, digits)}%`;
    }

    function currencyFormat(value) {
      if (value === null || value === undefined || Number.isNaN(value)) return 'Sin dato';
      return `S/ ${numberFormat(value, 0)}`;
    }

    function yearDelta(current, previous) {
      if (current == null || previous == null || Number.isNaN(current) || Number.isNaN(previous)) return 'Sin comparación';
      const diff = current - previous;
      const sign = diff > 0 ? '+' : '';
      return `${sign}${numberFormat(diff, 1)}`;
    }

    function panoramaMetricDelta(metric, current, previous) {
      if (current == null || previous == null || Number.isNaN(current) || Number.isNaN(previous)) return 'Sin comparación';
      const diff = current - previous;
      const sign = diff > 0 ? '+' : '';
      if (['activity_rate_pet', 'employment_rate_pet', 'unemployment_rate_pea'].includes(metric)) {
        return `${sign}${numberFormat(diff * 100, 1)} pp`;
      }
      if (metric === 'income_monthly_weighted_mean') {
        return `${diff < 0 ? '-' : sign}S/ ${numberFormat(Math.abs(diff), 0)}`;
      }
      if (metric === 'hours_week_weighted_mean') {
        return `${sign}${numberFormat(diff, 1)}`;
      }
      return `${sign}${numberFormat(diff, 0)}`;
    }

    function unique(values) {
      return [...new Set(values)];
    }

    function sortBy(arr, field, desc = false) {
      return [...arr].sort((a, b) => {
        const av = a[field] ?? (desc ? -Infinity : Infinity);
        const bv = b[field] ?? (desc ? -Infinity : Infinity);
        return desc ? bv - av : av - bv;
      });
    }

    function downloadText(filename, text, mime = 'text/plain;charset=utf-8') {
      const blob = new Blob([text], { type: mime });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      a.click();
      URL.revokeObjectURL(url);
    }

    function recordsToCSV(records) {
      if (!records.length) return '';
      const headers = Object.keys(records[0]);
      const escape = (value) => {
        if (value == null) return '';
        const str = String(value);
        return /[",\n]/.test(str) ? `"${str.replace(/"/g, '""')}"` : str;
      };
      return [headers.join(','), ...records.map(row => headers.map(h => escape(row[h])).join(','))].join('\n');
    }

    function extractPeriodCore(periodLabel) {
      const label = String(periodLabel || '');
      const match = label.match(/\(([^)]+)\)/);
      return match ? match[1] : label;
    }

    function seriesSortValue(row) {
      if (row.survey_variant === 'lima_movil') {
        const endMonth = PERIOD_END_MONTHS[extractPeriodCore(row.period_label)] || 99;
        return (Number(row.year) * 100) + endMonth;
      }
      return Number(row.year);
    }

    function seriesXAxisLabel(row) {
      if (row.survey_variant === 'lima_movil') {
        return `${row.year} ${extractPeriodCore(row.period_label)}`;
      }
      return String(row.year);
    }

    function periodEndMonth(periodLabel) {
      return PERIOD_END_MONTHS[extractPeriodCore(periodLabel)] || 0;
    }

    function isLimaMovilSource(sourceValue) {
      return sourceValue === 'EPEN|lima_movil';
    }

    function panoramaSourceRows(sourceValue) {
      return DATA.market_period_overview.filter(row => sourceKey(row) === sourceValue);
    }

    function panoramaYearRows(sourceValue, yearValue) {
      return panoramaSourceRows(sourceValue)
        .filter(row => Number(row.year) === Number(yearValue))
        .sort((a, b) => periodEndMonth(a.period_label) - periodEndMonth(b.period_label));
    }

    function panoramaCurrentRow(sourceValue, yearValue) {
      const rows = panoramaYearRows(sourceValue, yearValue);
      return rows.length ? rows[rows.length - 1] : null;
    }

    function panoramaPreviousRow(sourceValue, currentRow) {
      if (!currentRow) return null;
      if (isLimaMovilSource(sourceValue)) {
        const core = extractPeriodCore(currentRow.period_label);
        return panoramaSourceRows(sourceValue).find(row =>
          Number(row.year) === Number(currentRow.year) - 1 &&
          extractPeriodCore(row.period_label) === core
        ) || null;
      }
      return panoramaSourceRows(sourceValue).find(row => Number(row.year) === Number(currentRow.year) - 1) || null;
    }

    function panoramaHistoricalSeries(sourceValue, currentRow) {
      const rows = panoramaSourceRows(sourceValue);
      if (isLimaMovilSource(sourceValue) && currentRow) {
        const core = extractPeriodCore(currentRow.period_label);
        return rows
          .filter(row => extractPeriodCore(row.period_label) === core)
          .sort((a, b) => Number(a.year) - Number(b.year));
      }
      return rows.sort((a, b) => Number(a.year) - Number(b.year));
    }

    function formatMetricValue(metric, value) {
      if (value === null || value === undefined || Number.isNaN(value)) return 'Sin dato';
      if (metric === 'income_mean' || metric === 'income_median') return currencyFormat(value);
      if (metric === 'women_share' || metric === 'rural_share') return percentFormat(value);
      if (metric === 'hours_mean' || metric === 'age_mean') return numberFormat(value, 1);
      return numberFormat(value, 0);
    }

    function formatMetricDelta(metric, value) {
      if (value === null || value === undefined || Number.isNaN(value)) return 'Sin dato';
      const sign = value > 0 ? '+' : '';
      if (metric === 'income_mean' || metric === 'income_median') {
        return `${value < 0 ? '-' : sign}S/ ${numberFormat(Math.abs(value), 0)}`;
      }
      if (metric === 'women_share' || metric === 'rural_share') {
        return `${sign}${numberFormat(value * 100, 1)} pp`;
      }
      if (metric === 'hours_mean' || metric === 'age_mean') {
        return `${sign}${numberFormat(value, 1)}`;
      }
      return `${sign}${numberFormat(value, 0)}`;
    }

    function svgLineChart({ points, xField, yField, color = '#0f766e', formatter, xFormatter, xLabelField, minZero = false, labelRotation = 0, tickEvery = 1 }) {
      if (!points.length) return '<div class="empty">No hay datos para este filtro.</div>';
      const width = 760;
      const height = 240;
      const padding = { top: 18, right: 18, bottom: Math.abs(labelRotation) > 0 ? 80 : 36, left: 58 };
      const ys = points.map(p => Number(p[yField])).filter(v => Number.isFinite(v));
      if (!ys.length) return '<div class="empty">No hay valores válidos para graficar.</div>';
      const minY = Math.min(...ys);
      const maxY = Math.max(...ys);
      const yPad = minY === maxY ? Math.max(1, Math.abs(minY) * .1 || 1) : (maxY - minY) * 0.12;
      const yMin = minZero ? 0 : minY - yPad;
      const yMax = maxY === 0 && minZero ? 1 : maxY + yPad;
      const xScale = (index) => padding.left + (index / Math.max(points.length - 1, 1)) * (width - padding.left - padding.right);
      const yScale = (value) => height - padding.bottom - ((value - yMin) / (yMax - yMin)) * (height - padding.top - padding.bottom);
      const polyline = points.map((p, i) => `${xScale(i)},${yScale(Number(p[yField]))}`).join(' ');
      const yTicks = 4;
      const grid = [];
      for (let i = 0; i <= yTicks; i++) {
        const value = yMin + ((yMax - yMin) / yTicks) * i;
        const y = yScale(value);
        grid.push(`<line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" stroke="#ece6da" />`);
        grid.push(`<text x="${padding.left - 10}" y="${y + 4}" text-anchor="end" font-size="11" fill="#74808a">${formatter ? formatter(value) : numberFormat(value, 0)}</text>`);
      }
      const xTicks = points.map((p, i) => {
        if (i % tickEvery !== 0 && i !== points.length - 1) return '';
        const x = xScale(i);
        const label = xLabelField ? p[xLabelField] : (xFormatter ? xFormatter(p[xField]) : p[xField]);
        if (Math.abs(labelRotation) > 0) {
          const anchor = labelRotation < 0 ? 'end' : 'start';
          return `<text transform="translate(${x},${height - 14}) rotate(${labelRotation})" text-anchor="${anchor}" font-size="11" fill="#74808a">${label}</text>`;
        }
        return `<text x="${x}" y="${height - 12}" text-anchor="middle" font-size="11" fill="#74808a">${label}</text>`;
      }).join('');
      const dots = points.map((p, i) => {
        const x = xScale(i);
        const y = yScale(Number(p[yField]));
        const label = xLabelField ? p[xLabelField] : (xFormatter ? xFormatter(p[xField]) : p[xField]);
        const title = `${label}: ${formatter ? formatter(Number(p[yField])) : numberFormat(Number(p[yField]), 0)}`;
        return `<g><circle cx="${x}" cy="${y}" r="4.5" fill="${color}" /><title>${title}</title></g>`;
      }).join('');
      return `<svg class="chart" viewBox="0 0 ${width} ${height}" role="img">${grid.join('')}${xTicks}<polyline points="${polyline}" fill="none" stroke="${color}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"></polyline>${dots}</svg>`;
    }

    function svgBarChart({ items, labelField, valueField, formatter, color = '#0f766e', maxItems = 10 }) {
      const rows = items.filter(row => row[valueField] != null).slice(0, maxItems);
      if (!rows.length) return '<div class="empty">No hay datos para este filtro.</div>';
      const width = 760;
      const barHeight = 24;
      const gap = 14;
      const left = 240;
      const right = 110;
      const top = 24;
      const height = top + rows.length * (barHeight + gap) + 10;
      const maxValue = Math.max(...rows.map(row => Number(row[valueField]) || 0), 1);
      const bars = rows.map((row, idx) => {
        const y = top + idx * (barHeight + gap);
        const label = String(row[labelField] ?? '').slice(0, 44);
        const value = Number(row[valueField]) || 0;
        const barWidth = ((width - left - right) * value) / maxValue;
        return `<text x="${left - 14}" y="${y + 16}" text-anchor="end" font-size="12" fill="#334155">${label}</text>
                <rect x="${left}" y="${y}" width="${barWidth}" height="${barHeight}" rx="10" fill="${color}"></rect>
                <text x="${left + barWidth + 10}" y="${y + 16}" font-size="12" fill="#5b6871">${formatter ? formatter(value) : numberFormat(value, 0)}</text>`;
      }).join('');
      return `<svg class="chart" viewBox="0 0 ${width} ${height}" role="img">${bars}</svg>`;
    }

    function renderTable(tableEl, rows, columns) {
      if (!rows.length) {
        tableEl.innerHTML = '<tbody><tr><td class="empty">No hay datos para este filtro.</td></tr></tbody>';
        return;
      }
      const thead = `<thead><tr>${columns.map(col => `<th>${col.label}</th>`).join('')}</tr></thead>`;
      const tbody = `<tbody>${rows.map(row => `<tr>${columns.map(col => `<td>${col.render ? col.render(row[col.key], row) : (row[col.key] ?? '')}</td>`).join('')}</tr>`).join('')}</tbody>`;
      tableEl.innerHTML = thead + tbody;
    }

    function tabSetup() {
      document.querySelectorAll('.nav-tabs button').forEach(button => {
        button.addEventListener('click', () => {
          document.querySelectorAll('.nav-tabs button').forEach(b => b.classList.remove('active'));
          document.querySelectorAll('.panel').forEach(panel => panel.classList.remove('active'));
          button.classList.add('active');
          document.getElementById(`tab-${button.dataset.tab}`).classList.add('active');
        });
      });
    }

    function buildSourceSelect(selectEl, includeAll = false) {
      const options = unique(DATA.market_period_overview.map(row => sourceKey(row)));
      selectEl.innerHTML = [
        includeAll ? `<option value="ALL">Todas las fuentes</option>` : '',
        ...options.map(key => {
          const [source, survey_variant] = key.split('|');
          return `<option value="${key}">${SOURCE_LABELS[key] || `${source} · ${survey_variant}`}</option>`;
        })
      ].join('');
    }

    function buildYearSelect(selectEl, sourceValue) {
      const rows = DATA.market_period_overview.filter(row => sourceValue === 'ALL' || sourceKey(row) === sourceValue);
      const years = unique(rows.map(row => row.year)).sort((a, b) => a - b);
      selectEl.innerHTML = years.map(year => `<option value="${year}">${year}</option>`).join('');
      if (years.length) selectEl.value = String(years[years.length - 1]);
    }

    function buildChangesYearSelects(sourceValue) {
      const rows = DATA.summary_yearly.filter(row => sourceKey(row) === sourceValue);
      const years = unique(rows.map(row => row.year)).sort((a, b) => a - b);
      const startSelect = document.getElementById('changes-start-year');
      const endSelect = document.getElementById('changes-end-year');
      startSelect.innerHTML = years.map(year => `<option value="${year}">${year}</option>`).join('');
      endSelect.innerHTML = years.map(year => `<option value="${year}">${year}</option>`).join('');
      if (years.length) {
        startSelect.value = String(years[0]);
        endSelect.value = String(years[years.length - 1]);
      }
    }

    function panoramaState() {
      return {
        sourceValue: document.getElementById('panorama-source').value,
        yearValue: Number(document.getElementById('panorama-year').value),
        metric: document.getElementById('panorama-metric').value,
        monthMetric: document.getElementById('panorama-month-metric').value
      };
    }

    function renderPanorama() {
      const state = panoramaState();
      const current = panoramaCurrentRow(state.sourceValue, state.yearValue);
      const previous = panoramaPreviousRow(state.sourceValue, current);
      if (!current) return;

      const cards = [
        {
          kicker: 'Población ocupada',
          value: numberFormat(current.occupied_population, 0),
          delta: `${percentFormat(current.employment_rate_pet)} de la PET · PEA: ${numberFormat(current.pea_population, 0)} · vs. año previo: ${panoramaMetricDelta('occupied_population', current.occupied_population, previous?.occupied_population)}`
        },
        {
          kicker: 'Tasa de desempleo abierto',
          value: percentFormat(current.unemployment_rate_pea),
          delta: `Desocupados abiertos: ${numberFormat(current.unemployed_open_population, 0)} · vs. año previo: ${panoramaMetricDelta('unemployment_rate_pea', current.unemployment_rate_pea, previous?.unemployment_rate_pea)}`
        },
        {
          kicker: 'Tasa de actividad',
          value: percentFormat(current.activity_rate_pet),
          delta: `PET: ${numberFormat(current.pet_population, 0)} · Ocupaciones observadas: ${numberFormat(current.occupation_count, 0)}`
        },
        {
          kicker: 'Ingreso mensual medio',
          value: currencyFormat(current.income_monthly_weighted_mean),
          delta: `Horas: ${numberFormat(current.hours_week_weighted_mean, 1)} · Mujeres: ${percentFormat(current.women_share_weighted)} · Rural: ${percentFormat(current.rural_share_weighted)}`
        }
      ];
      document.getElementById('panorama-cards').innerHTML = cards.map(card => `<article class="card"><div class="kicker">${card.kicker}</div><div class="value">${card.value}</div><div class="delta">${card.delta}</div></article>`).join('');
      document.getElementById('panorama-labor').innerHTML = `
        <h3>Marco INEI del año seleccionado</h3>
        <p class="sub">
          <strong>Tasa de desempleo abierto</strong> = desocupados abiertos / PEA × 100.
          <strong> PEA</strong> = ocupados + desocupados abiertos.
          <strong> PET</strong> = población de 14 años a más.
        </p>
        <div class="formula-grid">
          <div class="formula-stat"><strong>PET</strong><span>${numberFormat(current.pet_population, 0)}</span></div>
          <div class="formula-stat"><strong>PEA</strong><span>${numberFormat(current.pea_population, 0)}</span></div>
          <div class="formula-stat"><strong>Ocupados</strong><span>${numberFormat(current.occupied_population, 0)}</span></div>
          <div class="formula-stat"><strong>Desocupados abiertos</strong><span>${numberFormat(current.unemployed_open_population, 0)}</span></div>
          <div class="formula-stat"><strong>Inactivos</strong><span>${numberFormat(current.inactive_population, 0)}</span></div>
        </div>
        <div class="chart-note">
          ${isLimaMovilSource(state.sourceValue)
            ? `Para ${SOURCE_LABELS[state.sourceValue]}, el año ${state.yearValue} se está leyendo con el corte ${current.period_label}.`
            : `Para ${SOURCE_LABELS[state.sourceValue]}, el año ${state.yearValue} corresponde al corte ${current.period_label}.`}
          La tasa de empleo mostrada en esta vista usa <strong>ocupados / PET</strong>, y la tasa de desempleo abierto usa <strong>desocupados abiertos / PEA</strong>.
        </div>
      `;

      const yearlySorted = panoramaHistoricalSeries(state.sourceValue, current);
      document.getElementById('panorama-line').innerHTML = svgLineChart({
        points: yearlySorted,
        xField: 'year',
        yField: state.metric,
        color: SERIES_COLORS[state.sourceValue] || '#0f766e',
        formatter: PANORAMA_METRIC_FORMATTERS[state.metric],
        xFormatter: value => String(value),
        minZero: true
      });
      document.getElementById('panorama-line-note').textContent = isLimaMovilSource(state.sourceValue)
        ? `${SOURCE_LABELS[state.sourceValue]} · serie anual alineada al corte ${current.period_label} · ${document.getElementById('panorama-metric').selectedOptions[0].textContent}`
        : `${SOURCE_LABELS[state.sourceValue]} · ${document.getElementById('panorama-metric').selectedOptions[0].textContent}`;

      if (isLimaMovilSource(state.sourceValue)) {
        const intraRows = panoramaYearRows(state.sourceValue, state.yearValue).map(row => ({
          ...row,
          chart_order: periodEndMonth(row.period_label),
          chart_label: extractPeriodCore(row.period_label)
        }));
        document.getElementById('panorama-month').innerHTML = svgLineChart({
          points: intraRows,
          xField: 'chart_order',
          yField: state.monthMetric,
          color: '#b45309',
          formatter: PANORAMA_METRIC_FORMATTERS[state.monthMetric],
          xLabelField: 'chart_label',
          minZero: true,
          labelRotation: -45,
          tickEvery: 1
        });
        document.getElementById('panorama-month-note').textContent = intraRows.length
          ? `Serie por trimestre móvil de ${document.getElementById('panorama-month-metric').selectedOptions[0].textContent.toLowerCase()} dentro de ${state.yearValue}.`
          : 'No hay suficiente detalle intraanual en esta fuente para el año elegido.';
      } else {
        const monthly = sortBy(
          DATA.market_month_overview.filter(row => sourceKey(row) === state.sourceValue && Number(row.year) === state.yearValue),
          'month'
        );
        document.getElementById('panorama-month').innerHTML = svgLineChart({
          points: monthly,
          xField: 'month',
          yField: state.monthMetric,
          color: '#b45309',
          formatter: PANORAMA_METRIC_FORMATTERS[state.monthMetric],
          xFormatter: value => `M${value}`,
          minZero: true
        });
        document.getElementById('panorama-month-note').textContent = monthly.length
          ? `Serie mensual de ${document.getElementById('panorama-month-metric').selectedOptions[0].textContent.toLowerCase()} observada en el año seleccionado.`
          : 'No hay suficiente detalle mensual en esta fuente para el año elegido.';
      }

      const topRows = sortBy(
        DATA.summary.filter(row =>
          sourceKey(row) === state.sourceValue &&
          Number(row.year) === state.yearValue &&
          row.period_label === current.period_label
        ),
        'population',
        true
      ).slice(0, 10);
      document.getElementById('top-occupations-chart').innerHTML = svgBarChart({
        items: topRows,
        labelField: 'name',
        valueField: 'population',
        formatter: value => numberFormat(value, 0),
        color: SERIES_COLORS[state.sourceValue] || '#0f766e'
      });

      renderTable(document.getElementById('panorama-table'), topRows, [
        { key: 'code', label: 'Código' },
        { key: 'name', label: 'Ocupación' },
        { key: 'population', label: 'Población', render: value => numberFormat(value, 0) },
        { key: 'population_share', label: '% del total ocupado', render: value => percentFormat(value) },
        { key: 'income_mean', label: 'Ingreso medio', render: value => currencyFormat(value) },
        { key: 'women_share', label: '% mujeres', render: value => percentFormat(value) },
        { key: 'rural_share', label: '% rural', render: value => percentFormat(value) }
      ]);
    }

    function populateOccupationSearch() {
      const dataList = document.getElementById('occupation-options');
      dataList.innerHTML = DATA.lookup.map(row =>
        `<option value="${row.code}">${row.name}</option><option value="${row.name}">${row.code} · ${row.name}</option>`
      ).join('');
    }

    function resolveOccupationInput(raw) {
      const query = (raw || '').trim().toLowerCase();
      if (!query) return [];
      const digits = query.replace(/\D/g, '').slice(0, 4);
      const exactCode = DATA.lookup.filter(row => row.code === digits);
      if (exactCode.length) return exactCode;
      return DATA.lookup.filter(row =>
        row.name?.toLowerCase().includes(query) ||
        row.code?.includes(digits) ||
        row.ciuo_names?.toLowerCase().includes(query)
      ).slice(0, 20);
    }

    function renderOccupationResults(results) {
      const holder = document.getElementById('occupation-results');
      if (!results.length) {
        holder.innerHTML = '<div class="empty">No encontré coincidencias con ese texto.</div>';
        return;
      }
      holder.innerHTML = results.map(row => `<button class="result-btn" data-code="${row.code}"><strong>${row.name}</strong><div style="color:#5b6871; margin-top:4px;">CNO ${row.code}${row.ciuo_names ? ` · CIUO: ${row.ciuo_names}` : ''}</div></button>`).join('');
      holder.querySelectorAll('.result-btn').forEach(btn => btn.addEventListener('click', () => renderOccupation(btn.dataset.code)));
    }

    function renderOccupation(code) {
      const sourceValue = document.getElementById('occupation-source').value;
      const metric = document.getElementById('occupation-metric').value;
      const rows = [...DATA.summary.filter(row => row.code === code && (sourceValue === 'ALL' || sourceKey(row) === sourceValue))]
        .sort((a, b) => seriesSortValue(a) - seriesSortValue(b));
      const meta = DATA.lookup.find(row => row.code === code);
      const content = document.getElementById('occupation-content');
      if (!rows.length) {
        content.innerHTML = '<div class="empty">No hay series para esa ocupación con el filtro seleccionado.</div>';
        return;
      }

      const latest = [...rows].sort((a, b) => seriesSortValue(b) - seriesSortValue(a))[0];
      const chips = [
        `<span class="pill">Código CNO: ${code}</span>`,
        meta?.ciuo_names ? `<span class="pill">CIUO: ${meta.ciuo_names}</span>` : '',
        `<span class="pill">${rows.length} observaciones serie</span>`
      ].join('');

      const cards = [
        { kicker: 'Población más reciente', value: numberFormat(latest.population, 0), delta: `${sourceLabel(latest)} · ${latest.year} · ${percentFormat(latest.population_share)} del ocupado observado` },
        { kicker: 'Ingreso medio más reciente', value: currencyFormat(latest.income_mean), delta: `Horas: ${numberFormat(latest.hours_mean, 1)}` },
        { kicker: '% mujeres', value: percentFormat(latest.women_share), delta: `Edad media: ${numberFormat(latest.age_mean, 1)} años` },
        { kicker: '% rural', value: percentFormat(latest.rural_share), delta: `Casos: ${numberFormat(latest.cases, 0)}` }
      ];

      const groupedKeys = unique(rows.map(row => sourceKey(row)));
      const chartsHtml = groupedKeys.map(key => {
        const series = [...rows.filter(row => sourceKey(row) === key)].sort((a, b) => seriesSortValue(a) - seriesSortValue(b));
        const chartPoints = series.map(row => ({
          ...row,
          chart_order: seriesSortValue(row),
          chart_label: seriesXAxisLabel(row)
        }));
        const rotated = key === 'EPEN|lima_movil';
        const tickStep = rotated ? Math.max(1, Math.ceil(chartPoints.length / 10)) : 1;
        return `<div style="margin-bottom:12px;"><div class="pill" style="background:#eef6f5;color:#0c4f4a;">${SOURCE_LABELS[key] || key}</div><div class="chart-wrap" style="min-height:180px;">${svgLineChart({
          points: chartPoints,
          xField: 'chart_order',
          yField: metric,
          color: SERIES_COLORS[key] || '#0f766e',
          formatter: SUMMARY_METRIC_FORMATTERS[metric],
          xLabelField: 'chart_label',
          minZero: true,
          labelRotation: rotated ? -45 : 0,
          tickEvery: tickStep
        })}</div></div>`;
      }).join('');

      content.innerHTML = `
        <div class="print-only"><h2>${meta?.name || code}</h2></div>
        <div class="card" style="margin-bottom:18px;" data-current-code="${code}">
          <h3 style="margin-bottom:6px;">${meta?.name || code}</h3>
          <div>${chips}</div>
        </div>
        <div class="grid-cards">${cards.map(card => `<article class="card"><div class="kicker">${card.kicker}</div><div class="value">${card.value}</div><div class="delta">${card.delta}</div></article>`).join('')}</div>
        <div class="layout-two">
          <div class="card">
            <h3>Tendencia por fuente</h3>
            <p class="sub">Comparación temporal de ${SUMMARY_METRIC_LABELS[metric].toLowerCase()} para la ocupación seleccionada.</p>
            ${chartsHtml}
          </div>
          <div class="card">
            <h3>Composición más reciente</h3>
            <p class="sub">Ficha rápida para contraste posterior con el índice IA.</p>
            <div style="margin-top:12px; line-height:1.9;">
              <div><strong>Fuente:</strong> ${sourceLabel(latest)}</div>
              <div><strong>Periodo:</strong> ${latest.period_label}</div>
              <div><strong>Año:</strong> ${latest.year}</div>
              <div><strong>Población:</strong> ${numberFormat(latest.population, 0)}</div>
              <div><strong>% del total ocupado observado:</strong> ${percentFormat(latest.population_share)}</div>
              <div><strong>Ingreso medio:</strong> ${currencyFormat(latest.income_mean)}</div>
              <div><strong>Ingreso mediano:</strong> ${currencyFormat(latest.income_median)}</div>
              <div><strong>% mujeres:</strong> ${percentFormat(latest.women_share)}</div>
              <div><strong>% rural:</strong> ${percentFormat(latest.rural_share)}</div>
              <div><strong>Edad media:</strong> ${numberFormat(latest.age_mean, 1)} años</div>
              <div><strong>Horas medias:</strong> ${numberFormat(latest.hours_mean, 1)}</div>
            </div>
          </div>
        </div>
        <div class="card">
          <div class="section-head inline"><div><h3>Serie completa</h3><p class="sub">Tabla lista para exportar como reporte base.</p></div></div>
          <div class="table-wrap"><table id="occupation-series-table"></table></div>
        </div>`;

      renderTable(document.getElementById('occupation-series-table'), rows, [
        { key: 'source', label: 'Fuente', render: (_, row) => sourceLabel(row) },
        { key: 'year', label: 'Año' },
        { key: 'period_label', label: 'Periodo' },
        { key: 'population', label: 'Población', render: value => numberFormat(value, 0) },
        { key: 'population_share', label: '% del total ocupado', render: value => percentFormat(value) },
        { key: 'income_mean', label: 'Ingreso medio', render: value => currencyFormat(value) },
        { key: 'income_median', label: 'Ingreso mediano', render: value => currencyFormat(value) },
        { key: 'women_share', label: '% mujeres', render: value => percentFormat(value) },
        { key: 'rural_share', label: '% rural', render: value => percentFormat(value) }
      ]);

      document.getElementById('export-occupation-csv').onclick = () => {
        downloadText(`ocupacion_${code}_serie.csv`, recordsToCSV(rows), 'text/csv;charset=utf-8');
      };
    }

    function changesState() {
      return {
        sourceValue: document.getElementById('changes-source').value,
        metric: document.getElementById('changes-metric').value,
        startYear: Number(document.getElementById('changes-start-year').value),
        endYear: Number(document.getElementById('changes-end-year').value)
      };
    }

    function computeChangesRows(state) {
      const rows = DATA.summary_yearly.filter(row => sourceKey(row) === state.sourceValue);
      const startMap = new Map(
        rows
          .filter(row => row.year === state.startYear)
          .map(row => [row.code, row])
      );
      const endMap = new Map(
        rows
          .filter(row => row.year === state.endYear)
          .map(row => [row.code, row])
      );
      return [...startMap.keys()]
        .filter(code => endMap.has(code))
        .map(code => {
          const start = startMap.get(code);
          const end = endMap.get(code);
          const startValue = Number(start[state.metric]);
          const endValue = Number(end[state.metric]);
          if (!Number.isFinite(startValue) || !Number.isFinite(endValue)) return null;
          return {
            code,
            name: end.name || start.name || code,
            start_value: startValue,
            end_value: endValue,
            delta_value: endValue - startValue,
            delta_pct: startValue !== 0 ? (endValue - startValue) / startValue : null,
            share_start: start.population_share,
            share_end: end.population_share,
            share_delta: (Number(end.population_share) - Number(start.population_share)),
            population_start: start.population,
            population_end: end.population,
            rural_start: start.rural_share,
            rural_end: end.rural_share,
            women_start: start.women_share,
            women_end: end.women_share
          };
        })
        .filter(Boolean);
    }

    function computeRuralChangeRows(state) {
      const rows = DATA.summary_yearly.filter(row => sourceKey(row) === state.sourceValue);
      const startMap = new Map(rows.filter(row => row.year === state.startYear).map(row => [row.code, row]));
      const endMap = new Map(rows.filter(row => row.year === state.endYear).map(row => [row.code, row]));
      return [...startMap.keys()]
        .filter(code => endMap.has(code))
        .map(code => {
          const start = startMap.get(code);
          const end = endMap.get(code);
          const ruralStart = Number(start.rural_share);
          const ruralEnd = Number(end.rural_share);
          if (!Number.isFinite(ruralStart) || !Number.isFinite(ruralEnd)) return null;
          return {
            code,
            name: end.name || start.name || code,
            rural_start: ruralStart,
            rural_end: ruralEnd,
            rural_delta: ruralEnd - ruralStart,
            urban_start: 1 - ruralStart,
            urban_end: 1 - ruralEnd,
            urban_delta: (1 - ruralEnd) - (1 - ruralStart),
            population_start: start.population,
            population_end: end.population
          };
        })
        .filter(Boolean)
        .sort((a, b) => Math.abs(b.rural_delta) - Math.abs(a.rural_delta));
    }

    function renderChanges() {
      const state = changesState();
      if (!state.sourceValue || !state.startYear || !state.endYear) return;
      const comparable = computeChangesRows(state).sort((a, b) => b.delta_value - a.delta_value);
      const positives = comparable.filter(row => row.delta_value > 0).slice(0, 12);
      const negatives = comparable.filter(row => row.delta_value < 0).sort((a, b) => a.delta_value - b.delta_value).slice(0, 12);
      const metricLabel = SUMMARY_METRIC_LABELS[state.metric].toLowerCase();

      if (state.endYear < state.startYear) {
        document.getElementById('changes-cards').innerHTML = '<div class="empty">El año final debe ser mayor o igual que el inicial.</div>';
        document.getElementById('changes-up-chart').innerHTML = '<div class="empty">Ajusta el rango de años.</div>';
        document.getElementById('changes-down-chart').innerHTML = '<div class="empty">Ajusta el rango de años.</div>';
        renderTable(document.getElementById('changes-table'), [], []);
        renderTable(document.getElementById('changes-rural-table'), [], []);
        return;
      }

      const topUp = positives[0];
      const topDown = negatives[0];
      const cards = [
        { kicker: 'Fuente comparada', value: SOURCE_LABELS[state.sourceValue] || state.sourceValue, delta: `${state.startYear} vs. ${state.endYear}` },
        { kicker: 'Ocupaciones comparables', value: numberFormat(comparable.length, 0), delta: `Indicador: ${SUMMARY_METRIC_LABELS[state.metric]}` },
        { kicker: 'Mayor aumento', value: topUp ? topUp.name : 'Sin dato', delta: topUp ? formatMetricDelta(state.metric, topUp.delta_value) : 'Sin dato' },
        { kicker: 'Mayor caída', value: topDown ? topDown.name : 'Sin dato', delta: topDown ? formatMetricDelta(state.metric, topDown.delta_value) : 'Sin dato' }
      ];
      document.getElementById('changes-cards').innerHTML = cards.map(card => `<article class="card"><div class="kicker">${card.kicker}</div><div class="value value-small">${card.value}</div><div class="delta">${card.delta}</div></article>`).join('');

      document.getElementById('changes-up-chart').innerHTML = svgBarChart({
        items: positives.map(row => ({ ...row, delta_abs: row.delta_value })),
        labelField: 'name',
        valueField: 'delta_abs',
        formatter: value => formatMetricDelta(state.metric, value),
        color: '#0f766e',
        maxItems: 12
      });

      document.getElementById('changes-down-chart').innerHTML = svgBarChart({
        items: negatives.map(row => ({ ...row, delta_abs: Math.abs(row.delta_value) })),
        labelField: 'name',
        valueField: 'delta_abs',
        formatter: value => formatMetricDelta(state.metric, -value),
        color: '#b45309',
        maxItems: 12
      });

      const changeColumns = [
        { key: 'code', label: 'Código' },
        { key: 'name', label: 'Ocupación' },
        { key: 'start_value', label: `${state.startYear}`, render: value => formatMetricValue(state.metric, value) },
        { key: 'end_value', label: `${state.endYear}`, render: value => formatMetricValue(state.metric, value) },
        { key: 'delta_value', label: 'Cambio', render: value => formatMetricDelta(state.metric, value) },
        { key: 'delta_pct', label: '% cambio', render: value => state.metric === 'women_share' || state.metric === 'rural_share' ? 'No aplica' : percentFormat(value) }
      ];
      if (state.metric === 'population') {
        changeColumns.splice(5, 0,
          { key: 'share_start', label: `% ocupado ${state.startYear}`, render: value => percentFormat(value) },
          { key: 'share_end', label: `% ocupado ${state.endYear}`, render: value => percentFormat(value) },
          { key: 'share_delta', label: 'Cambio en participación', render: value => formatMetricDelta('rural_share', value) }
        );
      }
      renderTable(
        document.getElementById('changes-table'),
        comparable.sort((a, b) => Math.abs(b.delta_value) - Math.abs(a.delta_value)).slice(0, 40),
        changeColumns
      );

      const ruralRows = computeRuralChangeRows(state);
      const ruralNote = document.getElementById('changes-rural-note');
      if (!ruralRows.length) {
        ruralNote.textContent = 'Esta fuente no tiene ruralidad disponible o no hay datos comparables para ese rango.';
        renderTable(document.getElementById('changes-rural-table'), [], [
          { key: 'code', label: 'Código' }
        ]);
      } else {
        ruralNote.textContent = `Cambio de composición rural/urbana entre ${state.startYear} y ${state.endYear} para ${SOURCE_LABELS[state.sourceValue] || state.sourceValue}.`;
        renderTable(document.getElementById('changes-rural-table'), ruralRows.slice(0, 30), [
          { key: 'code', label: 'Código' },
          { key: 'name', label: 'Ocupación' },
          { key: 'rural_start', label: `% rural ${state.startYear}`, render: value => percentFormat(value) },
          { key: 'rural_end', label: `% rural ${state.endYear}`, render: value => percentFormat(value) },
          { key: 'rural_delta', label: 'Cambio rural', render: value => formatMetricDelta('rural_share', value) },
          { key: 'urban_start', label: `% urbano ${state.startYear}`, render: value => percentFormat(value) },
          { key: 'urban_end', label: `% urbano ${state.endYear}`, render: value => percentFormat(value) }
        ]);
      }

      document.getElementById('export-changes-csv').onclick = () => {
        const exportRows = comparable.map(row => ({
          source: SOURCE_LABELS[state.sourceValue] || state.sourceValue,
          metric: SUMMARY_METRIC_LABELS[state.metric],
          start_year: state.startYear,
          end_year: state.endYear,
          code: row.code,
          occupation: row.name,
          start_value: row.start_value,
          end_value: row.end_value,
          delta_value: row.delta_value,
          delta_pct: row.delta_pct
        }));
        downloadText(`cambios_${state.sourceValue.replace('|', '_')}_${state.metric}_${state.startYear}_${state.endYear}.csv`, recordsToCSV(exportRows), 'text/csv;charset=utf-8');
      };
    }

    function renderSources() {
      document.getElementById('source-cards').innerHTML = DATA.source_cards.map(card => `<article class="source-card"><div class="source-title">${SOURCE_LABELS[`${card.source}|${card.survey_variant}`] || `${card.source} · ${card.survey_variant}`}</div><div class="sub" style="margin-bottom:10px;">Cobertura procesada lista para visualización.</div><div><strong>Años:</strong> ${card.year_min}–${card.year_max}</div><div><strong>Períodos:</strong> ${card.periods}</div><div><strong>Filas inventariadas:</strong> ${numberFormat(card.rows_sum, 0)}</div><div><strong>Máx. códigos ocupacionales por corte:</strong> ${numberFormat(card.occupation_codes_max, 0)}</div></article>`).join('');

      renderTable(document.getElementById('availability-table'), sortBy(DATA.availability, 'year'), [
        { key: 'source', label: 'Fuente', render: (_, row) => sourceLabel(row) },
        { key: 'year', label: 'Año' },
        { key: 'period_label', label: 'Periodo' },
        { key: 'rows', label: 'Filas', render: value => numberFormat(value, 0) },
        { key: 'valid_occupation_rows', label: 'Filas con código', render: value => numberFormat(value, 0) },
        { key: 'occupation_codes', label: 'N códigos', render: value => numberFormat(value, 0) }
      ]);
    }

    function renderMethodology() {
      document.getElementById('generated-at').textContent = DATA.generated_at;
      document.getElementById('source-pills').innerHTML = DATA.source_cards.map(card => `<span class="pill">${SOURCE_LABELS[`${card.source}|${card.survey_variant}`]}</span>`).join('');
      document.getElementById('file-pills').innerHTML = FILES.map(file => `<span class="pill">${file}</span>`).join('');
    }

    function setupEvents() {
      const panoramaSource = document.getElementById('panorama-source');
      const panoramaYear = document.getElementById('panorama-year');
      const panoramaMetric = document.getElementById('panorama-metric');
      const panoramaMonthMetric = document.getElementById('panorama-month-metric');

      panoramaSource.addEventListener('change', () => {
        buildYearSelect(panoramaYear, panoramaSource.value);
        renderPanorama();
      });
      panoramaYear.addEventListener('change', renderPanorama);
      panoramaMetric.addEventListener('change', renderPanorama);
      panoramaMonthMetric.addEventListener('change', renderPanorama);

      document.getElementById('export-panorama-csv').addEventListener('click', () => {
        const state = panoramaState();
        const current = panoramaCurrentRow(state.sourceValue, state.yearValue);
        if (!current) return;
        const rows = sortBy(
          DATA.summary.filter(row =>
            sourceKey(row) === state.sourceValue &&
            Number(row.year) === state.yearValue &&
            row.period_label === current.period_label
          ),
          'population',
          true
        );
        downloadText(`panorama_${state.sourceValue.replace('|', '_')}_${state.yearValue}.csv`, recordsToCSV(rows), 'text/csv;charset=utf-8');
      });
      document.getElementById('print-panorama').addEventListener('click', () => window.print());

      const occupationSource = document.getElementById('occupation-source');
      const occupationMetric = document.getElementById('occupation-metric');
      buildSourceSelect(occupationSource, true);
      const runSearch = () => {
        const input = document.getElementById('occupation-search').value;
        const results = resolveOccupationInput(input);
        renderOccupationResults(results);
        if (results.length === 1) renderOccupation(results[0].code);
      };
      document.getElementById('run-occupation-search').addEventListener('click', runSearch);
      document.getElementById('occupation-search').addEventListener('keydown', event => {
        if (event.key === 'Enter') runSearch();
      });
      document.getElementById('print-occupation').addEventListener('click', () => window.print());
      occupationSource.addEventListener('change', () => {
        const current = document.querySelector('[data-current-code]');
        if (current) renderOccupation(current.dataset.currentCode);
      });
      occupationMetric.addEventListener('change', () => {
        const current = document.querySelector('[data-current-code]');
        if (current) renderOccupation(current.dataset.currentCode);
      });

      const changesSource = document.getElementById('changes-source');
      const changesMetric = document.getElementById('changes-metric');
      const changesStartYear = document.getElementById('changes-start-year');
      const changesEndYear = document.getElementById('changes-end-year');

      changesSource.addEventListener('change', () => {
        buildChangesYearSelects(changesSource.value);
        renderChanges();
      });
      changesMetric.addEventListener('change', renderChanges);
      changesStartYear.addEventListener('change', renderChanges);
      changesEndYear.addEventListener('change', renderChanges);
      document.getElementById('print-changes').addEventListener('click', () => window.print());
    }

    function init() {
      tabSetup();
      buildSourceSelect(document.getElementById('panorama-source'));
      buildYearSelect(document.getElementById('panorama-year'), document.getElementById('panorama-source').value);
      buildSourceSelect(document.getElementById('occupation-source'), true);
      buildSourceSelect(document.getElementById('changes-source'));
      buildChangesYearSelects(document.getElementById('changes-source').value);
      populateOccupationSearch();
      renderPanorama();
      renderSources();
      renderChanges();
      renderMethodology();
      setupEvents();

      const defaultOccupation = DATA.lookup.find(row => row.code === '2631') || DATA.lookup[0];
      if (defaultOccupation) {
        document.getElementById('occupation-search').value = defaultOccupation.code;
        renderOccupation(defaultOccupation.code);
      }
    }

    init();
