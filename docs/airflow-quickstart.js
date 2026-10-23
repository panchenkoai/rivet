// Command builder for airflow-quickstart.md: renders a form into #af-builder and substitutes {{TOKEN}} placeholders in
// every code block on the page. Runs only on the mdBook site (GitHub strips <script>). Nothing leaves the browser and
// nothing is stored; no secret is asked for: the generated commands read passwords with `read -rs` at run time.
// Under node, `require('./airflow-quickstart.js').tokens(values)` returns the substitutions for a set of form values.
(() => {
  const PORT = { mysql: '3306', postgres: '5432' };
  const CONN_TYPE = { mysql: 'mysql', postgres: 'postgres' };

  // [field, label, options[] | placeholder | (g) => placeholder, show-when (every key must match)]
  const SPEC = [
    ['Airflow', [
      ['version', 'Version', ['3.3', '2.10']],
      ['svc', 'Service that runs tasks', ['airflow-scheduler', 'airflow-worker']],
    ]],
    ['Pipeline', [
      ['name', 'Name', 'shop_cdc'],
      ['mode', 'Mode', ['cdc', 'batch']],
      ['tables', 'Tables (space-separated)', 'orders customers'],
      ['pk', 'Primary key column', 'id', { mode: ['batch'] }],
    ]],
    ['Source', [
      ['engine', 'Engine', ['mysql', 'postgres']],
      ['conn', 'Connection id', (g) => 'rivet_' + g('db')],
      ['host', 'Host', 'db.internal'],
      ['port', 'Port', (g) => PORT[g('engine')]],
      ['db', 'Database', 'shop'],
      ['user', 'User', 'rivet'],
      ['tls', 'TLS', ['disable', 'require', 'verify-full']],
    ]],
    ['Bucket', [
      ['storage', 'Where', ['minio', 's3']],
      ['bucket', 'Bucket', (g) => g('name').replace(/_/g, '-')],
      ['region', 'Region', 'us-east-1'],
      ['storage_conn', 'Connection id', (g) => (g('storage') === 'minio' ? 'rivet_s3' : 'rivet_s3_aws')],
    ]],
    ['ClickHouse', [
      ['warehouse', 'Where', ['stack', 'own']],
      ['ch_url', 'HTTP URL', (g) => (g('warehouse') === 'stack' ? 'http://clickhouse:8123' : 'https://clickhouse.internal:8443'),
        { warehouse: ['own'] }],
      ['ch_db', 'Database', (g) => g('name')],
      ['ch_user', 'User', 'loader', { warehouse: ['own'] }],
      ['wh_conn', 'Connection id', (g) => (g('warehouse') === 'stack' ? 'rivet_clickhouse' : 'rivet_clickhouse_own'),
        { warehouse: ['own'] }],
    ]],
  ];

  // A JSON value inside a single-quoted `bash -c '…'`: double-quoted for the inner shell.
  const inner = (o) => `"${JSON.stringify(o).replace(/"/g, '\\"')}"`;

  function tokens(g) {
    const P = g('name'), svc = g('svc'), exec = `$DC exec -T ${svc}`, cdc = g('mode') === 'cdc';
    const minio = g('storage') === 'minio', chStack = g('warehouse') === 'stack';
    const chUrl = chStack ? 'http://clickhouse:8123' : g('ch_url'), chUser = chStack ? 'loader' : g('ch_user');
    const whConn = chStack ? 'rivet_clickhouse' : g('wh_conn'), stConn = g('storage_conn');
    const tables = g('tables').split(/\s+/).filter(Boolean);
    const ch = new URL(chUrl);
    const chPort = ch.port || (ch.protocol === 'https:' ? '443' : '80');

    const conn = [
      "read -rsp 'Source DB password: ' DB_PASSWORD && echo && export DB_PASSWORD",
      `${exec.replace(' -T ', ' -T -e DB_PASSWORD ')} bash -c 'airflow connections add ${g('conn')} --conn-type ${CONN_TYPE[g('engine')]} \\`,
      `  --conn-host ${g('host')} --conn-port ${g('port')} --conn-schema ${g('db')} --conn-login ${g('user')} --conn-password "$DB_PASSWORD"'`,
    ].join('\n');

    const state = [
      `$DC exec -T rivet-state createdb -U rivet rivet_state_${P}`,
      `${exec} airflow connections add rivet_state_${P} --conn-type postgres --conn-host rivet-state --conn-port 5432 \\`,
      `  --conn-schema rivet_state_${P} --conn-login rivet --conn-password rivet_state_demo \\`,
      `  --conn-extra '{"rivet_params": "sslmode=disable"}'`,
      `${exec} mkdir -p /opt/airflow/rivet-state/${P}`,
    ].join('\n');

    const bucket = minio
      ? `$DC exec -T -e MC_HOST_q=http://rivet:rivet_minio_demo@localhost:9000 minio mc mb --ignore-existing q/${g('bucket')}`
      : [
        `aws s3 mb s3://${g('bucket')} --region ${g('region')}      # skip if the bucket exists`,
        "read -rp 'AWS access key id: ' AWS_KEY && read -rsp 'AWS secret access key: ' AWS_SECRET && echo && export AWS_KEY AWS_SECRET",
        `${exec.replace(' -T ', ' -T -e AWS_KEY -e AWS_SECRET ')} bash -c 'airflow connections add ${stConn} --conn-type aws \\`,
        `  --conn-login "$AWS_KEY" --conn-password "$AWS_SECRET" --conn-extra ${inner({ region_name: g('region') })}'`,
      ].join('\n');

    const warehouse = chStack
      ? `$DC exec -T clickhouse clickhouse-client --user loader --password rivet_clickhouse_demo -q "CREATE DATABASE IF NOT EXISTS ${g('ch_db')}"`
      : [
        `# As a ClickHouse admin, once: CREATE DATABASE IF NOT EXISTS ${g('ch_db')};`,
        `#   GRANT SELECT, INSERT, ALTER ADD COLUMN, CREATE TABLE, DROP TABLE, CREATE VIEW, DROP VIEW ON ${g('ch_db')}.* TO ${chUser};`,
        "read -rsp 'ClickHouse password: ' CH_PASSWORD && echo && export CH_PASSWORD",
        `${exec.replace(' -T ', ' -T -e CH_PASSWORD ')} bash -c 'airflow connections add ${whConn} --conn-type clickhouse \\`,
        `  --conn-host ${ch.hostname} --conn-port ${chPort} --conn-login ${chUser} --conn-password "$CH_PASSWORD" \\`,
        // Without a ClickHouse provider Airflow stores the type as `generic`; `rivet_kind` names it for the operator.
        `  --conn-extra ${inner({ rivet_kind: 'clickhouse', ...(ch.protocol === 'https:' ? { secure: true } : {}) })}'`,
      ].join('\n');

    const init = [
      `${exec} bash -c '`,
      `  export RIVET_SOURCE_URL="$(python -W ignore -c "from airflow.models import Connection; from airflow_provider_rivet.connections import connection_url; print(connection_url(Connection.get_connection_from_secrets(\\"${g('conn')}\\")))" | tail -n 1)"`,
      `  export RIVET_STATE_URL="postgresql://rivet:rivet_state_demo@rivet-state:5432/rivet_state_${P}?sslmode=disable"`,
      `  cd /opt/airflow/dags/rivet && rivet init --source-env RIVET_SOURCE_URL --tls ${g('tls')} --mode ${cdc ? 'cdc' : 'incremental'} \\`,
      `    --include ${tables.join(' ')} --s3-bucket ${g('bucket')} --s3-region ${g('region')} \\`,
      `    --clickhouse-url ${chUrl} --clickhouse-database ${g('ch_db')} --clickhouse-user ${chUser} -o ${P}.yaml'`,
      ...(cdc ? [] : [
        '# A batch export reads a query: declare its key, so a load never depends on a key only this state holds (rivet#679)',
        `${exec} sed -i 's/^  pk: auto .*/  pk: [${g('pk')}]/' /opt/airflow/dags/rivet/${P}.yaml`,
      ]),
    ].join('\n');

    const extra = [
      ...(minio ? [] : [`"storage": "${stConn}"`]),
      ...(chStack ? [] : [`"warehouse": "${whConn}"`]),
    ];
    const dagLine = `    "${P}": {"source": "${g('conn')}"${extra.map((e) => ', ' + e).join('')}},`;

    const unpause = [
      `${exec} airflow dags list-import-errors        # expect: No data found`,
      `${exec} airflow dags unpause rivet_${P}        # repeat until the next line shows is_paused False`,
      `${exec} airflow dags list | grep rivet_${P}`,
    ].join('\n');

    const query = (t) => `SELECT count() FROM ${g('ch_db')}.${t}${cdc ? ' WHERE NOT __is_deleted' : ''}`;
    const check = chStack
      ? tables.map((t) => `$DC exec -T clickhouse clickhouse-client --user loader --password rivet_clickhouse_demo -q "${query(t)}"`).join('\n')
      : tables.map((t) => `curl -sS -u "${chUser}:$CH_PASSWORD" '${chUrl}/' --data-binary "${query(t)}"`).join('\n');

    const grants = g('engine') === 'mysql'
      ? [
        `-- On ${g('host')}, as an administrator`,
        `GRANT SELECT ON \`${g('db')}\`.* TO '${g('user')}'@'%';`,
        ...(cdc ? [
          `GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '${g('user')}'@'%';`,
          '-- and on the server: binlog_format=ROW, binlog_row_image=FULL, binlog_row_metadata=FULL',
        ] : []),
      ].join('\n')
      : [
        `-- On ${g('host')}, as an administrator, in database ${g('db')}`,
        `GRANT SELECT ON ${tables.map((t) => (t.includes('.') ? t : `public.${t}`)).join(', ')} TO ${g('user')};`,
        ...(cdc ? [
          `ALTER ROLE ${g('user')} WITH REPLICATION;`,
          '-- and on the server (restart): wal_level = logical',
        ] : []),
      ].join('\n');

    return {
      FOLDER: `airflow-${g('version')}`, SVC: svc, P, DAG: `rivet_${P}`,
      GRANTS: grants, CONN: conn, STATE: state, BUCKET: bucket, WAREHOUSE: warehouse, INIT: init,
      DAG_LINE: dagLine, UNPAUSE: unpause, CHECK: check,
    };
  }

  if (typeof module !== 'undefined' && module.exports) {
    const defaults = (values) => {
      const g = (n) => {
        if (values[n] !== undefined && values[n] !== '') return values[n];
        for (const [, fields] of SPEC) {
          for (const [f, , d] of fields) if (f === n) return Array.isArray(d) ? d[0] : (typeof d === 'function' ? d(g) : d);
        }
        return '';
      };
      return g;
    };
    module.exports = { tokens: (values) => tokens(defaults(values)) };
    return;
  }

  const root = document.getElementById('af-builder');
  if (!root) return;
  const CSS = `
    .rb{display:grid;gap:.75rem;margin:1rem 0 2rem}
    .rb fieldset{margin:0;padding:.4rem .9rem .9rem;border:1px solid var(--table-border-color,#ccc);border-radius:6px;
      display:grid;grid-template-columns:repeat(auto-fill,minmax(10.5rem,1fr));gap:.5rem .75rem}
    .rb legend{font-weight:600;padding:0 .3rem}
    .rb label{display:flex;flex-direction:column;gap:.2rem;font-size:.85em;min-width:0}
    .rb label[hidden]{display:none}
    .rb label span{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .rb input,.rb select{font:inherit;box-sizing:border-box;height:2em;padding:0 .4rem;min-width:0;border-radius:4px;
      border:1px solid var(--table-border-color,#ccc);background:var(--bg,#fff);color:var(--fg,#000)}
    .rb p{font-size:.85em;margin:0;opacity:.85}`;
  const control = (n, d) => Array.isArray(d)
    ? `<select name="${n}">${d.map((o) => `<option>${o}</option>`).join('')}</select>`
    : `<input name="${n}" spellcheck="false">`;
  root.innerHTML = `<style>${CSS}</style><form class="rb" autocomplete="off">` +
    SPEC.map(([legend, fields]) => `<fieldset><legend>${legend}</legend>` +
      fields.map(([n, label, d, show]) =>
        `<label${show ? ` data-show='${JSON.stringify(show)}'` : ''}><span title="${label}">${label}</span>${control(n, d)}</label>`).join('') +
      '</fieldset>').join('') +
    '<p>Empty fields use the grey default. Values stay in this browser tab. No secret is entered here: ' +
    'the commands read passwords with <code>read -rs</code>.</p></form>';

  const form = root.querySelector('form');
  const fields = SPEC.flatMap(([, f]) => f);
  const g = (n) => form.elements[n].value.trim() || form.elements[n].placeholder || '';
  const blocks = [...document.querySelectorAll('main code')].filter((c) => c.textContent.includes('{{'));
  const originals = blocks.map((c) => c.textContent);

  function render() {
    for (let pass = 0; pass < 2; pass++) {  // a default may read another field's default (Connection id <- Database)
      for (const [n, , d] of fields) {
        if (!Array.isArray(d)) form.elements[n].placeholder = typeof d === 'function' ? d(g) : d;
      }
    }
    for (const label of form.querySelectorAll('label[data-show]')) {
      label.hidden = !Object.entries(JSON.parse(label.dataset.show)).every(([k, vs]) => vs.includes(g(k)));
    }
    const v = tokens(g);
    blocks.forEach((c, i) => {
      c.textContent = originals[i].replace(/\{\{([A-Z_]+)\}\}/g, (m, k) => (k in v ? v[k] : m));
      if (window.hljs && c.parentElement.tagName === 'PRE') {
        delete c.dataset.highlighted;
        try { (hljs.highlightElement || hljs.highlightBlock).call(hljs, c); } catch (_) { /* plain text is fine */ }
      }
    });
  }

  form.addEventListener('input', render);
  render();
})();
