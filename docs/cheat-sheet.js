// Command builder for cheat-sheet.md: renders a form into #rivet-builder and
// substitutes {{TOKEN}} placeholders in every code block on the page.
// Runs only on the mdBook site (GitHub strips <script>). Nothing leaves the
// browser and nothing is stored; secrets are never asked for — the generated
// commands read them with `read -rs` at run time.
(() => {
  const root = document.getElementById('rivet-builder');
  if (!root) return;

  const PORT = { postgres: '5432', mysql: '3306', mssql: '1433', mongo: '27017' };
  const SCHEME = { postgres: 'postgresql', mysql: 'mysql', mssql: 'sqlserver', mongo: 'mongodb' };
  const name = (g) => g('table').split('.').pop().replace(/\W/g, '_');

  // [field, label, options[] | placeholder | (g) => placeholder, show-when (every key must match)]
  const SPEC = [
    ['Source', [
      ['engine', 'Engine', ['postgres', 'mysql', 'mssql', 'mongo']],
      ['host', 'Host', 'db.internal'],
      ['port', 'Port', (g) => PORT[g('engine')]],
      ['db', 'Database', 'shop'],
      ['user', 'User', 'rivet'],
      ['tls', 'TLS', ['verify-full', 'verify-ca', 'require', 'disable']],
      ['schema', 'Schema', (g) => ({ postgres: 'public', mssql: 'dbo' })[g('engine')] || g('db'),
        { engine: ['postgres', 'mysql', 'mssql'] }],
      ['table', 'Table', 'orders'],
      ['pk', 'Primary key', 'id'],
      ['cursor', 'Cursor column', 'updated_at'],
    ]],
    ['Destination', [
      ['dest', 'Type', ['gcs', 's3', 'azure', 'local']],
      ['bucket', 'Bucket', 'my-bucket', { dest: ['gcs', 's3', 'azure'] }],
      ['gcp_auth', 'GCP auth', ['adc', 'service-account'], { dest: ['gcs'] }],
      ['project', 'GCP project', 'my-gcp-project', { dest: ['gcs'] }],
      ['location', 'Location', 'US', { dest: ['gcs'] }],
      ['aws_auth', 'AWS auth', ['sso', 'access-keys', 'profile'], { dest: ['s3'] }],
      ['aws_profile', 'AWS profile', 'default', { dest: ['s3'], aws_auth: ['sso', 'profile'] }],
      ['region', 'Region', 'us-east-1', { dest: ['s3'] }],
      ['account', 'Storage account', 'mystorageacct', { dest: ['azure'] }],
      ['path', 'Path', './output', { dest: ['local'] }],
    ]],
    ['CDC', [
      ['slot', 'Slot', (g) => 'rivet_' + name(g), { engine: ['postgres'] }],
      ['server_id', 'server_id', '4271', { engine: ['mysql'] }],
      ['capture', 'Capture instance', (g) => g('schema') + '_' + name(g), { engine: ['mssql'] }],
      ['ckpt', 'Checkpoint dir', '/var/lib/rivet'],
    ]],
    ['Load', [
      ['load', 'Target', ['bigquery', 'snowflake']],
      ['dataset', 'Dataset', 'analytics', { load: ['bigquery'] }],
      ['sf_conn', 'snow connection', 'my_conn', { load: ['snowflake'] }],
      ['sf_wh', 'Warehouse', 'COMPUTE_WH', { load: ['snowflake'] }],
      ['sf_db', 'Database', 'ANALYTICS', { load: ['snowflake'] }],
      ['sf_schema', 'Schema', 'PUBLIC', { load: ['snowflake'] }],
      ['sf_int', 'Integration', 'MY_GCS_INT', { load: ['snowflake'] }],
      ['sf_role', 'Role', 'RIVET_ROLE', { load: ['snowflake'] }],
    ]],
  ];

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
    'the commands read passwords and keys with <code>read -rs</code>. If the DB password contains ' +
    '<code>@ : / ? # %</code>, type it percent-encoded.</p>' +
    '</form>';

  const form = root.querySelector('form');
  const fields = SPEC.flatMap(([, f]) => f);
  const g = (n) => form.elements[n].value.trim() || form.elements[n].placeholder || '';

  const GCLOUD = '# gcloud CLI: https://cloud.google.com/sdk/docs/install  (macOS: brew install --cask google-cloud-sdk)';
  const AWSCLI = '# AWS CLI v2: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html  (macOS: brew install awscli)';

  function cloudSetup(t) {
    const { bucket, p, member, sa } = t;
    const policy = JSON.stringify({
      Version: '2012-10-17',
      Statement: [
        { Sid: 'RivetObjects', Effect: 'Allow', Action: ['s3:PutObject', 's3:GetObject', 's3:DeleteObject'],
          Resource: [`arn:aws:s3:::${bucket}/exports/*`, `arn:aws:s3:::${bucket}/cdc/*`] },
        { Sid: 'RivetList', Effect: 'Allow', Action: ['s3:ListBucket'], Resource: `arn:aws:s3:::${bucket}`,
          Condition: { StringLike: { 's3:prefix': ['exports/*', 'cdc/*'] } } },
        { Sid: 'RivetBucketLocation', Effect: 'Allow', Action: ['s3:GetBucketLocation'], Resource: `arn:aws:s3:::${bucket}` },
      ],
    }, null, 2);
    const policyFile = ["cat > rivet-s3-policy.json <<'EOF'", policy, 'EOF'];
    const prof = g('aws_profile'), region = g('region'), acct = g('account');
    const gcs = [
      GCLOUD,
      'gcloud auth login',
      `gcloud config set project ${p}`,
      ...(g('gcp_auth') === 'adc'
        ? ['gcloud auth application-default login          # rivet uses these Application Default Credentials']
        : ['',
          '# Service account + key file (if your org blocks key creation, use GCP auth: adc)',
          'gcloud iam service-accounts create rivet --display-name=rivet',
          `gcloud iam service-accounts keys create "$HOME/.config/rivet-sa.json" --iam-account=${sa}`,
          'export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.config/rivet-sa.json"']),
      '',
      '# Bucket (skip if it exists) + object read/write/list/delete for rivet',
      `gcloud storage buckets create gs://${bucket} --location=${g('location')}`,
      `gcloud storage buckets add-iam-policy-binding gs://${bucket} \\`,
      `  --member="${member}" --role=roles/storage.objectAdmin`,
    ];
    const s3 = {
      sso: [
        AWSCLI,
        `aws configure sso --profile ${prof}      # one-time; or the AWS login flow your org uses`,
        `aws sso login --profile ${prof}`,
        `eval "$(aws configure export-credentials --profile ${prof} --format env)"   # temporary keys: re-run when they expire`,
        'aws sts get-caller-identity',
        '',
        '# Bucket (skip if it exists)',
        `aws s3 mb s3://${bucket} --region ${region}`,
        '',
        '# Permissions: attach this policy to the role you log in with (usually done by an AWS admin)',
        ...policyFile,
      ],
      'access-keys': [
        AWSCLI,
        '# As an AWS admin, once: bucket, IAM user, policy, access key',
        `aws s3 mb s3://${bucket} --region ${region}`,
        'aws iam create-user --user-name rivet',
        ...policyFile,
        'aws iam put-user-policy --user-name rivet --policy-name rivet-s3 --policy-document file://rivet-s3-policy.json',
        'aws iam create-access-key --user-name rivet       # prints AccessKeyId + SecretAccessKey',
        '',
        '# Where rivet runs: read the keys without putting them in shell history',
        "printf 'AWS access key id: '; read -r RIVET_AWS_ACCESS_KEY; export RIVET_AWS_ACCESS_KEY",
        "printf 'AWS secret access key: '; read -rs RIVET_AWS_SECRET_KEY; echo; export RIVET_AWS_SECRET_KEY",
      ],
      profile: [
        AWSCLI,
        `aws configure --profile ${prof}      # static access key → ~/.aws/credentials (SSO/login profiles do not work here)`,
        `aws sts get-caller-identity --profile ${prof}`,
        '',
        '# Bucket (skip if it exists)',
        `aws s3 mb s3://${bucket} --region ${region} --profile ${prof}`,
        '',
        '# Permissions for that IAM user (as an AWS admin)',
        ...policyFile,
        'aws iam put-user-policy --user-name <iam-user> --policy-name rivet-s3 --policy-document file://rivet-s3-policy.json',
      ],
    }[g('aws_auth')];
    const azure = [
      '# Azure CLI: https://learn.microsoft.com/cli/azure/install-azure-cli  (macOS: brew install azure-cli)',
      'az login',
      '# The storage account must exist: az storage account create -n <name> -g <resource-group> -l <region>',
      `export RIVET_AZURE_KEY="$(az storage account keys list --account-name ${acct} --query '[0].value' -o tsv)"`,
      `az storage container create --name ${bucket} --account-name ${acct} --account-key "$RIVET_AZURE_KEY"`,
    ];
    const local = [`mkdir -p ${g('path')}      # rivet writes as the current OS user; nothing else to set up`];
    return ({ gcs, s3, azure, local })[g('dest')].join('\n');
  }

  function loadSetup(t) {
    const { bucket, p, member } = t;
    const warn = g('dest') === 'gcs' ? [] : ['# ⚠ rivet load reads GCS only: set Destination type to gcs', ''];
    const conn = g('sf_conn');
    const lines = g('load') === 'bigquery'
      ? [
        ...warn,
        '# rivet calls the BigQuery API with the same Google credentials as the GCS destination',
        `gcloud services enable bigquery.googleapis.com --project=${p}`,
        `bq --location=${g('location')} mk --dataset ${p}:${g('dataset')}   # rivet does not create datasets; same location as the bucket`,
        '# Grants (skip if you are project Owner)',
        `gcloud projects add-iam-policy-binding ${p} --member="${member}" --role=roles/bigquery.jobUser`,
        `gcloud projects add-iam-policy-binding ${p} --member="${member}" --role=roles/bigquery.dataOwner`,
      ]
      : [
        ...warn,
        '# Snowflake CLI: rivet runs `snow sql` for every load',
        'pipx install snowflake-cli          # or: pip install snowflake-cli',
        `snow connection add                  # name it ${conn}; key-pair auth recommended`,
        `snow connection test -c ${conn}`,
        '# export RIVET_SNOWFLAKE_KEY=/absolute/path/rsa_key.p8   # only if the private_key_path in the connection uses ~',
        '',
        '# After the SQL below: let the integration\'s service account read the bucket',
        `gcloud storage buckets add-iam-policy-binding gs://${bucket} \\`,
        '  --member="serviceAccount:<STORAGE_GCP_SERVICE_ACCOUNT from DESC>" --role=roles/storage.objectViewer',
      ];
    return lines.join('\n');
  }

  function loadSql(t) {
    if (g('load') === 'bigquery') return '-- BigQuery needs no SQL setup: the dataset and grants are in the bash block above.';
    const si = g('sf_int'), db = g('sf_db'), sc = `${g('sf_db')}.${g('sf_schema')}`, role = g('sf_role');
    return [
      '-- Once, as ACCOUNTADMIN',
      'USE ROLE ACCOUNTADMIN;',
      `CREATE STORAGE INTEGRATION IF NOT EXISTS ${si}`,
      "  TYPE = EXTERNAL_STAGE STORAGE_PROVIDER = 'GCS' ENABLED = TRUE",
      `  STORAGE_ALLOWED_LOCATIONS = ('gcs://${t.bucket}/');`,
      `DESC STORAGE INTEGRATION ${si};   -- copy STORAGE_GCP_SERVICE_ACCOUNT for the gcloud step`,
      `CREATE DATABASE IF NOT EXISTS ${db};`,
      `CREATE SCHEMA IF NOT EXISTS ${sc};`,
      `CREATE ROLE IF NOT EXISTS ${role};`,
      `GRANT USAGE ON WAREHOUSE ${g('sf_wh')} TO ROLE ${role};`,
      `GRANT USAGE ON DATABASE ${db} TO ROLE ${role};`,
      `GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT ON SCHEMA ${sc} TO ROLE ${role};`,
      `GRANT USAGE ON INTEGRATION ${si} TO ROLE ${role};`,
      `GRANT ROLE ${role} TO USER <user of connection ${g('sf_conn')}>;`,
    ].join('\n');
  }

  function tokens() {
    const e = g('engine'), t = g('table'), n = name(g), bucket = g('bucket'), user = g('user');
    const db = g('db'), schema = g('schema'), bq = g('load') === 'bigquery', p = g('project');
    const sa = `rivet@${p}.iam.gserviceaccount.com`;
    const member = g('gcp_auth') === 'adc' ? 'user:$(gcloud config get-value account)' : `serviceAccount:${sa}`;
    const s3auth = ({
      sso: 'access_key_env: AWS_ACCESS_KEY_ID, secret_key_env: AWS_SECRET_ACCESS_KEY, session_token_env: AWS_SESSION_TOKEN',
      'access-keys': 'access_key_env: RIVET_AWS_ACCESS_KEY, secret_key_env: RIVET_AWS_SECRET_KEY',
      profile: `aws_profile: ${g('aws_profile')}`,
    })[g('aws_auth')];
    const dest = (top) => ({
      local: `{ type: local, path: ${g('path')} }`,
      gcs: `{ type: gcs, bucket: ${bucket}, prefix: ${top}/${n}/ }`,
      s3: `{ type: s3, bucket: ${bucket}, prefix: ${top}/${n}/, region: ${g('region')}, ${s3auth} }`,
      azure: `{ type: azure, bucket: ${bucket}, account_name: ${g('account')}, account_key_env: RIVET_AZURE_KEY, prefix: ${top}/${n}/ }`,
    })[g('dest')];
    const dsnDb = e === 'mysql' ? 'database' : 'dbname';
    const ctx = { bucket, p, member, sa };
    return {
      SOURCE_TYPE: e, TABLE: t, NAME: n, SCHEMA: schema, TLS: g('tls'), PK: g('pk'), CURSOR: g('cursor'),
      SLOT: g('slot'), CKPT_DIR: g('ckpt'), LOAD_KIND: g('load'),
      URL: `${SCHEME[e]}://${user}:\${DB_PASS}@${g('host')}:${g('port')}/${db}`,
      DSN: `host=${g('host')} port=${g('port')} ${dsnDb}=${db} user=${user} password=$DB_PASS`,
      VERIFY_TYPE: ['postgres', 'mysql'].includes(e) ? e : 'unsupported',
      DEST: dest('exports'),
      CDC_DEST: dest('cdc'),
      CLOUD_SETUP: cloudSetup(ctx),
      LOAD_SETUP: loadSetup(ctx),
      LOAD_SQL: loadSql(ctx),
      INIT_DEST: ({ gcs: ` --gcs-bucket ${bucket}`, s3: ` --s3-bucket ${bucket} --s3-region ${g('region')}` })[g('dest')] || '',
      CDC_PARAM: ({ postgres: `slot: ${g('slot')}`, mysql: `server_id: ${g('server_id')}`,
        mssql: `capture_instance: ${g('capture')}` })[e] || '# MongoDB: no engine-specific stream params',
      CDC_FLAG: ({ postgres: ` --slot ${g('slot')}`, mysql: ` --server-id ${g('server_id')}`,
        mssql: ` --capture-instance ${g('capture')}` })[e] || '',
      CDC_GRANTS: ({
        postgres: `ALTER ROLE ${user} WITH LOGIN REPLICATION;\n` +
          `GRANT SELECT ON ${t.includes('.') ? t : `${schema}.${t}`} TO ${user};`,
        mysql: `GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '${user}'@'%';\n` +
          `GRANT SELECT ON \`${db}\`.\`${n}\` TO '${user}'@'%';`,
        mssql: `-- one-time enable (sysadmin / db_owner)\nEXEC sys.sp_cdc_enable_db;\n` +
          `EXEC sys.sp_cdc_enable_table @source_schema = N'${schema}', @source_name = N'${n}',\n` +
          `     @role_name = NULL, @capture_instance = N'${g('capture')}', @supports_net_changes = 0;\n` +
          `-- runtime reader (least privilege)\nCREATE USER ${user} FOR LOGIN ${user};\nGRANT SELECT ON SCHEMA::cdc TO ${user};`,
      })[e] || `-- MongoDB: connect to a replica set; ${user} needs the read role on ${db}`,
      LOAD_TARGET: (bq
        ? ['target: bigquery', `project: ${p}`, `dataset: ${g('dataset')}`]
        : ['target: snowflake', `connection: ${g('sf_conn')}`, `warehouse: ${g('sf_wh')}`, `database: ${g('sf_db')}`,
          `schema: ${g('sf_schema')}`, `storage_integration: ${g('sf_int')}`]).join('\n  '),
      WAREHOUSE_TABLE: bq ? `${p}.${g('dataset')}.${n}` : `${g('sf_db')}.${g('sf_schema')}.${n}`,
      WAREHOUSE_SQL: bq ? `\`${p}.${g('dataset')}.${n}\`` : `${g('sf_db')}.${g('sf_schema')}.${n}`,
    };
  }

  // Captured before book.js highlights, so the originals are plain text.
  const blocks = [...document.querySelectorAll('main code')].filter((c) => c.textContent.includes('{{'));
  const originals = blocks.map((c) => c.textContent);

  function render() {
    for (const [n, , d] of fields) {
      if (!Array.isArray(d)) form.elements[n].placeholder = typeof d === 'function' ? d(g) : d;
    }
    for (const label of form.querySelectorAll('label[data-show]')) {
      label.hidden = !Object.entries(JSON.parse(label.dataset.show)).every(([k, vs]) => vs.includes(g(k)));
    }
    const v = tokens();
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
