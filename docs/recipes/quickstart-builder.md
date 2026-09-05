# Quickstart builder — pick a database, paste one block

Choose your source engine, where it lives, and where the files should land. The
builder assembles the **whole first-run flow** — `init` → `doctor` → `check` →
`run` → inspect — as one shell block with your values already in place. Copy it,
paste it into a terminal, done.

> **Reading this on GitHub?** The form below only works in the rendered docs:
> **<https://panchenkoai.github.io/rivet/recipes/quickstart-builder.html>**.
> The [ready-made recipes](#ready-made-recipes) further down are plain text and
> work anywhere.

Nothing you type here leaves the page — the generator is a few lines of
client-side JavaScript, there is no network call. Passwords are URL-encoded
into the connection string and single-quoted for the shell, so `p@ss/w0rd!`
works as-is.

<style>
.qb{border:1px solid var(--theme-popup-border,#ccc);border-radius:6px;padding:1rem 1.2rem;margin:1rem 0;background:var(--quote-bg,rgba(0,0,0,.03))}
.qb fieldset{border:0;margin:0 0 .8rem;padding:0}
.qb legend{font-weight:600;margin-bottom:.3rem;padding:0}
.qb .row{display:flex;flex-wrap:wrap;gap:.5rem 1rem;align-items:center}
.qb label{display:inline-flex;align-items:center;gap:.35rem;font-size:.95em}
.qb input[type=text],.qb input[type=password],.qb select{font:inherit;padding:.25rem .4rem;border:1px solid var(--theme-popup-border,#bbb);border-radius:4px;background:var(--bg,#fff);color:var(--fg,#000);min-width:9rem}
.qb input.wide{min-width:22rem;flex:1}
.qb .hint{font-size:.85em;opacity:.75;margin:.2rem 0 0}
.qb .out{position:relative;margin-top:.6rem}
.qb .out pre{margin:0;padding:.9rem;max-height:34rem;overflow:auto}
.qb .out button{position:absolute;top:.4rem;right:.4rem;font:inherit;font-size:.85em;padding:.2rem .6rem;border-radius:4px;border:1px solid var(--theme-popup-border,#bbb);background:var(--bg,#fff);color:var(--fg,#000);cursor:pointer}
.qb .out h4{margin:.8rem 0 .2rem;font-size:1em}
.qb [hidden]{display:none!important}
</style>

<details id="qbDetails">
<summary><strong>Interactive builder</strong> — needs the rendered docs (JavaScript); on GitHub use the ready-made recipes below.</summary>
<div class="qb" id="qb">
<fieldset>
<legend>1 · Source</legend>
<div class="row">
<label>Engine <select id="engine">
<option value="postgres">PostgreSQL</option>
<option value="mysql" selected>MySQL / MariaDB</option>
<option value="mssql">SQL Server</option>
<option value="mongo">MongoDB</option>
</select></label>
<label>Runs where <select id="where">
<option value="local">on this machine (localhost / docker)</option>
<option value="remote" selected>remote host (Cloud SQL, RDS, Azure, Atlas…)</option>
</select></label>
<label id="tlsWrap">TLS <select id="tls">
<option value="require">require (encrypt, don't verify cert)</option>
<option value="verify-full" selected>verify-full (system CA)</option>
<option value="verify-ca">verify-ca (private CA)</option>
<option value="disable">disable (plaintext — trusted network only)</option>
</select></label>
<label id="caWrap">CA file <input type="text" id="ca" placeholder="/etc/rivet/server-ca.pem"></label>
</div>
<div class="row" style="margin-top:.5rem">
<label><input type="radio" name="urlmode" value="fields" checked> build the URL from fields</label>
<label><input type="radio" name="urlmode" value="paste"> I already have a connection URL</label>
</div>
<div class="row" id="fieldsWrap" style="margin-top:.5rem">
<label>Host <input type="text" id="host" placeholder="db.example.com"></label>
<label>Port <input type="text" id="port" style="min-width:5rem" placeholder="3306"></label>
<label>Database <input type="text" id="db" placeholder="shop"></label>
<label>User <input type="text" id="user" placeholder="rivet_ro"></label>
<label>Password <input type="password" id="pass" placeholder="••••••"></label>
</div>
<div class="row" id="pasteWrap" hidden style="margin-top:.5rem">
<label style="flex:1">URL <input type="text" id="url" class="wide" placeholder="mysql://user:pass@host:3306/db"></label>
</div>
<div class="row" style="margin-top:.5rem">
<label>Table <input type="text" id="table" placeholder="orders (blank = whole schema)"></label>
<label id="schemaWrap">Schema <input type="text" id="schema" placeholder="public"></label>
<label>What to capture <select id="mode">
<option value="batch" selected>batch snapshot (full / incremental / chunked — init picks)</option>
<option value="cdc">change data capture (transaction log)</option>
</select></label>
</div>
<p class="hint" id="cdcHint" hidden></p>
</fieldset>
<fieldset>
<legend>2 · Destination</legend>
<div class="row">
<label>Type <select id="dest">
<option value="local">local directory (./output/)</option>
<option value="gcs" selected>Google Cloud Storage</option>
<option value="s3">Amazon S3</option>
<option value="azure">Azure Blob Storage</option>
</select></label>
<span id="gcsWrap" class="row">
<label>Bucket <input type="text" id="gcsBucket" placeholder="my-exports"></label>
<label>Auth <select id="gcsAuth">
<option value="adc" selected>gcloud ADC (already logged in)</option>
<option value="keyfile">service-account JSON key</option>
</select></label>
<label id="gcsKeyWrap" hidden>Key file <input type="text" id="gcsKey" placeholder="/secrets/rivet-sa.json"></label>
</span>
<span id="s3Wrap" class="row" hidden>
<label>Bucket <input type="text" id="s3Bucket" placeholder="my-exports"></label>
<label>Region <input type="text" id="s3Region" placeholder="eu-central-1"></label>
<label>Auth <select id="s3Auth">
<option value="chain" selected>AWS default chain (profile / role / SSO)</option>
<option value="keys">static access key</option>
</select></label>
<label id="s3KeyWrap" hidden>Key id <input type="text" id="s3KeyId" placeholder="AKIA…"></label>
<label id="s3SecretWrap" hidden>Secret <input type="password" id="s3Secret" placeholder="••••••"></label>
</span>
<span id="azureWrap" class="row" hidden>
<label>Container <input type="text" id="azContainer" placeholder="exports"></label>
<label>Account <input type="text" id="azAccount" placeholder="mystorageacct"></label>
<label>Account key <input type="password" id="azKey" placeholder="••••••"></label>
</span>
</div>
</fieldset>
<fieldset>
<legend>3 · Run with</legend>
<div class="row">
<label><input type="radio" name="runner" value="binary" checked> the <code>rivet</code> binary (brew / cargo / release tarball)</label>
<label><input type="radio" name="runner" value="docker"> Docker image <code>ghcr.io/panchenkoai/rivet</code></label>
</div>
</fieldset>
<div id="outputs"></div>
</div>
</details>

<script>
(function () {
  'use strict';
  var $ = function (id) { return document.getElementById(id); };

  var DEFAULT_PORT = { postgres: '5432', mysql: '3306', mssql: '1433', mongo: '27017' };
  var SCHEME = { postgres: 'postgresql', mysql: 'mysql', mssql: 'sqlserver', mongo: 'mongodb' };
  var DEFAULT_SCHEMA = { postgres: 'public', mssql: 'dbo' };
  var ENGINE_NAME = { postgres: 'PostgreSQL', mysql: 'MySQL', mssql: 'SQL Server', mongo: 'MongoDB' };
  var DEST_NAME = { local: 'local files', gcs: 'Google Cloud Storage', s3: 'Amazon S3', azure: 'Azure Blob Storage' };
  var CDC_PREREQ = {
    postgres: 'wal_level=logical on the server and a role with REPLICATION (init writes slot: rivet_slot).',
    mysql: 'binlog_format=ROW on the server and REPLICATION SLAVE + REPLICATION CLIENT grants for the user.',
    mssql: 'CDC enabled on the table (sys.sp_cdc_enable_table) and SQL Server Agent running.',
    mongo: 'a replica set (change streams do not work on a standalone mongod).'
  };

  // Shell-safe single quoting: 'it''s' → 'it'\''s'.
  function sq(s) { return "'" + String(s).replace(/'/g, "'\\''") + "'"; }
  // A value is "filled" when the user typed something.
  function val(id) { return $(id).value.trim(); }
  function radio(name) {
    var el = document.querySelector('input[name="' + name + '"]:checked');
    return el ? el.value : '';
  }

  // Assemble scheme://user:pass@host:port/db with credentials URL-encoded.
  function buildUrl(s) {
    if (s.urlmode === 'paste') { return s.url || SCHEME[s.engine] + '://user:pass@host:port/db'; }
    var host = s.host || (s.where === 'local' ? 'localhost' : 'db.example.com');
    if (s.runner === 'docker' && s.where === 'local') { host = 'host.docker.internal'; }
    var port = s.port || DEFAULT_PORT[s.engine];
    var db = s.db || 'mydb';
    var auth = '';
    if (s.user) {
      auth = encodeURIComponent(s.user);
      if (s.pass) { auth += ':' + encodeURIComponent(s.pass); }
      auth += '@';
    } else if (s.engine !== 'mongo') {
      auth = 'user:pass@';
    }
    return SCHEME[s.engine] + '://' + auth + host + ':' + port + '/' + db;
  }

  // The host the TLS gate sees: loopback → plaintext allowed; anything else needs --tls.
  function hostIsLoopback(s) {
    if (s.runner === 'docker' && s.where === 'local') { return false; } // host.docker.internal is not loopback
    if (s.urlmode === 'paste') {
      var m = /^[a-z+]+:\/\/(?:[^@\/]*@)?([^\/?#]*)/i.exec(s.url || '');
      if (!m) { return s.where === 'local'; }
      return m[1].split(',').every(function (hp) {
        var h = hp.replace(/^\[([^\]]*)\].*$/, '$1').split(':')[0];
        return /^localhost$/i.test(h) || /^127\./.test(h) || h === '::1';
      });
    }
    return s.where === 'local';
  }

  // Produce the recipe as a list of sections [{title, lang, text}].
  function buildRecipe(s) {
    var out = [];
    var sh = [];
    var rivet = 'rivet'; // with Docker, a same-named shell function defined below
    var tableName = s.table || 'orders';
    var landing = s.table ? tableName : '<table>';
    var url = buildUrl(s);
    var loopback = hostIsLoopback(s);
    var dockerLocal = s.runner === 'docker' && s.where === 'local';
    var step = 0;
    var n = function () { step += 1; return '# ' + step + ' · '; };

    sh.push('# Rivet quickstart: ' + ENGINE_NAME[s.engine] + ' → ' + DEST_NAME[s.dest] +
            (s.mode === 'cdc' ? ' (change data capture)' : ' (batch snapshot)'));
    sh.push('# Generated by docs/recipes/quickstart-builder — every step is safe to re-run.');
    sh.push('');
    sh.push(n() + 'Source connection. `rivet init` writes `url_env: DATABASE_URL` into the config,');
    sh.push('#     so the URL (and the password) never lands in a file.');
    sh.push('export DATABASE_URL=' + sq(url));
    sh.push('');

    // Destination credentials.
    var dockerEnv = ['-e DATABASE_URL'];
    var dockerMounts = ['-v "$PWD":/work'];
    if (s.dest === 'gcs') {
      sh.push(n() + 'Destination credentials.');
      if (s.gcsAuth === 'adc' && s.runner !== 'docker') {
        sh.push('#     Application Default Credentials — run once per machine, or skip on GCE / Cloud Run:');
        sh.push('gcloud auth application-default login');
      } else {
        if (s.gcsAuth === 'adc') {
          sh.push('#     Inside Docker the gcloud ADC file is not visible, so a service-account key is mounted instead.');
        }
        var key = s.gcsKey || '/secrets/rivet-sa.json';
        if (s.runner === 'docker') {
          dockerMounts.push('-v ' + sq(key) + ':/sa.json:ro');
          dockerEnv.push('-e GOOGLE_APPLICATION_CREDENTIALS=/sa.json');
          sh.push('#     Service-account JSON key (needs roles/storage.objectAdmin on the bucket): ' + key);
        } else {
          sh.push('#     Service-account JSON key (needs roles/storage.objectAdmin on the bucket):');
          sh.push('export GOOGLE_APPLICATION_CREDENTIALS=' + sq(key));
        }
      }
      sh.push('');
    } else if (s.dest === 's3') {
      sh.push(n() + 'Destination credentials.');
      if (s.s3Auth === 'keys') {
        sh.push('export AWS_ACCESS_KEY_ID=' + sq(s.s3KeyId || 'AKIA...'));
        sh.push('export AWS_SECRET_ACCESS_KEY=' + sq(s.s3Secret || '...'));
        dockerEnv.push('-e AWS_ACCESS_KEY_ID', '-e AWS_SECRET_ACCESS_KEY');
      } else {
        sh.push('#     AWS default credential chain (~/.aws/credentials, instance role, SSO) — nothing to export.');
        if (s.runner === 'docker') {
          dockerMounts.push('-v "$HOME/.aws":/home/rivet/.aws:ro');
          dockerEnv.push('-e HOME=/home/rivet', '-e AWS_PROFILE');
        }
      }
      sh.push('');
    } else if (s.dest === 'azure') {
      sh.push(n() + 'Destination credentials (Storage account → Access keys → key1).');
      sh.push('export RIVET_AZURE_KEY=' + sq(s.azKey || '<account-key>'));
      dockerEnv.push('-e RIVET_AZURE_KEY');
      sh.push('');
    }

    if (s.runner === 'docker') {
      var extra = dockerLocal ? ' --add-host=host.docker.internal:host-gateway' : '';
      sh.push('# `rivet` becomes a shell function that runs the container. The current directory is mounted');
      sh.push('# as /work, so rivet.yaml, .rivet_state.db and ./output/ land next to you on the host.');
      sh.push('rivet() { docker run --rm -i --user "$(id -u):$(id -g)" ' + dockerEnv.join(' ') + ' ' +
              dockerMounts.join(' ') + ' -w /work' + extra + ' ghcr.io/panchenkoai/rivet:latest "$@"; }');
      sh.push('');
    }

    // init
    var init = [rivet, 'init', '--source-env DATABASE_URL'];
    var schema = s.schema && s.schema !== DEFAULT_SCHEMA[s.engine] ? s.schema : '';
    if (s.table) {
      init.push('--table ' + sq(schema ? schema + '.' + tableName : tableName));
    } else if (schema) {
      init.push('--schema ' + sq(schema));
    }
    if (s.mode === 'cdc') { init.push('--mode cdc'); }
    if (dockerLocal) {
      init.push('--tls disable');
    } else if (!loopback) {
      init.push('--tls ' + s.tls);
      if ((s.tls === 'verify-ca' || s.tls === 'verify-full') && s.ca) { init.push('--tls-ca ' + sq(s.ca)); }
    }
    if (s.dest === 'gcs') { init.push('--gcs-bucket ' + sq(s.gcsBucket || 'my-exports')); }
    if (s.dest === 's3') {
      init.push('--s3-bucket ' + sq(s.s3Bucket || 'my-exports'));
      init.push('--s3-region ' + sq(s.s3Region || 'us-east-1'));
    }
    init.push('-o rivet.yaml');

    sh.push(n() + 'Scaffold the config from the live schema (connects once, reads columns + row estimates).');
    if (dockerLocal) {
      sh.push('#     From the container your machine is host.docker.internal, not loopback — hence --tls disable.');
    } else if (!loopback) {
      sh.push('#     A non-loopback host requires an explicit --tls posture; init records it as `source.tls:`.');
    }
    if (!s.table) {
      sh.push('#     No --table given: one export per table/view. Add --include/--exclude GLOB to narrow it.');
    }
    sh.push(init.join(' '));
    sh.push('');

    if (s.dest === 'azure') {
      sh.push(n() + '`rivet init` has no --azure flag: open rivet.yaml and replace each `destination:` block');
      sh.push('#     with the YAML shown below this script, then continue.');
      sh.push('');
    }

    sh.push(n() + 'Preflight: source + destination auth (doctor)' + (s.mode === 'batch' ? ', then a per-export schema/type report (check).' : '.'));
    sh.push(rivet + ' doctor -c rivet.yaml');
    if (s.mode === 'batch') { sh.push(rivet + ' check  -c rivet.yaml'); }
    sh.push('');

    if (s.mode === 'cdc') {
      sh.push(n() + 'Capture: drain every change since the checkpoint to typed Parquet, then exit.');
      sh.push('#     Source prerequisite: ' + CDC_PREREQ[s.engine]);
      sh.push('#     The first run starts at the current log position — make a change, run again, it lands as a row (__op).');
      sh.push('#     Schedule this line (cron / Airflow) for continuous capture; each run resumes from the checkpoint.');
      sh.push(rivet + ' run -c rivet.yaml');
    } else {
      sh.push(n() + 'Export. --validate re-reads every file and checks row counts' +
              (s.engine === 'mongo' ? '.' : '; --reconcile compares with a source COUNT(*).'));
      sh.push(rivet + ' run -c rivet.yaml --validate' + (s.engine === 'mongo' ? '' : ' --reconcile'));
    }
    sh.push('');

    var where = '';
    if (s.dest === 'local') { where = './output/' + landing + '/'; }
    if (s.dest === 'gcs') { where = 'gs://' + (s.gcsBucket || 'my-exports') + '/exports/' + landing + '/'; }
    if (s.dest === 's3') { where = 's3://' + (s.s3Bucket || 'my-exports') + '/exports/' + landing + '/'; }
    if (s.dest === 'azure') { where = 'https://' + (s.azAccount || 'mystorageacct') + '.blob.core.windows.net/' + (s.azContainer || 'exports') + '/exports/' + landing + '/'; }
    sh.push(n() + 'Inspect. Files: ' + where + ' (+ manifest.json and _SUCCESS per export).');
    sh.push(rivet + ' state files -c rivet.yaml');
    sh.push(rivet + ' metrics -c rivet.yaml --last 5');
    if (s.mode === 'batch') {
      sh.push('# Second run only exports new rows if the export is `mode: incremental` (cursor_column:) —');
      sh.push('# see the mode comment init left in rivet.yaml. Interrupted run? `' + rivet + ' run -c rivet.yaml --resume`.');
    }

    out.push({ title: 'Shell — copy, paste, run', lang: 'bash', text: sh.join('\n') });

    if (s.dest === 'azure') {
      var y = [
        '    destination:',
        '      type: azure',
        '      bucket: ' + (s.azContainer || 'exports') + '          # container name',
        '      account_name: ' + (s.azAccount || 'mystorageacct'),
        '      account_key_env: RIVET_AZURE_KEY',
        '      prefix: exports/' + landing + '/'
      ];
      out.push({ title: 'rivet.yaml — replace each `destination:` block with', lang: 'yaml', text: y.join('\n') });
    }
    return out;
  }

  function readState() {
    return {
      engine: $('engine').value, where: $('where').value, tls: $('tls').value, ca: val('ca'),
      urlmode: radio('urlmode'), url: val('url'),
      host: val('host'), port: val('port'), db: val('db'), user: val('user'), pass: $('pass').value,
      table: val('table'), schema: val('schema'), mode: $('mode').value,
      dest: $('dest').value,
      gcsBucket: val('gcsBucket'), gcsAuth: $('gcsAuth').value, gcsKey: val('gcsKey'),
      s3Bucket: val('s3Bucket'), s3Region: val('s3Region'), s3Auth: $('s3Auth').value, s3KeyId: val('s3KeyId'), s3Secret: $('s3Secret').value,
      azContainer: val('azContainer'), azAccount: val('azAccount'), azKey: $('azKey').value,
      runner: radio('runner')
    };
  }

  function escapeHtml(t) {
    return t.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  function render() {
    var s = readState();
    // Visibility.
    $('tlsWrap').hidden = s.where === 'local';
    $('caWrap').hidden = s.where === 'local' || (s.tls !== 'verify-ca' && s.tls !== 'verify-full');
    $('fieldsWrap').hidden = s.urlmode !== 'fields';
    $('pasteWrap').hidden = s.urlmode !== 'paste';
    $('schemaWrap').hidden = !(s.engine === 'postgres' || s.engine === 'mssql');
    $('schema').placeholder = DEFAULT_SCHEMA[s.engine] || '';
    $('port').placeholder = DEFAULT_PORT[s.engine];
    $('table').placeholder = s.engine === 'mongo' ? 'orders (blank = every collection)' : 'orders (blank = whole schema)';
    $('gcsWrap').hidden = s.dest !== 'gcs';
    $('s3Wrap').hidden = s.dest !== 's3';
    $('azureWrap').hidden = s.dest !== 'azure';
    $('gcsKeyWrap').hidden = s.gcsAuth !== 'keyfile';
    $('s3KeyWrap').hidden = s.s3Auth !== 'keys';
    $('s3SecretWrap').hidden = s.s3Auth !== 'keys';
    var cdcHint = $('cdcHint');
    cdcHint.hidden = s.mode !== 'cdc';
    cdcHint.textContent = s.mode === 'cdc' ? 'CDC prerequisite on the source: ' + CDC_PREREQ[s.engine] + ' Full list: reference/cdc.md.' : '';

    // Output.
    var sections = buildRecipe(s);
    var html = '';
    sections.forEach(function (sec, i) {
      html += '<h4>' + escapeHtml(sec.title) + '</h4>' +
              '<div class="out"><button type="button" data-copy="' + i + '">Copy</button>' +
              '<pre><code class="language-' + sec.lang + '">' + escapeHtml(sec.text) + '</code></pre></div>';
    });
    var outputs = $('outputs');
    outputs.innerHTML = html;
    outputs.__sections = sections;
  }

  document.getElementById('qb').addEventListener('input', render);
  document.getElementById('qb').addEventListener('change', render);
  document.getElementById('outputs').addEventListener('click', function (ev) {
    var btn = ev.target.closest('button[data-copy]');
    if (!btn) { return; }
    var text = $('outputs').__sections[Number(btn.getAttribute('data-copy'))].text;
    var done = function () { btn.textContent = 'Copied'; setTimeout(function () { btn.textContent = 'Copy'; }, 1500); };
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(done, function () { window.prompt('Copy:', text); });
    } else {
      window.prompt('Copy:', text);
    }
  });
  // Expose for tests / console.
  window.rivetQuickstart = { buildRecipe: buildRecipe, buildUrl: buildUrl };
  document.getElementById('qbDetails').open = true; // collapsed only where scripts do not run (GitHub)
  render();
})();
</script>

## What the generated block does

| Step | Command | Why it is there |
|---|---|---|
| 1 | `export DATABASE_URL=…` | The only place the password exists. `init` writes `url_env: DATABASE_URL` into the YAML, never the URL. |
| 2 | cloud credentials | GCS: ADC or a service-account key. S3: default chain or static keys. Azure: account key in `RIVET_AZURE_KEY`. |
| 3 | `rivet init … -o rivet.yaml` | Connects once, introspects columns + row estimates, picks a mode per table, writes the destination block. Remote hosts need `--tls`. |
| 4 | `rivet doctor` / `rivet check` | Fail loudly on auth or network problems before a single row is read; `check` reports types and query cost. |
| 5 | `rivet run --validate --reconcile` | The export. Output is re-read and row-counted, then compared with a source `COUNT(*)`. |
| 6 | `rivet state files` / `rivet metrics` | What was written, and the run history kept in `.rivet_state.db`. |

Two facts worth knowing before the first run:

- **Remote hosts require a TLS posture.** Any host that is not `localhost` /
  `127.x` / `::1` is refused without `--tls` (`require`, `verify-ca`,
  `verify-full`, or an explicit `disable`). From a Docker container your own
  machine is `host.docker.internal`, which is *not* loopback — the builder adds
  `--tls disable` for that case.
- **Buckets must already exist.** `rivet doctor` writes a small probe object to
  verify write access; it does not create buckets or containers.

## Ready-made recipes

The same flow, pre-filled for the four most common pairings. Replace the
placeholders in `<angle brackets>`.

### MySQL on Cloud SQL → Google Cloud Storage

Full walkthrough with expected output and troubleshooting:
[quickstart-mysql-gcs.md](quickstart-mysql-gcs.md).

```bash
export DATABASE_URL='mysql://<user>:<password>@<cloud-sql-ip>:3306/<db>'
gcloud auth application-default login          # or: export GOOGLE_APPLICATION_CREDENTIALS=/path/sa.json
rivet init --source-env DATABASE_URL --table <table> --tls require --gcs-bucket <bucket> -o rivet.yaml
rivet doctor -c rivet.yaml && rivet check -c rivet.yaml
rivet run -c rivet.yaml --validate --reconcile
rivet state files -c rivet.yaml                # → gs://<bucket>/exports/<table>/
```

### PostgreSQL on RDS → Amazon S3

```bash
export DATABASE_URL='postgresql://<user>:<password>@<rds-endpoint>:5432/<db>'
export AWS_PROFILE=<profile>                   # or AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
rivet init --source-env DATABASE_URL --table <table> --tls verify-full --s3-bucket <bucket> --s3-region <region> -o rivet.yaml
rivet doctor -c rivet.yaml && rivet check -c rivet.yaml
rivet run -c rivet.yaml --validate --reconcile
rivet state files -c rivet.yaml                # → s3://<bucket>/exports/<table>/
```

### SQL Server → Azure Blob Storage

```bash
export DATABASE_URL='sqlserver://<user>:<password>@<host>:1433/<db>'
export RIVET_AZURE_KEY='<storage-account-key>'
rivet init --source-env DATABASE_URL --table dbo.<table> --tls verify-full -o rivet.yaml
# init has no --azure flag: in rivet.yaml replace the `destination:` block with
#   destination: { type: azure, bucket: <container>, account_name: <account>, account_key_env: RIVET_AZURE_KEY, prefix: exports/<table>/ }
rivet doctor -c rivet.yaml && rivet check -c rivet.yaml
rivet run -c rivet.yaml --validate --reconcile
```

### Anything on localhost → local Parquet (no cloud at all)

```bash
export DATABASE_URL='postgresql://<user>:<password>@localhost:5432/<db>'   # or mysql:// sqlserver:// mongodb://
rivet init --source-env DATABASE_URL --table <table> -o rivet.yaml         # loopback: no --tls needed
rivet doctor -c rivet.yaml && rivet check -c rivet.yaml
rivet run -c rivet.yaml --validate
ls output/<table>/                              # *.parquet + manifest.json + _SUCCESS
```

Next: [Getting started](../getting-started.md) explains each step in depth;
[reference/init.md](../reference/init.md) lists every `init` flag;
[destinations/](../destinations/local.md) covers the cloud auth options in full.
