# PPL Documentation Exporter

Exports PPL documentation to the OpenSearch documentation website. Auto-injects Jekyll front-matter, converts SQL CLI tables to markdown, fixes relative links, and adds copy buttons.

## Directory Structure

```
sql/
├── docs/user/ppl/           <-- SOURCE
├── scripts/docs_exporter/   <-- THIS TOOL

documentation-website/       <-- MUST BE SIBLING OF sql/
└── _sql-and-ppl/ppl/        <-- DESTINATION
```

## SOP: Exporting to Documentation Website

### 1. Clone documentation-website to same root as `sql` repo (first time only)

```bash
cd /path/to/sql/../
git clone https://github.com/opensearch-project/documentation-website.git
```

### 2. Rebase documentation-website to latest

```bash
cd documentation-website
git fetch origin
git rebase origin/main
```

### 3. Run the export

As of Dec 17 2025, the migration to auto-export for documentation-website is ongoing.  
Currently select directories (e.g. `docs/user/ppl/cmd`) are only exported to documentation-website. 

#### How to export specific directories only:
```bash
# Export only cmd/
./export_to_docs_website.py --only-dirs cmd

# Export cmd/ and functions/
./export_to_docs_website.py --only-dirs cmd,functions
```

#### How to export all directories
```bash
cd sql/scripts/docs_exporter
./export_to_docs_website.py
```

### 4. Review and commit changes

```bash
cd documentation-website
git diff
git add -A
git commit -m "Update PPL documentation"
```

### 5. Open Pull Request in documentation-website repo
Example: https://github.com/opensearch-project/documentation-website/pull/11688

## Options

| Option | Description |
|--------|-------------|
| `-y, --yes` | Auto-overwrite existing files without prompting |
| `--only-dirs` | Comma-separated list of directories to export (e.g., `cmd`, `cmd,functions`) |

## What the exporter does

- Injects Jekyll front-matter (title, parent, nav_order, etc.)
- Converts SQL CLI table output to markdown tables
- Removes empty tables and their surrounding whitespace
- Escapes angle brackets and asterisks in table cells for Jekyll compatibility
- Converts `docs.opensearch.org` links to Jekyll site variables
- Fixes relative links to use `{{site.url}}{{site.baseurl}}`
- Handles anchor normalization (removes dots)
- Converts `ppl` code fences to `sql`
- Converts `bash ignore` code blocks to `json` with curl copy buttons
- Adds copy buttons to code blocks
- Converts markdown emphasis (**Note**, **Warning**, **Important**) to Jekyll attribute syntax
- Rolls up third-level directories to avoid Jekyll rendering limitations
