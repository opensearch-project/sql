#!/usr/bin/env python3
"""
Markdown docs exporter for PPL documentation to documentation-website.
Auto-injects Jekyll front-matter, fixes relative links and adds copy buttons.

Note:
Exports ${REPO_ROOT}/docs/user/ppl to ${REPO_ROOT}/../documentation-website/_sql-and-ppl
Must clone documentation-website to ${REPO_ROOT}/../documentation-website before running this script.

sql
├── docs
|   └── user
|       └── ppl        <-- SOURCE DIRECTORY
├── scripts
│   └── docs_exporter  <-- YOU ARE HERE
|

documentation-website  <-- MUST BE SAME DIRECTORY AS sql
├── _sql-and-ppl
│   ├── ppl
│   ├── ppl-reference  <-- DESTINATION DIRECTORY
│   └── sql
"""

import re
import os
import argparse
from collections import defaultdict
from pathlib import Path
from typing import Optional


# Base path for links in the documentation website
DOCS_PARENT_BASE_PATH = "sql-and-ppl/ppl"
DOCS_PARENT_BASE_TITLE = "PPL"

# Directory name to heading mappings (as they appear on website)
DIR_NAMES_TO_HEADINGS_MAP = {
    "cmd": "Commands",
    "admin": "Administration",
    "functions": "Functions",
    "general": "General",
    "interfaces": "Interfaces",
    "limitations": "Limitations",
    "reference": "Reference",
}

# Custom redirect_from lists for specific files (relative path from source root)
# Required for backward compatibility from old website links. Injected in Jekyll front-matter.
CUSTOM_REDIRECTS = {
    "cmd/index.md": [
        "/search-plugins/sql/ppl/functions/",
        "/observability-plugin/ppl/commands/",
        "/search-plugins/ppl/commands/",
        "/search-plugins/ppl/functions/",
        "/sql-and-ppl/ppl/functions/",
    ],
}

# Directory name mappings for export (source_dir -> target_dir)
DIR_PATH_MAPPINGS = {
    "cmd": "commands",
}

# Command title overrides (filename -> custom title)
CMD_TITLE_OVERRIDES = {
    "showdatasources": "show datasources",
    "syntax": "PPL syntax",
}


def get_heading_for_dir(dir_name: str) -> str:
    """Get heading for directory name, using mapped value or fallback to title-case."""
    return DIR_NAMES_TO_HEADINGS_MAP.get(dir_name, dir_name.replace("-", " ").title())


def map_directory_path(rel_path: Path) -> Path:
    """Map directory paths from source to target naming conventions."""
    parts = list(rel_path.parts)

    # Apply directory mappings
    for i, part in enumerate(parts[:-1]):  # Don't map the filename itself
        if part in DIR_PATH_MAPPINGS:
            parts[i] = DIR_PATH_MAPPINGS[part]

    return Path(*parts)


def convert_sql_table_to_markdown(table_text: str) -> str:
    """Convert SQL CLI table format to markdown table."""
    lines = table_text.strip().split('\n')
    result = []
    header_done = False
    data_row_count = 0

    for line in lines:
        # Skip border lines (+---+---+), separator lines (|---+---|), and fetched rows line
        if re.match(r'^\+[-+]+\+$', line.strip()) or re.match(r'^\|[-+|]+\|$', line.strip()):
            continue
        if re.match(r'^fetched rows\s*/\s*total rows\s*=', line.strip()):
            continue
        # Data/header row
        if line.strip().startswith('|') and line.strip().endswith('|'):
            cells = [c.strip() for c in line.strip().strip('|').split('|')]
            # Escape angle brackets for Jekyll in converted tables (results tables)
            cells = [c.replace('<', '\\<').replace('>', '\\>').replace('*', '\\*') for c in cells]
            result.append('| ' + ' | '.join(cells) + ' |')
            if not header_done:
                result.append('|' + '|'.join([' --- ' for _ in cells]) + '|')
                header_done = True
            else:
                data_row_count += 1

    # Return empty string if table has no data rows (only header)
    if data_row_count == 0:
        return ''

    return '\n'.join(result)


def convert_tables_in_code_blocks(content: str) -> str:
    """Find and convert SQL CLI tables in code blocks to markdown tables."""
    def replace_table(match):
        block_content = match.group(1)
        # Check if this looks like a SQL CLI table
        if re.search(r'^\+[-+]+\+$', block_content, re.MULTILINE):
            return convert_sql_table_to_markdown(block_content)
        return match.group(0)

    # First, remove empty tables with their trailing blank line
    def replace_empty_table(match):
        block_content = match.group(1)
        # Check if table is empty
        if re.search(r'^\+[-+]+\+$', block_content, re.MULTILINE):
            converted_table = convert_sql_table_to_markdown(block_content)
            if converted_table == '':
                return ''  # Remove entire match (table + blank line)
        return match.group(0)  # Keep original if not empty table

    # Remove empty tables and their surrounding blank lines
    content = re.sub(r'\n```[^\n]*\n(.*?)```\n', replace_empty_table, content, flags=re.DOTALL)

    # Then convert remaining tables normally
    return re.sub(r'```[^\n]*\n(.*?)```', replace_table, content, flags=re.DOTALL)


def extract_title(content: str) -> Optional[str]:
    """Extract title from first H1 heading or return None."""
    match = re.search(r'^#\s+(.+)$', content, re.MULTILINE)
    return match.group(1).strip() if match else None


def generate_frontmatter(
    title: Optional[str],
    parent: Optional[str] = None,
    grand_parent: Optional[str] = None,
    nav_order: int = 1,
    has_children: bool = False,
    redirect_from: Optional[list] = None,
) -> str:
    """Generate Jekyll front-matter."""
    fm = ["---", "layout: default"]
    if title:
        fm.append(f"title: {title}")
    if parent:
        fm.append(f"parent: {parent}")
    if grand_parent:
        fm.append(f"grand_parent: {grand_parent}")
    fm.append(f"nav_order: {nav_order}")
    if has_children:
        fm.append("has_children: true")
    if redirect_from:
        fm.append("redirect_from:")
        for r in redirect_from:
            fm.append(f"  - {r}")
    fm.append("---\n")
    return "\n".join(fm)


def fix_link(match, current_file_path=None):
    """Fix relative links to use site.url prefix, preserving anchor-only (#heading) links.

    Handles:
    - Relative paths with ../ and ./
    - Same-directory links (file.md)
    - Same-directory links with ./ prefix (./file.md)
    - Subdirectory relative paths (subdir/file.md)
    - Anchor normalization for Jekyll
    - Rollup redirects for deep directory structures
    - Malformed syntax escaping
    """
    link = match.group(1)
    anchor = match.group(3) or ""

    # Keep anchor-only links unchanged
    if (not link and anchor) or link.startswith("#"):
        return match.group(0)

    # Skip external links
    if link.startswith("http"):
        return match.group(0)

    # Remove .md extension
    link = link.replace(".md", "")

    # Resolve path based on link type
    if (
        ("/" not in link and "\\" not in link) or link.startswith("./")
    ) and current_file_path:
        # Same directory link
        current_dir = str(current_file_path.parent).replace("\\", "/")
        clean_link = link.lstrip("./")
        resolved_path = (
            f"{current_dir}/{clean_link}" if current_dir != "." else clean_link
        )
    elif not link.startswith("../") and current_file_path:
        # Relative path within current directory context
        current_dir = str(current_file_path.parent).replace("\\", "/")
        resolved_path = f"{current_dir}/{link}" if current_dir != "." else link
    else:
        # Normalize relative path and remove leading ../
        resolved_path = os.path.normpath(link).replace("\\", "/")
        while resolved_path.startswith("../"):
            resolved_path = resolved_path[3:]

    # Clean up malformed paths
    resolved_path = re.sub(r"[,\s]+", "-", resolved_path.strip())

    # Normalize anchor for Jekyll (remove dots)
    if anchor:
        anchor = re.sub(r"[.]", "", anchor.lower())

    # Add trailing slash for directories (but not with anchors)
    if resolved_path and not resolved_path.endswith((".html", ".htm")) and not anchor:
        resolved_path = resolved_path.rstrip("/") + "/"

    return f"]({{{{site.url}}}}{{{{site.baseurl}}}}/{DOCS_PARENT_BASE_PATH}/{resolved_path}{anchor})"


def process_content(content: str, current_file_path=None) -> str:
    """Process markdown content with PPL->SQL conversion, copy buttons, and link fixes."""
    # Convert SQL CLI tables in code blocks to markdown tables
    content = convert_tables_in_code_blocks(content)
    
    # Convert PPL code fences to SQL
    content = re.sub(r'^```ppl\b.*$', '```sql', content, flags=re.MULTILINE)

    # Convert bash ignore blocks to JSON with copy-curl buttons
    content = re.sub(r'^```bash ignore\b.*?\n(.*?)^```$',
                     r'```json\n\1```\n{% include copy-curl.html %}',
                     content, flags=re.MULTILINE | re.DOTALL)

    # Add copy buttons after code fences
    content = re.sub(r'^```(bash|sh|sql)\b.*?\n(.*?)^```$',
                     r'```\1\n\2```\n{% include copy.html %}',
                     content, flags=re.MULTILINE | re.DOTALL)

    # Convert syntax code fences to SQL (for syntax definitions, no copy buttons)
    content = re.sub(r'^```syntax\b.*$', '```sql', content, flags=re.MULTILINE)

    # Convert relative links with current file context
    def fix_link_with_context(match):
        return fix_link(match, current_file_path)

    content = re.sub(
        r"\]\((?!https?://)(.*?)(\.md)?(#[^\)]*)?\)", fix_link_with_context, content
    )

    # Convert docs.opensearch.org links to site variables
    def fix_opensearch_link(match):
        path = match.group(1)
        return f"]({{{{site.url}}}}{{{{site.baseurl}}}}{path})"
    
    content = re.sub(
        r"\]\(https://docs\.opensearch\.org/[^/]+(.*?)\)", fix_opensearch_link, content
    )

    for source_dir, target_dir in DIR_PATH_MAPPINGS.items():
        content = content.replace(f'/{source_dir}/', f'/{target_dir}/')

    return content


def export_docs(
    source_dir: Path,
    target_dir: Path,
    auto_yes: bool = False,
    only_dirs: Optional[set] = None,
) -> None:
    """Export PPL docs to documentation website."""
    if not source_dir.exists():
        print(f"Source directory {source_dir} not found")
        return

    # Check if target directory exists and has files
    if target_dir.exists() and any(target_dir.glob('**/*.md')):
        if auto_yes:
            print(
                f"Target directory {target_dir} contains files. Auto-overwriting (--yes flag)."
            )
        else:
            response = input(
                f"Target directory {target_dir} contains files. Overwrite? (y/n): "
            )
            if response.lower() != "y":
                print("Export cancelled")
                return

    # Get all markdown files sorted alphabetically
    md_files = sorted(source_dir.glob("**/*.md"))

    # Filter to only specified directories if provided
    if only_dirs:
        md_files = [
            f for f in md_files if f.relative_to(source_dir).parts[0] in only_dirs
        ]

    # Filter to only specified directories if provided
    if only_dirs:
        md_files = [
            f for f in md_files if f.relative_to(source_dir).parts[0] in only_dirs
        ]

    # Group files by directory for local nav_order

    files_by_dir = defaultdict(list)
    for md_file in md_files:
        rel_path = md_file.relative_to(source_dir)

        # Determine final directory after potential roll-up
        if len(rel_path.parts) >= 3:
            # Will be rolled up to parent directory
            final_dir = rel_path.parts[0]
        else:
            final_dir = str(rel_path.parent)

        files_by_dir[final_dir].append(md_file)

    # Sort files within each directory alphabetically for proper nav_order
    for dir_name in files_by_dir:
        files_by_dir[dir_name].sort(key=lambda f: f.name)
        if dir_name == "cmd" and any(f.name == "syntax.md" for f in files_by_dir[dir_name]):
            files_by_dir[dir_name].sort(key=lambda f: (f.name != "syntax.md", f.name))

    for _, files in files_by_dir.items():
        for i, md_file in enumerate(files, 1):
            rel_path = md_file.relative_to(source_dir)
            rel_path_str = str(rel_path)

            # Check for custom redirects
            redirect_from = CUSTOM_REDIRECTS.get(rel_path_str, None)

            # Roll up third-level files to second level to avoid rendering limitations
            if len(rel_path.parts) >= 3:
                # Move from admin/connectors/file.md to admin/connectors_file.md
                parent_dir = rel_path.parts[0]  # e.g., "admin"
                subdir = rel_path.parts[1]  # e.g., "connectors"
                filename = rel_path.name  # e.g., "prometheus_connector.md"
                new_filename = f"{subdir}_{filename}"
                target_file = target_dir / parent_dir / new_filename

                # Generate redirect_from for the original path
                original_path = f"/{DOCS_PARENT_BASE_PATH}/{rel_path.with_suffix('')}/"
                redirect_from = (redirect_from or []) + [original_path]

                print(
                    f"\033[93mWARNING: Rolling up {rel_path} to {parent_dir}/{new_filename} due to rendering limitations\033[0m"
                )

                # Update rel_path for parent/grand_parent logic
                rel_path = Path(parent_dir) / new_filename
            else:
                mapped_path = map_directory_path(rel_path)
                target_file = target_dir / mapped_path

            # Determine parent and grand_parent based on directory structure
            if rel_path.parent == Path("."):
                # Root level files
                parent = None
                grand_parent = None
            elif len(rel_path.parts) == 2:
                # Second level files (including rolled-up files)
                parent = get_heading_for_dir(rel_path.parent.name)
                grand_parent = DOCS_PARENT_BASE_TITLE
            else:
                # This shouldn't happen after roll-up, but keeping for safety
                parent = get_heading_for_dir(rel_path.parent.name)
                grand_parent = get_heading_for_dir(rel_path.parts[-3])

            # Check if this is an index.md (root or directory) - these have children
            is_index = rel_path.name == "index.md"
            has_children = is_index
            
            # For directory index files, parent should be one level up
            if is_index and rel_path.parent != Path("."):
                parent = DOCS_PARENT_BASE_TITLE
                grand_parent = None

            # Determine title - use directory name for index files, filename for cmd files
            if is_index:
                # For index files, use the directory heading as title
                title = get_heading_for_dir(rel_path.parent.name) if rel_path.parent != Path(".") else DOCS_PARENT_BASE_TITLE
            elif len(rel_path.parts) >= 2 and rel_path.parts[0] == "cmd":
                # For command files, check for custom title override first
                if md_file.stem in CMD_TITLE_OVERRIDES:
                    title = CMD_TITLE_OVERRIDES[md_file.stem]
                else:
                    # Use filename as ground truth
                    title = md_file.stem.replace("-", " ")
            else:
                title = (
                    extract_title(md_file.read_text(encoding="utf-8"))
                    or md_file.stem.replace("-", " ").title()
                )
            frontmatter = generate_frontmatter(
                title, parent, grand_parent, i, has_children, redirect_from
            )

            # Process the file content
            content = md_file.read_text(encoding="utf-8")
            rel_path_for_links = md_file.relative_to(source_dir)
            content = process_content(content, rel_path_for_links)

            target_file.parent.mkdir(parents=True, exist_ok=True)
            target_file.write_text(frontmatter + content, encoding="utf-8")
            print(f"Exported: {md_file} -> {target_file}")

    # Generate index.md for each directory (only up to second level)
    dirs = set(md_file.relative_to(source_dir).parent for md_file in md_files)
    for dir_path in sorted(dirs):
        if dir_path == Path("."):
            continue
        # Skip third-level directories since files are rolled up
        if len(dir_path.parts) > 1:
            continue
        # Skip directories not in only_dirs filter
        if only_dirs and dir_path.parts[0] not in only_dirs:
            continue
        # Skip if source index.md exists (it will be exported with the other files)
        if (source_dir / dir_path / "index.md").exists():
            continue

        target_index = target_dir / dir_path / "index.md"
        title = get_heading_for_dir(dir_path.name)

        # Determine parent for directory index based on depth
        if len(dir_path.parts) == 1:
            # Second-level directory (e.g., admin/) - parent is root title
            parent = DOCS_PARENT_BASE_TITLE
        else:
            # This shouldn't happen after filtering, but keeping for safety
            parent = get_heading_for_dir(dir_path.parent.name)

        frontmatter = generate_frontmatter(title, parent, None, has_children=True)
        target_index.write_text(frontmatter, encoding='utf-8')
        print(f"Generated: {target_index}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export PPL docs to documentation website"
    )
    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="Automatically overwrite existing files without prompting",
    )
    parser.add_argument(
        "--only-dirs",
        type=str,
        help="Comma-separated list of directories to export (e.g., 'cmd' or 'cmd,functions')",
    )
    args = parser.parse_args()

    script_dir = Path(__file__).parent
    source_dir_ppl = script_dir / "../../docs/user/ppl"
    target_dir_ppl = (
        script_dir / f"../../../documentation-website/_{DOCS_PARENT_BASE_PATH}"
    )
    only_dirs = set(args.only_dirs.split(",")) if args.only_dirs else None
    export_docs(source_dir_ppl, target_dir_ppl, args.yes, only_dirs)
