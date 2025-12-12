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
from collections import defaultdict
from pathlib import Path
from typing import Optional


# Base path for links in the documentation website
DOCS_BASE_PATH = "sql-and-ppl/ppl-reference"
DOCS_BASE_TITLE = "OpenSearch PPL Reference Manual"

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


def get_heading_for_dir(dir_name: str) -> str:
    """Get heading for directory name, using mapped value or fallback to title-case."""
    return DIR_NAMES_TO_HEADINGS_MAP.get(dir_name, dir_name.replace("-", " ").title())


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
    redirect_from: Optional[str] = None,
) -> str:
    """Generate Jekyll front-matter."""
    def escape_yaml_string(s: str) -> str:
        """Escape string for YAML double quotes."""
        return s.replace('\\', '\\\\').replace('"', '\\"')
    
    fm = ["---", "layout: default"]
    if title:
        fm.append(f'title: "{escape_yaml_string(title)}"')
    if parent:
        fm.append(f'parent: "{escape_yaml_string(parent)}"')
    if grand_parent:
        fm.append(f'grand_parent: "{escape_yaml_string(grand_parent)}"')
    fm.append(f"nav_order: {nav_order}")
    if has_children:
        fm.append("has_children: true")
    if redirect_from:
        fm.append(f'redirect_from: ["{escape_yaml_string(redirect_from)}"]')
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

    # Normalize anchor for Jekyll (remove dots and dashes)
    if anchor:
        anchor = re.sub(r"[.-]", "", anchor.lower())

    # Add trailing slash for directories (but not with anchors)
    if resolved_path and not resolved_path.endswith((".html", ".htm")) and not anchor:
        resolved_path = resolved_path.rstrip("/") + "/"

    return f"]({{{{site.url}}}}{{{{site.baseurl}}}}/{DOCS_BASE_PATH}/{resolved_path}{anchor})"


def process_content(content: str, current_file_path=None) -> str:
    """Process markdown content with PPL->SQL conversion, copy buttons, and link fixes."""
    # Convert PPL code fences to SQL
    content = re.sub(r'^```ppl\b.*$', '```sql', content, flags=re.MULTILINE)

    # Add copy buttons after code fences
    content = re.sub(r'^```(bash|sh|sql)\b.*?\n(.*?)^```$', 
                     r'```\1\n\2```\n{% include copy.html %}', 
                     content, flags=re.MULTILINE | re.DOTALL)

    # Convert relative links with current file context
    def fix_link_with_context(match):
        return fix_link(match, current_file_path)

    content = re.sub(
        r"\]\((?!https?://)(.*?)(\.md)?(#[^\)]*)?\)", fix_link_with_context, content
    )

    return content


def export_docs(source_dir: Path, target_dir: Path) -> None:
    """Export PPL docs to documentation website."""
    if not source_dir.exists():
        print(f"Source directory {source_dir} not found")
        return

    # Check if target directory exists and has files
    if target_dir.exists() and any(target_dir.glob('**/*.md')):
        response = input(f"Target directory {target_dir} contains files. Overwrite? (y/n): ")
        if response.lower() != 'y':
            print("Export cancelled")
            return

    # Get all markdown files sorted alphabetically
    md_files = sorted(source_dir.glob("**/*.md"))

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

    for _, files in files_by_dir.items():
        for i, md_file in enumerate(files, 1):
            rel_path = md_file.relative_to(source_dir)

            # Roll up third-level files to second level to avoid rendering limitations
            redirect_from = None
            if len(rel_path.parts) >= 3:
                # Move from admin/connectors/file.md to admin/connectors_file.md
                parent_dir = rel_path.parts[0]  # e.g., "admin"
                subdir = rel_path.parts[1]  # e.g., "connectors"
                filename = rel_path.name  # e.g., "prometheus_connector.md"
                new_filename = f"{subdir}_{filename}"
                target_file = target_dir / parent_dir / new_filename

                # Generate redirect_from for the original path
                original_path = f"/{DOCS_BASE_PATH}/{rel_path.with_suffix('')}/"
                redirect_from = original_path

                print(
                    f"\033[93mWARNING: Rolling up {rel_path} to {parent_dir}/{new_filename} due to rendering limitations\033[0m"
                )

                # Update rel_path for parent/grand_parent logic
                rel_path = Path(parent_dir) / new_filename
            else:
                target_file = target_dir / rel_path

            # Determine parent and grand_parent based on directory structure
            if rel_path.parent == Path("."):
                # Root level files
                parent = None
                grand_parent = None
            elif len(rel_path.parts) == 2:
                # Second level files (including rolled-up files)
                parent = get_heading_for_dir(rel_path.parent.name)
                grand_parent = DOCS_BASE_TITLE
            else:
                # This shouldn't happen after roll-up, but keeping for safety
                parent = get_heading_for_dir(rel_path.parent.name)
                grand_parent = get_heading_for_dir(rel_path.parts[-3])
                grand_parent = DIR_NAMES_TO_HEADINGS_MAP.get(
                    grand_parent_name, grand_parent_name.replace("-", " ").title()
                )

            # Check if this is the root index.md and has children
            is_root_index = rel_path.name == "index.md" and rel_path.parent == Path(".")
            has_children = (
                is_root_index
                or (md_file.parent / md_file.stem).is_dir()
                and any((md_file.parent / md_file.stem).glob("*/*.md"))
            )

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

        target_index = target_dir / dir_path / "index.md"
        title = get_heading_for_dir(dir_path.name)

        # Determine parent for directory index based on depth
        if len(dir_path.parts) == 1:
            # Second-level directory (e.g., admin/) - parent is root title
            parent = DOCS_BASE_TITLE
        else:
            # This shouldn't happen after filtering, but keeping for safety
            parent = get_heading_for_dir(dir_path.parent.name)

        frontmatter = generate_frontmatter(title, parent, None, has_children=True)
        target_index.write_text(frontmatter, encoding='utf-8')
        print(f"Generated: {target_index}")


if __name__ == "__main__":
    script_dir = Path(__file__).parent
    source_dir_ppl = script_dir / "../../docs/user/ppl"
    target_dir_ppl = script_dir / f"../../../documentation-website/_{DOCS_BASE_PATH}"
    export_docs(source_dir_ppl, target_dir_ppl)
