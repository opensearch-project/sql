#!/usr/bin/env python3
"""
Minimal markdown exporter for OpenSearch SQL documentation.
Exports docs/user/ppl to ../documentation-website/_search-plugins/sql/
"""

import re
from pathlib import Path
from typing import Optional

def extract_title(content: str) -> Optional[str]:
    """Extract title from first H1 heading or return None."""
    match = re.search(r'^#\s+(.+)$', content, re.MULTILINE)
    return match.group(1).strip() if match else None

def generate_frontmatter(title: Optional[str], parent: Optional[str] = None, nav_order: int = 1, has_children: bool = False) -> str:
    """Generate Jekyll front-matter."""
    fm = ["---", "layout: default"]
    if title:
        fm.append(f"title: {title}")
    if parent:
        fm.append(f"parent: {parent}")
    fm.append(f"nav_order: {nav_order}")
    if has_children:
        fm.append("has_children: true")
    fm.append("---\n")
    return "\n".join(fm)

def process_file(source_file: Path, target_file: Path, parent: Optional[str] = None, nav_order: int = 1) -> None:
    """Process a single markdown file."""
    content = source_file.read_text(encoding='utf-8')
    
    # Convert PPL code fences to SQL
    content = re.sub(r'^```ppl\b.*$', '```sql', content, flags=re.MULTILINE)
    
    # Add copy buttons after code fences
    content = re.sub(r'^```(bash|sh|sql)\b.*?\n(.*?)^```$', 
                     r'```\1\n\2```\n{% include copy.html %}', 
                     content, flags=re.MULTILINE | re.DOTALL)
    
    # Remove .md extension from relative links (keep http/https links unchanged)
    content = re.sub(r'\]\((?!https?://)(.*?)\.md(#[^\)]*)?\)', r'](\1\2)', content)
    
    title = extract_title(content) or source_file.stem.replace('-', ' ').title()
    
    # Check if this directory has child markdown files in subdirectories
    has_children = any(source_file.parent.glob('*/*.md'))
    
    frontmatter = generate_frontmatter(title, parent, nav_order, has_children)
    
    # Create target directory
    target_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Write file with front-matter
    target_file.write_text(frontmatter + content, encoding='utf-8')

def export_docs() -> None:
    """Export PPL docs to documentation website."""
    source_dir = Path("../../docs/user/ppl")
    target_dir = Path("../../../documentation-website/_sql-and-ppl/ppl-reference")

    if not source_dir.exists():
        print(f"Source directory {source_dir} not found")
        return

    # Check if target directory exists and has files
    if target_dir.exists() and any(target_dir.glob('**/*.md')):
        response = input(f"Target directory {target_dir} contains files. Overwrite? (y/n): ")
        if response.lower() != 'y':
            print("Export cancelled")
            return

    # Get all markdown files
    md_files = list(source_dir.glob('**/*.md'))

    for i, md_file in enumerate(md_files, 1):
        # Calculate relative path from source
        rel_path = md_file.relative_to(source_dir)
        target_file = target_dir / rel_path

        # Determine parent based on directory structure
        parent = (
            "SQL and PPL"
            if rel_path.parent == Path(".")
            else rel_path.parent.name.replace("-", " ").title()
        )

        process_file(md_file, target_file, parent, i)
        print(f"Exported: {md_file} -> {target_file}")
    
    # Generate index.md for each directory
    dirs = set(md_file.relative_to(source_dir).parent for md_file in md_files)
    for dir_path in sorted(dirs):
        if dir_path == Path("."):
            continue
        target_index = target_dir / dir_path / "index.md"
        title = dir_path.name.replace("-", " ").title()
        parent = "Opensearch Ppl Reference Manual" if dir_path.parent == Path(".") else dir_path.parent.name.replace("-", " ").title()
        frontmatter = generate_frontmatter(title, parent, has_children=True)
        target_index.write_text(frontmatter, encoding='utf-8')
        print(f"Generated: {target_index}")

if __name__ == "__main__":
    export_docs()
