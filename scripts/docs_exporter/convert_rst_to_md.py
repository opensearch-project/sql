#!/usr/bin/env python3
"""
Convert RST PPL documentation to Markdown format.

This script converts RST files with os> prompts to Markdown with clean code fences.
"""

import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def convert_rst_table_to_markdown(table_lines: List[str]) -> Optional[str]:
    """Convert RST grid table to Markdown table."""
    # Extract rows (lines starting with |)
    rows = [line for line in table_lines if line.strip().startswith('|')]
    
    if len(rows) < 2:
        return None
    
    # Parse cells from each row
    parsed_rows = []
    for row in rows:
        # Split by | and clean up
        cells = [cell.strip() for cell in row.split('|')[1:-1]]
        parsed_rows.append(cells)
    
    # Build markdown table
    md_table = []
    
    # Header row
    md_table.append('| ' + ' | '.join(parsed_rows[0]) + ' |')
    
    # Separator row
    md_table.append('| ' + ' | '.join(['---'] * len(parsed_rows[0])) + ' |')
    
    # Data rows
    for row in parsed_rows[1:]:
        md_table.append('| ' + ' | '.join(row) + ' |')
    
    return '\n'.join(md_table)


def convert_inline_code(text: str) -> str:
    """Convert RST inline code ``code`` to Markdown `code`."""
    # Special case: ```*``` (three backticks) renders as `*` with backticks visible
    # Convert ```*``` to `` `*` `` in Markdown
    text = re.sub(r'```([^`]+)```', r'`` `\1` ``', text)
    
    # Convert regular ``code`` to `code`
    text = re.sub(r'``([^`]+)``', r'`\1`', text)
    return text


def convert_links(text: str) -> str:
    """Convert RST links to Markdown links."""
    # Convert `link text <url>`_ to [link text](url)
    # Also convert .rst to .md for internal links
    def replace_link(match):
        link_text = match.group(1).strip()
        url = match.group(2)
        # Convert .rst to .md for all links (including GitHub URLs and anchors)
        url = re.sub(r'\.rst(#|$)', r'.md\1', url)
        return f'[{link_text}]({url})'
    
    # More specific regex: match backtick, non-greedy text, space, <url>, backtick, underscore
    text = re.sub(r'`([^`<]+?)\s*<([^>]+)>`_', replace_link, text)
    
    # Convert section references `Section Name`_ to [Section Name](#section-name)
    # These are internal anchor links to headings in the same document
    def replace_section_ref(match):
        section_name = match.group(1)
        # Convert to lowercase and replace spaces with hyphens for anchor
        # Keep underscores as they are (don't convert to hyphens)
        anchor = section_name.lower().replace(' ', '-')
        # Remove special characters that aren't valid in anchors (but keep underscores and hyphens)
        anchor = re.sub(r'[^\w\-]', '', anchor)
        return f'[{section_name}](#{anchor})'
    
    text = re.sub(r'`([^`<]+)`_', replace_section_ref, text)
    
    return text


def convert_heading(line: str, next_line: Optional[str], heading_map: Dict[str, int]) -> Optional[str]:
    """Convert RST heading to Markdown.
    
    Args:
        line: The heading text
        next_line: The underline
        heading_map: Dict mapping underline chars to heading levels
    
    Returns:
        Markdown heading string or None
    """
    if not next_line:
        return None
    
    # Detect underline character
    underline_char = None
    if re.match(r'^=+$', next_line):
        underline_char = '='
    elif re.match(r'^-+$', next_line):
        underline_char = '-'
    elif re.match(r'^\^+$', next_line):
        underline_char = '^'
    elif re.match(r'^~+$', next_line):
        underline_char = '~'
    elif re.match(r'^>+$', next_line):
        underline_char = '>'
    elif re.match(r'^`+$', next_line):
        underline_char = '`'
    
    if underline_char:
        # Assign level based on first appearance (after title)
        if underline_char not in heading_map:
            # Start at H2 (H1 is reserved for title with overline)
            heading_map[underline_char] = len(heading_map) + 2
        
        level = heading_map[underline_char]
        prefix = '#' * level
        return f"{prefix} {line}\n"
    
    return None


def parse_os_block(block_content: str) -> Tuple[str, str]:
    """Parse a block with os>, PPL>, or > prompts into command and output."""
    lines = block_content.strip().split('\n')
    
    command_lines = []
    output_lines = []
    in_output = False
    
    for line in lines:
        # Handle os>, PPL>, OS>, and > prompts (case-insensitive for os/ppl)
        line_lower = line.lower()
        if line_lower.startswith('os> ') or line_lower.startswith('ppl> ') or line.startswith('> '):
            # Command line - remove prompt prefix and ; suffix
            if line_lower.startswith('os> '):
                cmd = line[4:].rstrip(';').strip()
            elif line_lower.startswith('ppl> '):
                cmd = line[5:].rstrip(';').strip()
            else:  # >
                cmd = line[2:].rstrip(';').strip()
            command_lines.append(cmd)
        elif line.strip() == '':
            # Blank line separates command from output
            if command_lines and not in_output:
                in_output = True
        else:
            # Output line
            in_output = True
            output_lines.append(line)
    
    # Format command with pipes on separate lines
    if command_lines:
        command = command_lines[0]
        # Split on | and format nicely
        if '|' in command:
            parts = [p.strip() for p in command.split('|')]
            command = parts[0] + '\n' + '\n'.join(f'| {p}' for p in parts[1:])
    else:
        command = ''
    
    return command, '\n'.join(output_lines)


def split_multiple_queries(block_content: str) -> List[str]:
    """Split a block with multiple os>/PPL>/OS> queries into separate blocks."""
    lines = block_content.strip().split('\n')
    blocks = []
    current_block = []
    
    for line in lines:
        # Check if this is a new query prompt (case-insensitive)
        line_lower = line.lower()
        if (line_lower.startswith('os> ') or line_lower.startswith('ppl> ') or line.startswith('> ')) and current_block:
            # Save the previous block
            blocks.append('\n'.join(current_block))
            current_block = [line]
        else:
            current_block.append(line)
    
    # Don't forget the last block
    if current_block:
        blocks.append('\n'.join(current_block))
    
    return blocks if len(blocks) > 1 else [block_content]


def convert_code_block_to_markdown(block_content: str) -> Optional[str]:
    """Convert RST code block to Markdown code fences."""
    # Check if there are multiple queries in this block
    query_blocks = split_multiple_queries(block_content)
    
    md_parts = []
    for query_block in query_blocks:
        command, output = parse_os_block(query_block)
        
        if not command:
            continue
        
        # Create Markdown code fences
        md = f"```ppl\n{command}\n```\n"
        
        if output:
            md += f"\nExpected output:\n\n```text\n{output}\n```"
        
        md_parts.append(md)
    
    return '\n\n'.join(md_parts) if md_parts else None


def convert_rst_to_markdown(rst_content: str) -> str:
    """Convert RST content to Markdown."""
    lines = rst_content.split('\n')
    md_lines = []
    i = 0
    skip_next = False
    heading_map: Dict[str, int] = {}  # Track underline char to heading level mapping
    
    while i < len(lines):
        if skip_next:
            skip_next = False
            i += 1
            continue
        
        line = lines[i]
        next_line = lines[i + 1] if i + 1 < len(lines) else None
        prev_line = lines[i - 1] if i > 0 else None
        
        # Check for title with overline (e.g., ====\nTitle\n====)
        if (prev_line and next_line and 
            re.match(r'^=+$', prev_line.strip()) and 
            re.match(r'^=+$', next_line.strip()) and
            len(prev_line.strip()) == len(next_line.strip())):
            # This is a title (H1) with overline
            md_lines.append(f"# {line}\n")
            skip_next = True
            i += 1
            continue
        
        # Skip RST artifacts (standalone underlines)
        if line.strip() and re.match(r'^[=\-\^~]+$', line.strip()):
            i += 1
            continue
        
        # Check for headings (underline only)
        heading = convert_heading(line, next_line, heading_map)
        if heading:
            md_lines.append(heading)
            skip_next = True
            i += 1
            continue
        
        # Check for RST grid tables (lines starting with +---+)
        if line.strip().startswith('+') and '-' in line and '+' in line:
            # Collect the entire table
            table_lines = []
            j = i
            while j < len(lines) and (lines[j].strip().startswith('+') or lines[j].strip().startswith('|')):
                table_lines.append(lines[j])
                j += 1
            
            if table_lines:
                # Convert RST table to markdown
                md_table = convert_rst_table_to_markdown(table_lines)
                if md_table:
                    md_lines.append(md_table + '\n')
                    i = j
                    continue
        
        # Check for list-table directive
        if line.strip().startswith('.. list-table::'):
            # Extract table caption if present
            caption = line.strip()[15:].strip()
            i += 1
            
            # Skip options (like :widths:, :header-rows:)
            while i < len(lines) and lines[i].strip().startswith(':'):
                i += 1
            
            # Skip blank line
            if i < len(lines) and lines[i].strip() == '':
                i += 1
            
            # Collect table rows (lines starting with * -)
            table_rows = []
            current_row = []
            
            while i < len(lines):
                line_content = lines[i]
                
                # New row starts with * -
                if line_content.strip().startswith('* -'):
                    if current_row:
                        table_rows.append(current_row)
                    current_row = [line_content.strip()[3:].strip()]
                    i += 1
                # Continuation of cell (starts with - or indented)
                elif line_content.strip().startswith('- ') and current_row:
                    current_row.append(line_content.strip()[2:].strip())
                    i += 1
                # End of table
                elif line_content.strip() == '' or not (line_content.startswith('   ') or line_content.strip().startswith('-')):
                    if current_row:
                        table_rows.append(current_row)
                    break
                else:
                    i += 1
            
            # Convert to markdown table
            if table_rows:
                if caption:
                    md_lines.append(f"{caption}\n\n")
                
                # Header row
                md_lines.append('| ' + ' | '.join(table_rows[0]) + ' |')
                md_lines.append('| ' + ' | '.join(['---'] * len(table_rows[0])) + ' |')
                
                # Data rows
                for row in table_rows[1:]:
                    md_lines.append('| ' + ' | '.join(row) + ' |')
                
                md_lines.append('\n')
            continue
        
        # Check for image directive
        if line.strip().startswith('.. image::'):
            image_url = line.strip()[10:].strip()
            # Use the URL as alt text (can be improved if there's a :alt: option)
            md_lines.append(f'![Image]({image_url})\n')
            i += 1
            # Skip any image options (like :alt:, :width:, etc.)
            while i < len(lines) and lines[i].strip().startswith(':'):
                i += 1
            continue
        
        # Check for other RST directives to skip
        if line.strip().startswith('..'):
            # Skip directive and its options
            while i < len(lines) and (lines[i].strip().startswith('..') or 
                                     lines[i].strip().startswith(':') or
                                     lines[i].strip() == ''):
                i += 1
            continue
        
        # Remove pipe prefix from description lines
        if line.startswith('| '):
            line = line[2:]
        
        # Convert links
        line = convert_links(line)
        
        # Convert inline code
        line = convert_inline_code(line)
        
        # Detect subsections (lines that look like subsection titles before code blocks)
        if (i + 1 < len(lines) and 
            not line.startswith('#') and 
            line.strip() and 
            not line.strip().startswith('*') and
            not line.strip().startswith('-') and
            lines[i + 1].strip() and
            not lines[i + 1].startswith('    ') and
            len(line) < 80 and
            (i + 2 < len(lines) and 
             (lines[i + 2].strip().startswith('The ') or 
              lines[i + 2].strip().startswith('This ')))):
            # This looks like a subsection title
            md_lines.append(f"### {line}\n")
            i += 1
            continue
        
        # Check for RST directives
        if line.strip().startswith('.. code-block::'):
            # Extract language if present
            match = re.match(r'\s*\.\. code-block::\s*(\w+)?', line)
            lang = match.group(1) if match and match.group(1) else 'text'
            
            # Look ahead for indented block
            j = i + 1
            # Skip blank line after directive
            if j < len(lines) and lines[j].strip() == '':
                j += 1
            
            block_lines = []
            while j < len(lines) and (lines[j].startswith('   ') or lines[j].strip() == ''):
                if lines[j].startswith('   '):
                    block_lines.append(lines[j][3:])
                else:
                    block_lines.append(lines[j])
                j += 1
            
            if block_lines:
                md_lines.append(f'```{lang}')
                md_lines.extend(block_lines)
                md_lines.append('```\n')
                i = j
                continue
        
        # Check for .. list-table:: directive
        if line.strip().startswith('.. list-table::'):
            # Skip the directive - tables need manual conversion
            md_lines.append('**Table:**\n')
            i += 1
            # Skip options and blank lines
            while i < len(lines) and (lines[i].strip().startswith(':') or lines[i].strip() == ''):
                i += 1
            continue
        
        # Check for .. note:: directive
        if line.strip().startswith('.. note::'):
            md_lines.append('> **Note:**')
            i += 1
            # Get the note content (indented lines)
            while i < len(lines) and (lines[i].startswith('   ') or lines[i].strip() == ''):
                if lines[i].startswith('   '):
                    md_lines.append(f'> {lines[i][3:]}')
                elif lines[i].strip():
                    md_lines.append(f'> {lines[i]}')
                else:
                    md_lines.append('>')
                i += 1
            md_lines.append('')
            continue
        
        # Check for code block marker (:: at end of line)
        if line.strip().endswith('::'):
            # Look ahead for indented block
            j = i + 1
            # Skip blank line after ::
            if j < len(lines) and lines[j].strip() == '':
                j += 1
            
            block_lines = []
            # Check for any indentation (tabs or spaces)
            while j < len(lines) and (lines[j].startswith('\t') or lines[j].startswith('    ') or 
                                     lines[j].startswith('   ') or lines[j].startswith('  ') or 
                                     lines[j].startswith(' ') or lines[j].strip() == ''):
                if lines[j].startswith('\t'):
                    block_lines.append(lines[j][1:])
                elif lines[j].startswith('    '):
                    block_lines.append(lines[j][4:])
                elif lines[j].startswith('   '):
                    block_lines.append(lines[j][3:])
                elif lines[j].startswith('  '):
                    block_lines.append(lines[j][2:])
                elif lines[j].startswith(' '):
                    block_lines.append(lines[j][1:])
                else:
                    block_lines.append(lines[j])
                j += 1
            
            if block_lines:
                block_content = '\n'.join(block_lines)
                
                # Check if it has os>, PPL>, or > prompts
                if 'os>' in block_content or 'PPL>' in block_content or block_content.strip().startswith('>'):
                    md_block = convert_code_block_to_markdown(block_content)
                    if md_block:
                        # Add the description line before :: (if not "PPL query")
                        desc_line = line.rstrip(':').strip()
                        if desc_line and desc_line.lower() not in ['ppl query', 'query']:
                            md_lines.append(desc_line + '\n')
                        md_lines.append(md_block + '\n')
                        i = j
                        continue
                else:
                    # Generic code block without prompts - wrap in markdown fence
                    desc_line = line.rstrip(':').strip()
                    if desc_line and desc_line.lower() not in ['example', 'result', 'result set']:
                        md_lines.append(desc_line + '\n')
                    md_lines.append('```bash\n' + block_content + '\n```\n')
                    i = j
                    continue
            
            # If no indented block found, just remove the ::
            md_lines.append(line.rstrip(':').strip())
            i += 1
            continue
        
        # Regular line
        if line.strip():  # Skip empty lines at the start
            md_lines.append(line)
        i += 1
    
    return '\n'.join(md_lines)


def convert_file(rst_path: Path, md_path: Path) -> None:
    """Convert a single RST file to Markdown."""
    print(f"Converting {rst_path} -> {md_path}")
    
    rst_content = rst_path.read_text(encoding='utf-8')
    
    # Convert
    md_content = convert_rst_to_markdown(rst_content)
    
    # Write output
    md_path.write_text(md_content, encoding='utf-8')
    
    print(f"  ✓ Converted successfully")


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python convert_rst_to_md.py <rst_file> [output_md_file]")
        print("   or: python convert_rst_to_md.py --batch <directory>")
        sys.exit(1)
    
    if sys.argv[1] == '--batch':
        # Batch convert all RST files in directory
        directory = Path(sys.argv[2]) if len(sys.argv) > 2 else Path('docs/user/ppl/cmd')
        
        rst_files = list(directory.glob('*.rst'))
        print(f"Found {len(rst_files)} RST files in {directory}")
        
        for rst_file in rst_files:
            md_file = rst_file.with_suffix('.md')
            try:
                convert_file(rst_file, md_file)
            except Exception as e:
                print(f"  ✗ Error: {e}")
    else:
        # Single file conversion
        rst_file = Path(sys.argv[1])
        md_file = Path(sys.argv[2]) if len(sys.argv) > 2 else rst_file.with_suffix('.md')
        
        convert_file(rst_file, md_file)


if __name__ == '__main__':
    main()
