#!/usr/bin/env python3
"""
Comprehensive markdown formatting script for docs/user/ppl/**/*.md
- Adds proper spacing before code blocks and tables for Jekyll compatibility
- Adds double spaces after headers and list items for proper line breaks
- Adds blank lines after lists end
"""

import re
from pathlib import Path
from typing import List

def fix_markdown_formatting(content: str) -> str:
    """Fix markdown formatting by adding proper spacing and line breaks."""
    lines = content.split('\n')
    fixed_lines: List[str] = []
    in_code_block = False
    in_table = False
    
    for i, line in enumerate(lines):
        # Check if current line is a code block start/end
        is_code_block_marker = line.startswith('```')
        
        # Check if current line is any table line (including separator rows)
        is_table_line = line.strip().startswith('|') and line.strip().endswith('|')
        
        # Check if it's a data row (not a separator)
        is_table_separator = re.match(r'^\s*\|[\s\-\|:]*\|\s*$', line)
        is_table_row = is_table_line and not is_table_separator
        
        # Get previous and next lines for context
        prev_line = lines[i-1] if i > 0 else ''
        next_line = lines[i+1] if i+1 < len(lines) else ''
        
        # Convert blank lines after code blocks/tables to double-space lines
        if not line.strip() and prev_line.strip():
            prev_is_code_end = prev_line.startswith('```')
            prev_is_table_end = (prev_line.strip().startswith('|') and prev_line.strip().endswith('|') and
                                not re.match(r'^\s*\|[\s\-\|:]*\|\s*$', prev_line))
            if prev_is_code_end or (prev_is_table_end and not in_table):
                line = '  '
        
        # Add spacing BEFORE code blocks and tables (check BEFORE updating in_table)
        if not in_code_block:
            # Opening code block
            if is_code_block_marker:
                if fixed_lines and fixed_lines[-1].strip():
                    fixed_lines.append('  ')
                elif fixed_lines and not fixed_lines[-1].strip():
                    fixed_lines[-1] = '  '
            
            # Starting table (first table row)
            elif is_table_row and not in_table:
                if fixed_lines and fixed_lines[-1].strip():
                    fixed_lines.append('  ')
                elif fixed_lines and not fixed_lines[-1].strip():
                    fixed_lines[-1] = '  '
        
        # Detect table start/end (AFTER spacing logic) - use is_table_line to include separators
        if not in_code_block:
            if is_table_line and not in_table:
                in_table = True
            elif not is_table_line and in_table:
                in_table = False
        
        # Process line for double spaces (only outside code blocks)
        if not in_code_block and not is_code_block_marker and line.strip():
            # Add double spaces to headers and list items if not already present
            is_header = line.startswith('#')
            is_list = (line.strip().startswith('* ') or 
                      line.strip().startswith('- ') or 
                      line.strip().startswith('+ ') or
                      re.match(r'^\s*\d+\.\s', line))
            
            if (is_header or is_list) and not line.endswith('  '):
                line = line + '  '
        
        fixed_lines.append(line)
        
        # Update code block state AFTER processing the line
        if is_code_block_marker:
            in_code_block = not in_code_block
        
        # Add spacing AFTER code blocks and tables
        if not in_code_block:
            # Closing code block
            if is_code_block_marker and next_line.strip():
                if i+1 < len(lines) and not lines[i+1].strip():
                    pass  # Will convert blank line when we reach it
                else:
                    fixed_lines.append('  ')
            
            # Ending table (last table row before non-table content)
            elif is_table_row and in_table and next_line.strip() and not (next_line.strip().startswith('|') and next_line.strip().endswith('|')):
                if i+1 < len(lines) and not lines[i+1].strip():
                    pass  # Will convert blank line when we reach it
                else:
                    fixed_lines.append('  ')
        
        # Add blank line after list ends (only outside code blocks and tables)
        if not in_code_block and not is_code_block_marker and not in_table:
            current_is_list = (line.strip().startswith('* ') or 
                              line.strip().startswith('- ') or 
                              line.strip().startswith('+ ') or
                              re.match(r'^\s*\d+\.\s', line))
            next_is_not_list = (next_line.strip() and 
                               not next_line.strip().startswith('* ') and
                               not next_line.strip().startswith('- ') and
                               not next_line.strip().startswith('+') and
                               not re.match(r'^\s*\d+\.\s', next_line) and
                               not next_line.strip().startswith(' '))  # Not indented continuation
            
            # Add blank line after list ends (with double spaces)
            if current_is_list and next_is_not_list:
                fixed_lines.append('  ')
    
    return '\n'.join(fixed_lines)

def process_file(file_path: Path) -> bool:
    """Process a single markdown file."""
    content = file_path.read_text(encoding='utf-8')
    
    fixed_content = fix_markdown_formatting(content)
    
    # Only write if content changed
    if fixed_content != content:
        file_path.write_text(fixed_content, encoding='utf-8')
        print(f"Fixed: {file_path}")
        return True
    return False

def main() -> None:
    """Fix all markdown files in docs/user/ppl/"""
    # Get the directory where this script is located
    script_dir = Path(__file__).parent
    
    # Define path relative to the script location
    source_dir = script_dir / "../../docs/user/ppl"
    
    if not source_dir.exists():
        print(f"Source directory {source_dir} not found")
        return
    
    print("Fixing markdown formatting:")
    print("- Adding double-space lines above and below code blocks")
    print("- Adding double-space lines above and below tables")
    print("- Adding double spaces after headers and list items")
    print("- Adding blank lines after lists")
    print()
    
    md_files = list(source_dir.glob('**/*.md'))
    fixed_count = 0
    
    for md_file in md_files:
        if process_file(md_file):
            fixed_count += 1
    
    print(f"\nProcessed {len(md_files)} files, fixed {fixed_count} files")

if __name__ == "__main__":
    main()
