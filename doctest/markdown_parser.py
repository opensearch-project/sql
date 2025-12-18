"""
Markdown-based doctest parser for clean copy-paste documentation.

Parses Markdown code fences instead of RST directives.
"""

import inspect
import re
from pathlib import Path
from typing import Callable, List, Optional, Tuple, Union
import doctest


class MarkdownDocTestParser:
    """
    Parses Markdown files looking for paired code blocks:
    1. Input code block (sql, ppl, sh, bash, bash ppl)
    2. Output code block (text, console, json, yaml, or output)

    Example Markdown format:

        ```sql
        SELECT * FROM accounts
        ```

        ```text
        +------+
        | name |
        +------+
        | John |
        +------+
        ```
    """

    # Regex to match Markdown code fences with optional attributes
    CODE_FENCE_PATTERN = re.compile(
        r'^```(\w+)([^\n]*?)\s*\n'   # ```language [attributes] (no newlines in attributes)
        r'(.*?)'                     # code content (non-greedy)
        r'^```\s*$',                 # closing ```
        re.MULTILINE | re.DOTALL
    )

    def __init__(self, input_languages: Optional[List[str]] = None, 
                 output_languages: Optional[List[str]] = None, 
                 transform: Optional[Callable] = None) -> None:
        """
        Args:
            input_languages: List of languages for input blocks (e.g., ['sql', 'ppl'])
            output_languages: List of languages for output blocks (e.g., ['text', 'console'])
            transform: Function to transform input code before execution
        """
        self.input_languages = input_languages or ['sql', 'ppl', 'bash', 'sh', 'bash ppl']
        self.output_languages = output_languages or ['text', 'console', 'output', 'json', 'yaml']
        self.transform = transform or (lambda x: x)

    def parse(self, text: str, name: str = '<string>') -> doctest.DocTest:
        """
        Parse Markdown text and extract test cases from code fence pairs.
        
        Returns a DocTest object compatible with doctest.DocTestRunner.
        """
        examples = []
        blocks = self._extract_code_blocks(text)

        # Find pairs of input/output blocks
        i = 0
        while i < len(blocks) - 1:
            lang1, code1, lineno1 = blocks[i]
            lang2, code2, lineno2 = blocks[i + 1]

            # Check if this is an input/output pair
            if lang1 in self.input_languages and lang2 in self.output_languages:
                # Create a doctest example
                source = code1.rstrip('\n')
                want = code2.rstrip('\n') + '\n'  # doctest expects trailing newline

                # Apply transform to source
                if callable(self.transform):
                    # Check if transform accepts language parameter
                    sig = inspect.signature(self.transform)
                    if len(sig.parameters) > 1:
                        transformed_source = self.transform(source, lang1)
                    else:
                        transformed_source = self.transform(source)
                else:
                    transformed_source = source

                example = doctest.Example(
                    source=transformed_source,
                    want=want,
                    lineno=lineno1,
                    indent=0,
                    options={}
                )
                examples.append(example)

                # Skip the output block since we've paired it
                i += 2
            else:
                # Not a pair, move to next block
                i += 1

        return doctest.DocTest(
            examples=examples,
            globs={},
            name=name,
            filename=name,
            lineno=0,
            docstring=text
        )

    def get_doctest(self, docstring: str, globs: dict, name: str, filename: str, lineno: int) -> doctest.DocTest:
        """
        Extract a DocTest object from the given docstring.
        This method is required for compatibility with DocFileSuite.
        """
        # Read the file content
        content = Path(filename).read_text(encoding='utf-8')

        # Parse the markdown content and update globs
        doctest_obj = self.parse(content, name=filename)
        doctest_obj.globs.update(globs)
        return doctest_obj

    def _extract_code_blocks(self, text: str) -> List[Tuple[str, str, int]]:
        """
        Extract all code blocks from Markdown text, skipping those with 'ignore' attribute.
        
        Returns list of (language, code, line_number) tuples.
        """
        blocks = []
        for match in self.CODE_FENCE_PATTERN.finditer(text):
            language = match.group(1).lower()
            attributes = match.group(2) or ""
            code = match.group(3)
            lineno = text[:match.start()].count('\n') + 1

            # Skip blocks with 'ignore' attribute
            if "ignore" in attributes:
                continue

            blocks.append((language, code, lineno))

        return blocks


def create_markdown_suite(filepath: Union[str, Path], transform: Optional[Callable] = None, 
                         setup: Optional[Callable] = None, globs: Optional[dict] = None) -> doctest.DocTestSuite:
    """
    Create a test suite from a Markdown file.
    
    Args:
        filepath: Path to Markdown file
        transform: Function to transform input code
        setup: Setup function to run before tests
        globs: Global variables for test execution
    
    Returns:
        doctest.DocTestSuite
    """
    parser = MarkdownDocTestParser(transform=transform)
    
    content = Path(filepath).read_text(encoding='utf-8')
    
    doctest_obj = parser.parse(content, name=str(filepath))
    
    # Set up globs if provided
    if globs:
        doctest_obj.globs.update(globs)
    
    # Create a test case
    test = doctest.DocTestCase(
        doctest_obj,
        optionflags=doctest.NORMALIZE_WHITESPACE | doctest.ELLIPSIS,
        setUp=setup
    )
    
    return doctest.DocTestSuite(test_finder=lambda: [doctest_obj])


# Transform functions for different languages
def sql_markdown_transform(code: str, lang: str = "sql") -> str:
    """Transform SQL code for execution."""
    return f'sql_cmd.process({repr(code.strip().rstrip(";"))})'


def ppl_markdown_transform(code: str, lang: str = "ppl") -> str:
    """Transform PPL code for execution."""
    # Join multi-line PPL queries into a single line
    # Remove leading/trailing whitespace and join lines with space
    single_line = " ".join(
        line.strip() for line in code.strip().split("\n") if line.strip()
    )
    return f'ppl_cmd.process({repr(single_line.rstrip(";"))})'


def bash_markdown_transform(code: str, lang: str = "bash") -> str:
    """Transform bash code for execution."""
    if code.strip().startswith("opensearchsql"):
        match = re.search(r'opensearchsql\s+-q\s+"(.*?)"', code)
        if match:
            query = match.group(1)
            return f'cmd.process({repr(query.strip().rstrip(";"))})'
    return f'pretty_print(sh("""{code}""").stdout.decode("utf-8"))'


def bash_ppl_markdown_transform(code: str, lang: str = "bash ppl") -> str:
    """Transform bash ppl code for execution (curl commands with PPL queries)."""
    return f'pretty_print(sh("""{code}""").stdout.decode("utf-8"))'


def mixed_ppl_transform(code: str, lang: str = "ppl") -> str:
    """Mixed transform that handles both ppl and bash ppl."""
    if lang == "bash ppl" or "curl" in code.lower():
        return bash_ppl_markdown_transform(code, lang)
    else:
        return ppl_markdown_transform(code, lang)


def detect_markdown_format(filepath: Union[str, Path]) -> bool:
    """
    Check if a file uses Markdown code fences.
    
    Returns:
        True if file uses ```language``` code fences
        False otherwise
    """
    content = Path(filepath).read_text(encoding='utf-8')
    
    # Check for Markdown code fences
    return bool(re.search(r'^```\w+\s*\n', content, re.MULTILINE))


def create_hybrid_markdown_suite(filepaths: List[Union[str, Path]], doc_type: str, 
                               setup_func: Optional[Callable] = None) -> doctest.DocTestSuite:
    """
    Create test suite for Markdown files.
    
    Args:
        filepaths: List of Markdown file paths
        doc_type: 'sql', 'ppl', or 'bash'
        setup_func: Setup function to initialize test environment
    
    Returns:
        doctest.DocTestSuite
    """
    # Choose transform based on doc type
    if 'sql' in doc_type:
        transform = sql_markdown_transform
        input_langs = ['sql']
    elif 'ppl' in doc_type:
        transform = ppl_markdown_transform
        input_langs = ['ppl']
    else:  # bash
        transform = bash_markdown_transform
        input_langs = ['bash', 'sh']
    
    parser = MarkdownDocTestParser(
        input_languages=input_langs,
        output_languages=['text', 'console', 'output'],
        transform=transform
    )
    
    all_tests = []
    
    for filepath in filepaths:
        content = Path(filepath).read_text(encoding='utf-8')
        
        doctest_obj = parser.parse(content, name=str(filepath))
        
        # Only add if there are examples
        if doctest_obj.examples:
            all_tests.append(doctest_obj)
    
    # Create test suite
    def setUp(test):
        if setup_func:
            setup_func(test)
    
    suite = doctest.DocTestSuite(
        test_finder=lambda: all_tests,
        setUp=setUp,
        optionflags=doctest.NORMALIZE_WHITESPACE | doctest.ELLIPSIS
    )
    
    return suite
