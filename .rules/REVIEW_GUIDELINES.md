# Code Review Guidelines for OpenSearch SQL

This document provides general code review principles and standards for the OpenSearch SQL project. These guidelines apply across all file types and are used by CodeRabbit AI for automated code reviews.

## Core Review Principles

### Code Quality
- **Simplicity First**: Prefer simpler solutions unless there's significant functional or performance degradation
- **Self-Documenting Code**: Code should be clear through naming and structure, not comments
- **No Redundant Comments**: Avoid comments that merely restate what the code does
- **Concise Implementation**: Keep code, docs, and notes short and focused on essentials
- **Code Reuse**: Identify opportunities across similar implementations
- **Maintainability**: Assess if code is easy to understand and modify

### Java Standards
- **Naming Conventions**: 
  - Classes: `PascalCase` (e.g., `QueryExecutor`)
  - Methods/Variables: `camelCase` (e.g., `executeQuery`)
  - Constants: `UPPER_SNAKE_CASE` (e.g., `MAX_RETRY_COUNT`)
- **Method Size**: 
  - Target: Under 20 lines with single responsibility
  - Suggest refactoring: Methods >100 lines
- **Class Size**: Flag classes >500 lines for organization review
- **JavaDoc Required**: All public classes and methods must have proper JavaDoc
  - NOT required on test methods, test helpers, override methods, or private methods
- **Error Handling**: Use specific exception types with meaningful messages
- **Null Safety**: Prefer `Optional<T>` for nullable returns
- **Resource Management**: Use try-with-resources for proper cleanup
- **Dead Code**: Check for unused imports, variables, and methods

### Testing Requirements
- **Test Coverage**: All new business logic requires unit tests in the same commit
- **Required Test Cases**:
  - NULL input tests for all new functions
  - Boundary condition tests (min/max values, empty inputs)
  - Error condition tests (invalid inputs, exceptions)
  - Multi-document tests for per-document operations
- **Test Naming**: Follow pattern `test<Functionality><Condition>`
- **Test Independence**: Tests must not rely on execution order
- **Test Data**: 
  - Use realistic data that reflects real-world scenarios
  - Inline test data is acceptable when it improves readability
  - Don't require loading from files if inline is clearer
- **Test Assertions**: Flag smoke tests without meaningful assertions only if they provide no value
- **Resource Cleanup**: Ensure proper cleanup of test resources
- **Integration Tests**: End-to-end scenarios need integration tests in `integ-test/` module
- **Test Execution**: Verify changes with `./gradlew :integ-test:integTest`
- **No Failing Tests**: All tests must pass before merge; fix or ask for guidance if blocked

### Code Organization
- **Single Responsibility**: Each class should have one clear purpose
- **Package Structure**: Follow existing module organization (core, ppl, sql, opensearch)
- **Separation of Concerns**: Keep parsing, execution, and storage logic separate
- **Composition Over Inheritance**: Prefer composition for code reuse

### Performance & Security
- **Efficient Loops**: Avoid unnecessary object creation in loops
- **String Handling**: Use `StringBuilder` for concatenation in loops
- **Input Validation**: Validate all user inputs, especially queries
- **Logging Safety**: Sanitize data before logging to prevent injection

## Review Focus Areas

### What to Check
1. **Code Clarity**: Is the code self-explanatory?
2. **Test Coverage**: Are there adequate tests?
3. **Error Handling**: Are errors handled appropriately?
4. **Documentation**: Is JavaDoc complete and accurate?
5. **Performance**: Are there obvious performance issues?
6. **Security**: Are inputs validated and sanitized?

### What to Flag
- Redundant or obvious comments
- Methods longer than target size (see Java Standards)
- Missing JavaDoc on public APIs
- Generic exception handling
- Unused imports or dead code
- Hard-coded values that should be constants
- Missing or inadequate test coverage

### What to Encourage
- Clear, descriptive naming
- Proper use of Java idioms
- Comprehensive test coverage
- Meaningful error messages
- Efficient algorithms and data structures
- Security-conscious coding practices

## Project Context

### OpenSearch SQL
- **JDK 21**: Required for development
- **Java 11 Compatibility**: Maintain when possible for OpenSearch 2.x
- **Module Structure**: Respect existing module boundaries (core, ppl, sql, opensearch, integ-test)
- **Test Naming**: `*IT.java` for integration tests, `*Test.java` for unit tests

### Grammar and Parser Development
- Test new grammar rules with positive and negative cases
- Verify AST generation for new syntax
- Include edge cases and boundary conditions
- Update corresponding AST builder classes
- Check for code reuse opportunities vs creating new rules

### Calcite Integration
- Follow existing patterns in visitor implementations
- Test SQL generation and optimization paths
- Document Calcite-specific workarounds
- Ensure version compatibility

## Review Tone and Focus

### Review Priorities
- **Correctness** > Performance > Maintainability > Style
- Focus on functional issues, not cosmetic ones
- Explain WHY when suggesting improvements, not just WHAT
- Avoid repeating the same suggestion multiple times

### What NOT to Flag
- Minor refactorings unless code is clearly problematic (>100 lines, deeply nested, etc.)
- Missing JavaDoc on test methods, test helpers, override methods, or private methods
- Inline test data when it improves readability
- NDJSON format as invalid JSON
- Missing language identifiers in markdown code blocks (unless in documentation files)
- Hard-coded test data when it's intentionally inline for clarity
- Helper methods in tests unless they're truly unused
- Extracting helper methods unless the method exceeds 100 lines

### Keep Reviews Concise
- Be direct and actionable
- Focus on significant issues that impact functionality or maintainability
- Avoid nitpicking on style preferences
- Prioritize issues that could cause bugs or maintenance problems
