# Code Review Guidelines for OpenSearch SQL

This document provides guidelines for code reviews in the OpenSearch SQL project. These guidelines are used by CodeRabbit AI for automated code reviews and serve as a reference for human reviewers.

## Core Review Principles

### Code Quality
- **Simplicity First**: Prefer simpler solutions unless there's significant functional or performance degradation
- **Self-Documenting Code**: Code should be clear through naming and structure, not comments
- **No Redundant Comments**: Avoid comments that merely restate what the code does
- **Concise Implementation**: Keep code, docs, and notes short and focused on essentials

### Java Standards
- **Naming Conventions**: 
  - Classes: `PascalCase` (e.g., `QueryExecutor`)
  - Methods/Variables: `camelCase` (e.g., `executeQuery`)
  - Constants: `UPPER_SNAKE_CASE` (e.g., `MAX_RETRY_COUNT`)
- **Method Size**: Keep methods under 20 lines with single responsibility
- **JavaDoc Required**: All public classes and methods must have proper JavaDoc
- **Error Handling**: Use specific exception types with meaningful messages
- **Null Safety**: Prefer `Optional<T>` for nullable returns

### Testing Requirements
- **Test Coverage**: All new business logic requires unit tests
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
- **Resource Management**: Use try-with-resources for proper cleanup

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
- Methods longer than 20 lines
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

## Project-Specific Guidelines

### OpenSearch SQL Context
- **JDK 21**: Required for development
- **Java 11 Compatibility**: Maintain when possible for OpenSearch 2.x
- **Module Structure**: Respect existing module boundaries
- **Integration Tests**: Use `./gradlew :integ-test:integTest` for testing
- **Test Naming**: `*IT.java` for integration tests, `*Test.java` for unit tests

### PPL Parser Changes
- Test new grammar rules with positive and negative cases
- Verify AST generation for new syntax
- Include edge cases and boundary conditions
- Update corresponding AST builder classes

### Calcite Integration
- If the PR is for PPL command, refer docs/dev/ppl-commands.md and verify the PR satisfy the checklist.
- Follow existing patterns in `CalciteRelNodeVisitor` and `CalciteRexNodeVisitor`
- Test SQL generation and optimization paths
- Document Calcite-specific workarounds
