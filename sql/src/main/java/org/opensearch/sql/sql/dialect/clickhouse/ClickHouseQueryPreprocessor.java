/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.dialect.clickhouse;

import java.util.ArrayList;
import java.util.List;
import org.opensearch.sql.api.dialect.QueryPreprocessor;

/**
 * Strips ClickHouse-specific top-level clauses: FORMAT, SETTINGS, FINAL. Uses a lightweight
 * state-machine tokenizer that tracks:
 *
 * <ul>
 *   <li>Inside single-quoted string literal (with escaped quote handling)
 *   <li>Inside block comment ({@code /* ... *}{@code /})
 *   <li>Inside line comment ({@code -- ...})
 *   <li>Parenthesis nesting depth (to skip function args / subqueries)
 * </ul>
 *
 * <p>Only tokens at parenthesis depth 0 and outside strings/comments are candidates for stripping.
 *
 * <h3>Thread-safety (Requirement 16.2)</h3>
 *
 * This class is unconditionally thread-safe. It holds no instance fields and uses no regex
 * patterns. All tokenizer state ({@code pos}, {@code depth}, token lists) is local to the {@link
 * #preprocess} call stack, so concurrent invocations share no mutable state. No pre-compiled
 * patterns are needed because the tokenizer is a hand-written character-level state machine.
 *
 * <p>Invariant: tokens inside string literals, comments, or nested parentheses are never modified.
 */
public class ClickHouseQueryPreprocessor implements QueryPreprocessor {

  /** Token types recognized by the lightweight tokenizer. */
  enum TokenType {
    /** SQL keyword or unquoted identifier. */
    WORD,
    /** Numeric literal, e.g. {@code 42}, {@code 3.14}. */
    NUMBER,
    /** Single-quoted string literal, e.g. {@code 'hello'}. */
    STRING_LITERAL,
    /** Block comment: {@code /* ... *}{@code /}. */
    BLOCK_COMMENT,
    /** Line comment: {@code -- ...}. */
    LINE_COMMENT,
    /** Left parenthesis. */
    LPAREN,
    /** Right parenthesis. */
    RPAREN,
    /** Any other character(s): whitespace, operators, punctuation. */
    OTHER
  }

  /** A token produced by the tokenizer. */
  static final class Token {
    final TokenType type;
    final String text;
    /** Parenthesis depth at which this token was found. */
    final int depth;

    Token(TokenType type, String text, int depth) {
      this.type = type;
      this.text = text;
      this.depth = depth;
    }
  }

  @Override
  public String preprocess(String query) {
    List<Token> tokens = tokenize(query);
    List<Token> stripped = stripClauses(tokens);
    return reconstruct(stripped);
  }

  // -------------------------------------------------------------------------
  // Tokenizer: state-machine scanning
  // -------------------------------------------------------------------------

  /**
   * Tokenize the query into a list of tokens using a character-by-character state machine. Tracks
   * string literals, block comments, line comments, and parenthesis depth.
   */
  List<Token> tokenize(String query) {
    List<Token> tokens = new ArrayList<>();
    int len = query.length();
    int pos = 0;
    int depth = 0;

    while (pos < len) {
      char c = query.charAt(pos);

      // --- Single-quoted string literal ---
      if (c == '\'') {
        int start = pos;
        pos++; // skip opening quote
        while (pos < len) {
          char sc = query.charAt(pos);
          if (sc == '\\') {
            pos += 2; // skip escaped character
          } else if (sc == '\'') {
            pos++; // skip closing quote
            break;
          } else {
            pos++;
          }
        }
        tokens.add(new Token(TokenType.STRING_LITERAL, query.substring(start, pos), depth));
        continue;
      }

      // --- Block comment: /* ... */ ---
      if (c == '/' && pos + 1 < len && query.charAt(pos + 1) == '*') {
        int start = pos;
        pos += 2; // skip /*
        while (pos + 1 < len) {
          if (query.charAt(pos) == '*' && query.charAt(pos + 1) == '/') {
            pos += 2; // skip */
            break;
          }
          pos++;
        }
        // Handle unterminated block comment
        if (pos <= start + 2
            || (pos >= len
                && !(query.charAt(pos - 2) == '*' && query.charAt(pos - 1) == '/'))) {
          pos = len;
        }
        tokens.add(new Token(TokenType.BLOCK_COMMENT, query.substring(start, pos), depth));
        continue;
      }

      // --- Line comment: -- ... ---
      if (c == '-' && pos + 1 < len && query.charAt(pos + 1) == '-') {
        int start = pos;
        pos += 2; // skip --
        while (pos < len && query.charAt(pos) != '\n') {
          pos++;
        }
        tokens.add(new Token(TokenType.LINE_COMMENT, query.substring(start, pos), depth));
        continue;
      }

      // --- Parentheses ---
      if (c == '(') {
        tokens.add(new Token(TokenType.LPAREN, "(", depth));
        depth++;
        pos++;
        continue;
      }
      if (c == ')') {
        depth = Math.max(0, depth - 1);
        tokens.add(new Token(TokenType.RPAREN, ")", depth));
        pos++;
        continue;
      }

      // --- Numeric literal ---
      if (Character.isDigit(c)) {
        int start = pos;
        pos++;
        while (pos < len && (Character.isDigit(query.charAt(pos)) || query.charAt(pos) == '.')) {
          pos++;
        }
        tokens.add(new Token(TokenType.NUMBER, query.substring(start, pos), depth));
        continue;
      }

      // --- Word (keyword / identifier) ---
      if (isWordStart(c)) {
        int start = pos;
        pos++;
        while (pos < len && isWordPart(query.charAt(pos))) {
          pos++;
        }
        tokens.add(new Token(TokenType.WORD, query.substring(start, pos), depth));
        continue;
      }

      // --- Everything else (whitespace, operators, punctuation) ---
      tokens.add(new Token(TokenType.OTHER, String.valueOf(c), depth));
      pos++;
    }

    return tokens;
  }

  private static boolean isWordStart(char c) {
    return Character.isLetter(c) || c == '_';
  }

  private static boolean isWordPart(char c) {
    return Character.isLetterOrDigit(c) || c == '_' || c == '.';
  }

  // -------------------------------------------------------------------------
  // Clause stripping
  // -------------------------------------------------------------------------

  /**
   * Walk the token list and remove top-level (depth 0) FORMAT, SETTINGS, and FINAL clauses. Only
   * WORD tokens at depth 0 are considered. Tokens inside strings, comments, or nested parens are
   * left untouched.
   */
  private List<Token> stripClauses(List<Token> tokens) {
    List<Token> result = new ArrayList<>(tokens.size());
    int i = 0;

    while (i < tokens.size()) {
      Token t = tokens.get(i);

      // Only consider WORD tokens at depth 0
      if (t.type == TokenType.WORD && t.depth == 0) {
        String upper = t.text.toUpperCase();

        // --- FORMAT <identifier> ---
        if ("FORMAT".equals(upper)) {
          // Skip FORMAT keyword + optional whitespace + the format identifier
          int next = skipWhitespaceTokens(tokens, i + 1);
          if (next < tokens.size() && tokens.get(next).type == TokenType.WORD) {
            // Skip trailing whitespace after the format identifier
            i = skipWhitespaceTokens(tokens, next + 1);
            continue;
          }
          // FORMAT without a following identifier — leave it (shouldn't happen in valid queries)
        }

        // --- SETTINGS key=value[, key=value]* ---
        if ("SETTINGS".equals(upper)) {
          int end = skipSettingsClause(tokens, i + 1);
          if (end > i + 1) {
            i = end;
            continue;
          }
        }

        // --- FINAL ---
        if ("FINAL".equals(upper)) {
          // Skip the FINAL keyword and any surrounding whitespace
          i++;
          continue;
        }
      }

      result.add(t);
      i++;
    }

    return result;
  }

  /**
   * Skip past a SETTINGS clause: key=value pairs separated by commas. Returns the index of the
   * first token after the SETTINGS clause.
   */
  private int skipSettingsClause(List<Token> tokens, int start) {
    int i = skipWhitespaceTokens(tokens, start);

    // Expect at least one key=value pair
    if (!isSettingsKeyStart(tokens, i)) {
      return start; // Not a valid SETTINGS clause
    }

    while (i < tokens.size()) {
      // Skip key (may contain dots like max_memory_usage)
      i = skipWhitespaceTokens(tokens, i);
      if (!isSettingsKeyStart(tokens, i)) break;
      i++; // skip key word

      // Skip '='
      i = skipWhitespaceTokens(tokens, i);
      if (i >= tokens.size() || !isEquals(tokens.get(i))) break;
      i++; // skip '='

      // Skip value (could be a number, word, or negative number)
      i = skipWhitespaceTokens(tokens, i);
      if (i >= tokens.size()) break;
      // Handle negative values like -1
      if (tokens.get(i).type == TokenType.OTHER && tokens.get(i).text.equals("-")) {
        i++;
      }
      if (i >= tokens.size()) break;
      i++; // skip value token

      // Check for comma (more key=value pairs)
      int afterValue = skipWhitespaceTokens(tokens, i);
      if (afterValue < tokens.size()
          && tokens.get(afterValue).type == TokenType.OTHER
          && tokens.get(afterValue).text.equals(",")) {
        i = afterValue + 1; // skip comma, continue to next pair
      } else {
        i = afterValue;
        break;
      }
    }

    return i;
  }

  private boolean isSettingsKeyStart(List<Token> tokens, int i) {
    return i < tokens.size() && tokens.get(i).type == TokenType.WORD && tokens.get(i).depth == 0;
  }

  private boolean isEquals(Token t) {
    return t.type == TokenType.OTHER && t.text.equals("=");
  }

  /** Skip whitespace OTHER tokens (spaces, tabs, newlines). */
  private int skipWhitespaceTokens(List<Token> tokens, int start) {
    int i = start;
    while (i < tokens.size()
        && tokens.get(i).type == TokenType.OTHER
        && tokens.get(i).text.trim().isEmpty()) {
      i++;
    }
    return i;
  }

  // -------------------------------------------------------------------------
  // Reconstruction
  // -------------------------------------------------------------------------

  /** Reconstruct the query string from the remaining tokens. */
  private String reconstruct(List<Token> tokens) {
    StringBuilder sb = new StringBuilder();
    for (Token t : tokens) {
      sb.append(t.text);
    }
    return sb.toString().trim();
  }
}
