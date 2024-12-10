/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic.scope;

import static java.util.Collections.emptyNavigableMap;

import java.util.EnumMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.opensearch.sql.legacy.antlr.semantic.types.Type;

/** Symbol table for symbol definition and resolution. */
public class SymbolTable {

  /** Two-dimension hash table to manage symbols with type in different namespace */
  private final Map<Namespace, NavigableMap<String, TypeSupplier>> tableByNamespace =
      new EnumMap<>(Namespace.class);

  /**
   * Store symbol with the type. Create new map for namespace for the first time.
   *
   * @param symbol symbol to define
   * @param type symbol type
   */
  public void store(Symbol symbol, Type type) {
    tableByNamespace
        .computeIfAbsent(symbol.getNamespace(), ns -> new TreeMap<>())
        .computeIfAbsent(symbol.getName(), symbolName -> new TypeSupplier(symbolName, type))
        .add(type);
  }

  /**
   * Look up symbol in the namespace map.
   *
   * @param symbol symbol to look up
   * @return symbol type which is optional
   */
  public Optional<Type> lookup(Symbol symbol) {
    Map<String, TypeSupplier> table = tableByNamespace.get(symbol.getNamespace());
    TypeSupplier typeSupplier = null;
    if (table != null) {
      typeSupplier = table.get(symbol.getName());
    }
    return Optional.ofNullable(typeSupplier).map(TypeSupplier::get);
  }

  /**
   * Look up symbols by a prefix.
   *
   * @param prefix a symbol prefix
   * @return symbols starting with the prefix
   */
  public Map<String, Type> lookupByPrefix(Symbol prefix) {
    NavigableMap<String, TypeSupplier> table = tableByNamespace.get(prefix.getNamespace());
    if (table != null) {
      return table
          .subMap(prefix.getName(), prefix.getName() + Character.MAX_VALUE)
          .entrySet()
          .stream()
          .filter(entry -> null != entry.getValue().get())
          .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()));
    }
    return Map.of();
  }

  /**
   * Look up all symbols in the namespace.
   *
   * @param namespace a namespace
   * @return all symbols in the namespace map
   */
  public Map<String, Type> lookupAll(Namespace namespace) {
    return tableByNamespace.getOrDefault(namespace, emptyNavigableMap()).entrySet().stream()
        .filter(entry -> null != entry.getValue().get())
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()));
  }

  /**
   * Check if namespace map in empty (none definition)
   *
   * @param namespace a namespace
   * @return true for empty
   */
  public boolean isEmpty(Namespace namespace) {
    return tableByNamespace.getOrDefault(namespace, emptyNavigableMap()).isEmpty();
  }
}
