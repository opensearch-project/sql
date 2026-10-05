/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.util;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares the {@link Capability} the annotated test method or test class requires. Enforced by
 * {@code CapabilityRule}: when the active backend lacks any listed capability the test is skipped,
 * with the capability's reason — plus the optional {@code note} — as the skip message. The note
 * applies to every capability in {@code value()}.
 *
 * <p>{@code @Inherited} so a class-level gate on a base test class (e.g. {@code SearchCommandIT})
 * also gates its subclasses (e.g. {@code CalciteSearchCommandIT}), which re-run the same tests
 * against the same fixtures and hit the same backend limitation.
 */
@Inherited
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface RequiresCapability {
  Capability[] value();

  String note() default "";
}
