/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.pagination;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.planner.physical.PaginateOperator;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.TableScanOperator;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class PaginatedPlanCacheTest {

  StorageEngine storageEngine;

  PaginatedPlanCache planCache;

  // encoded query 'select * from cacls' o_O
  static final String testCursor = "(Paginate,1,2,(Project,"
      + "(namedParseExpressions,),(projectList,rO0ABXNyAC1vcmcub3BlbnNlYXJjaC5zcWwuZXhwcmVzc2lvbi5"
      + "OYW1lZEV4cHJlc3Npb274hhKW/q2YQQIAA0wABWFsaWFzdAASTGphdmEvbGFuZy9TdHJpbmc7TAAJZGVsZWdhdGVk"
      + "dAAqTG9yZy9vcGVuc2VhcmNoL3NxbC9leHByZXNzaW9uL0V4cHJlc3Npb247TAAEbmFtZXEAfgABeHBwc3IAMW9yZ"
      + "y5vcGVuc2VhcmNoLnNxbC5leHByZXNzaW9uLlJlZmVyZW5jZUV4cHJlc3Npb274AO0rxWvMkAIAA0wABGF0dHJxAH"
      + "4AAUwABXBhdGhzdAAQTGphdmEvdXRpbC9MaXN0O0wABHR5cGV0ACdMb3JnL29wZW5zZWFyY2gvc3FsL2RhdGEvdHl"
      + "wZS9FeHByVHlwZTt4cHQABWJvb2wzc3IAGmphdmEudXRpbC5BcnJheXMkQXJyYXlMaXN02aQ8vs2IBtICAAFbAAFh"
      + "dAATW0xqYXZhL2xhbmcvT2JqZWN0O3hwdXIAE1tMamF2YS5sYW5nLlN0cmluZzut0lbn6R17RwIAAHhwAAAAAXEAf"
      + "gAIfnIAKW9yZy5vcGVuc2VhcmNoLnNxbC5kYXRhLnR5cGUuRXhwckNvcmVUeXBlAAAAAAAAAAASAAB4cgAOamF2YS"
      + "5sYW5nLkVudW0AAAAAAAAAABIAAHhwdAAHQk9PTEVBTnEAfgAI,rO0ABXNyAC1vcmcub3BlbnNlYXJjaC5zcWwuZX"
      + "hwcmVzc2lvbi5OYW1lZEV4cHJlc3Npb274hhKW/q2YQQIAA0wABWFsaWFzdAASTGphdmEvbGFuZy9TdHJpbmc7TAA"
      + "JZGVsZWdhdGVkdAAqTG9yZy9vcGVuc2VhcmNoL3NxbC9leHByZXNzaW9uL0V4cHJlc3Npb247TAAEbmFtZXEAfgAB"
      + "eHBwc3IAMW9yZy5vcGVuc2VhcmNoLnNxbC5leHByZXNzaW9uLlJlZmVyZW5jZUV4cHJlc3Npb274AO0rxWvMkAIAA"
      + "0wABGF0dHJxAH4AAUwABXBhdGhzdAAQTGphdmEvdXRpbC9MaXN0O0wABHR5cGV0ACdMb3JnL29wZW5zZWFyY2gvc3"
      + "FsL2RhdGEvdHlwZS9FeHByVHlwZTt4cHQABGludDBzcgAaamF2YS51dGlsLkFycmF5cyRBcnJheUxpc3TZpDy+zYg"
      + "G0gIAAVsAAWF0ABNbTGphdmEvbGFuZy9PYmplY3Q7eHB1cgATW0xqYXZhLmxhbmcuU3RyaW5nO63SVufpHXtHAgAA"
      + "eHAAAAABcQB+AAh+cgApb3JnLm9wZW5zZWFyY2guc3FsLmRhdGEudHlwZS5FeHByQ29yZVR5cGUAAAAAAAAAABIAA"
      + "HhyAA5qYXZhLmxhbmcuRW51bQAAAAAAAAAAEgAAeHB0AAdJTlRFR0VScQB+AAg=,rO0ABXNyAC1vcmcub3BlbnNlY"
      + "XJjaC5zcWwuZXhwcmVzc2lvbi5OYW1lZEV4cHJlc3Npb274hhKW/q2YQQIAA0wABWFsaWFzdAASTGphdmEvbGFuZy"
      + "9TdHJpbmc7TAAJZGVsZWdhdGVkdAAqTG9yZy9vcGVuc2VhcmNoL3NxbC9leHByZXNzaW9uL0V4cHJlc3Npb247TAA"
      + "EbmFtZXEAfgABeHBwc3IAMW9yZy5vcGVuc2VhcmNoLnNxbC5leHByZXNzaW9uLlJlZmVyZW5jZUV4cHJlc3Npb274"
      + "AO0rxWvMkAIAA0wABGF0dHJxAH4AAUwABXBhdGhzdAAQTGphdmEvdXRpbC9MaXN0O0wABHR5cGV0ACdMb3JnL29wZ"
      + "W5zZWFyY2gvc3FsL2RhdGEvdHlwZS9FeHByVHlwZTt4cHQABXRpbWUxc3IAGmphdmEudXRpbC5BcnJheXMkQXJyYX"
      + "lMaXN02aQ8vs2IBtICAAFbAAFhdAATW0xqYXZhL2xhbmcvT2JqZWN0O3hwdXIAE1tMamF2YS5sYW5nLlN0cmluZzu"
      + "t0lbn6R17RwIAAHhwAAAAAXEAfgAIfnIAKW9yZy5vcGVuc2VhcmNoLnNxbC5kYXRhLnR5cGUuRXhwckNvcmVUeXBl"
      + "AAAAAAAAAAASAAB4cgAOamF2YS5sYW5nLkVudW0AAAAAAAAAABIAAHhwdAAJVElNRVNUQU1QcQB+AAg=,rO0ABXNy"
      + "AC1vcmcub3BlbnNlYXJjaC5zcWwuZXhwcmVzc2lvbi5OYW1lZEV4cHJlc3Npb274hhKW/q2YQQIAA0wABWFsaWFzd"
      + "AASTGphdmEvbGFuZy9TdHJpbmc7TAAJZGVsZWdhdGVkdAAqTG9yZy9vcGVuc2VhcmNoL3NxbC9leHByZXNzaW9uL0"
      + "V4cHJlc3Npb247TAAEbmFtZXEAfgABeHBwc3IAMW9yZy5vcGVuc2VhcmNoLnNxbC5leHByZXNzaW9uLlJlZmVyZW5"
      + "jZUV4cHJlc3Npb274AO0rxWvMkAIAA0wABGF0dHJxAH4AAUwABXBhdGhzdAAQTGphdmEvdXRpbC9MaXN0O0wABHR5"
      + "cGV0ACdMb3JnL29wZW5zZWFyY2gvc3FsL2RhdGEvdHlwZS9FeHByVHlwZTt4cHQABWJvb2wyc3IAGmphdmEudXRpb"
      + "C5BcnJheXMkQXJyYXlMaXN02aQ8vs2IBtICAAFbAAFhdAATW0xqYXZhL2xhbmcvT2JqZWN0O3hwdXIAE1tMamF2YS"
      + "5sYW5nLlN0cmluZzut0lbn6R17RwIAAHhwAAAAAXEAfgAIfnIAKW9yZy5vcGVuc2VhcmNoLnNxbC5kYXRhLnR5cGU"
      + "uRXhwckNvcmVUeXBlAAAAAAAAAAASAAB4cgAOamF2YS5sYW5nLkVudW0AAAAAAAAAABIAAHhwdAAHQk9PTEVBTnEA"
      + "fgAI,rO0ABXNyAC1vcmcub3BlbnNlYXJjaC5zcWwuZXhwcmVzc2lvbi5OYW1lZEV4cHJlc3Npb274hhKW/q2YQQIA"
      + "A0wABWFsaWFzdAASTGphdmEvbGFuZy9TdHJpbmc7TAAJZGVsZWdhdGVkdAAqTG9yZy9vcGVuc2VhcmNoL3NxbC9le"
      + "HByZXNzaW9uL0V4cHJlc3Npb247TAAEbmFtZXEAfgABeHBwc3IAMW9yZy5vcGVuc2VhcmNoLnNxbC5leHByZXNzaW"
      + "9uLlJlZmVyZW5jZUV4cHJlc3Npb274AO0rxWvMkAIAA0wABGF0dHJxAH4AAUwABXBhdGhzdAAQTGphdmEvdXRpbC9"
      + "MaXN0O0wABHR5cGV0ACdMb3JnL29wZW5zZWFyY2gvc3FsL2RhdGEvdHlwZS9FeHByVHlwZTt4cHQABGludDJzcgAa"
      + "amF2YS51dGlsLkFycmF5cyRBcnJheUxpc3TZpDy+zYgG0gIAAVsAAWF0ABNbTGphdmEvbGFuZy9PYmplY3Q7eHB1c"
      + "gATW0xqYXZhLmxhbmcuU3RyaW5nO63SVufpHXtHAgAAeHAAAAABcQB+AAh+cgApb3JnLm9wZW5zZWFyY2guc3FsLm"
      + "RhdGEudHlwZS5FeHByQ29yZVR5cGUAAAAAAAAAABIAAHhyAA5qYXZhLmxhbmcuRW51bQAAAAAAAAAAEgAAeHB0AAd"
      + "JTlRFR0VScQB+AAg=,rO0ABXNyAC1vcmcub3BlbnNlYXJjaC5zcWwuZXhwcmVzc2lvbi5OYW1lZEV4cHJlc3Npb27"
      + "4hhKW/q2YQQIAA0wABWFsaWFzdAASTGphdmEvbGFuZy9TdHJpbmc7TAAJZGVsZWdhdGVkdAAqTG9yZy9vcGVuc2Vh"
      + "cmNoL3NxbC9leHByZXNzaW9uL0V4cHJlc3Npb247TAAEbmFtZXEAfgABeHBwc3IAMW9yZy5vcGVuc2VhcmNoLnNxb"
      + "C5leHByZXNzaW9uLlJlZmVyZW5jZUV4cHJlc3Npb274AO0rxWvMkAIAA0wABGF0dHJxAH4AAUwABXBhdGhzdAAQTG"
      + "phdmEvdXRpbC9MaXN0O0wABHR5cGV0ACdMb3JnL29wZW5zZWFyY2gvc3FsL2RhdGEvdHlwZS9FeHByVHlwZTt4cHQ"
      + "ABGludDFzcgAaamF2YS51dGlsLkFycmF5cyRBcnJheUxpc3TZpDy+zYgG0gIAAVsAAWF0ABNbTGphdmEvbGFuZy9P"
      + "YmplY3Q7eHB1cgATW0xqYXZhLmxhbmcuU3RyaW5nO63SVufpHXtHAgAAeHAAAAABcQB+AAh+cgApb3JnLm9wZW5zZ"
      + "WFyY2guc3FsLmRhdGEudHlwZS5FeHByQ29yZVR5cGUAAAAAAAAAABIAAHhyAA5qYXZhLmxhbmcuRW51bQAAAAAAAA"
      + "AAEgAAeHB0AAdJTlRFR0VScQB+AAg=,rO0ABXNyAC1vcmcub3BlbnNlYXJjaC5zcWwuZXhwcmVzc2lvbi5OYW1lZE"
      + "V4cHJlc3Npb274hhKW/q2YQQIAA0wABWFsaWFzdAASTGphdmEvbGFuZy9TdHJpbmc7TAAJZGVsZWdhdGVkdAAqTG9"
      + "yZy9vcGVuc2VhcmNoL3NxbC9leHByZXNzaW9uL0V4cHJlc3Npb247TAAEbmFtZXEAfgABeHBwc3IAMW9yZy5vcGVu"
      + "c2VhcmNoLnNxbC5leHByZXNzaW9uLlJlZmVyZW5jZUV4cHJlc3Npb274AO0rxWvMkAIAA0wABGF0dHJxAH4AAUwAB"
      + "XBhdGhzdAAQTGphdmEvdXRpbC9MaXN0O0wABHR5cGV0ACdMb3JnL29wZW5zZWFyY2gvc3FsL2RhdGEvdHlwZS9FeH"
      + "ByVHlwZTt4cHQABHN0cjNzcgAaamF2YS51dGlsLkFycmF5cyRBcnJheUxpc3TZpDy+zYgG0gIAAVsAAWF0ABNbTGp"
      + "hdmEvbGFuZy9PYmplY3Q7eHB1cgATW0xqYXZhLmxhbmcuU3RyaW5nO63SVufpHXtHAgAAeHAAAAABcQB+AAh+cgAp"
      + "b3JnLm9wZW5zZWFyY2guc3FsLmRhdGEudHlwZS5FeHByQ29yZVR5cGUAAAAAAAAAABIAAHhyAA5qYXZhLmxhbmcuR"
      + "W51bQAAAAAAAAAAEgAAeHB0AAZTVFJJTkdxAH4ACA==,rO0ABXNyAC1vcmcub3BlbnNlYXJjaC5zcWwuZXhwcmVzc"
      + "2lvbi5OYW1lZEV4cHJlc3Npb274hhKW/q2YQQIAA0wABWFsaWFzdAASTGphdmEvbGFuZy9TdHJpbmc7TAAJZGVsZW"
      + "dhdGVkdAAqTG9yZy9vcGVuc2VhcmNoL3NxbC9leHByZXNzaW9uL0V4cHJlc3Npb247TAAEbmFtZXEAfgABeHBwc3I"
      + "AMW9yZy5vcGVuc2VhcmNoLnNxbC5leHByZXNzaW9uLlJlZmVyZW5jZUV4cHJlc3Npb274AO0rxWvMkAIAA0wABGF0"
      + "dHJxAH4AAUwABXBhdGhzdAAQTGphdmEvdXRpbC9MaXN0O0wABHR5cGV0ACdMb3JnL29wZW5zZWFyY2gvc3FsL2Rhd"
      + "GEvdHlwZS9FeHByVHlwZTt4cHQABGludDNzcgAaamF2YS51dGlsLkFycmF5cyRBcnJheUxpc3TZpDy+zYgG0gIAAV"
      + "sAAWF0ABNbTGphdmEvbGFuZy9PYmplY3Q7eHB1cgATW0xqYXZhLmxhbmcuU3RyaW5nO63SVufpHXtHAgAAeHAAAAA"
      + "BcQB+AAh+cgApb3JnLm9wZW5zZWFyY2guc3FsLmRhdGEudHlwZS5FeHByQ29yZVR5cGUAAAAAAAAAABIAAHhyAA5q"
      + "YXZhLmxhbmcuRW51bQAAAAAAAAAAEgAAeHB0AAdJTlRFR0VScQB+AAg=,rO0ABXNyAC1vcmcub3BlbnNlYXJjaC5z"
      + "cWwuZXhwcmVzc2lvbi5OYW1lZEV4cHJlc3Npb274hhKW/q2YQQIAA0wABWFsaWFzdAASTGphdmEvbGFuZy9TdHJpb"
      + "mc7TAAJZGVsZWdhdGVkdAAqTG9yZy9vcGVuc2VhcmNoL3NxbC9leHByZXNzaW9uL0V4cHJlc3Npb247TAAEbmFtZX"
      + "EAfgABeHBwc3IAMW9yZy5vcGVuc2VhcmNoLnNxbC5leHByZXNzaW9uLlJlZmVyZW5jZUV4cHJlc3Npb274AO0rxWv"
      + "MkAIAA0wABGF0dHJxAH4AAUwABXBhdGhzdAAQTGphdmEvdXRpbC9MaXN0O0wABHR5cGV0ACdMb3JnL29wZW5zZWFy"
      + "Y2gvc3FsL2RhdGEvdHlwZS9FeHByVHlwZTt4cHQABHN0cjFzcgAaamF2YS51dGlsLkFycmF5cyRBcnJheUxpc3TZp"
      + "Dy+zYgG0gIAAVsAAWF0ABNbTGphdmEvbGFuZy9PYmplY3Q7eHB1cgATW0xqYXZhLmxhbmcuU3RyaW5nO63SVufpHX"
      + "tHAgAAeHAAAAABcQB+AAh+cgApb3JnLm9wZW5zZWFyY2guc3FsLmRhdGEudHlwZS5FeHByQ29yZVR5cGUAAAAAAAA"
      + "AABIAAHhyAA5qYXZhLmxhbmcuRW51bQAAAAAAAAAAEgAAeHB0AAZTVFJJTkdxAH4ACA==,rO0ABXNyAC1vcmcub3B"
      + "lbnNlYXJjaC5zcWwuZXhwcmVzc2lvbi5OYW1lZEV4cHJlc3Npb274hhKW/q2YQQIAA0wABWFsaWFzdAASTGphdmEv"
      + "bGFuZy9TdHJpbmc7TAAJZGVsZWdhdGVkdAAqTG9yZy9vcGVuc2VhcmNoL3NxbC9leHByZXNzaW9uL0V4cHJlc3Npb"
      + "247TAAEbmFtZXEAfgABeHBwc3IAMW9yZy5vcGVuc2VhcmNoLnNxbC5leHByZXNzaW9uLlJlZmVyZW5jZUV4cHJlc3"
      + "Npb274AO0rxWvMkAIAA0wABGF0dHJxAH4AAUwABXBhdGhzdAAQTGphdmEvdXRpbC9MaXN0O0wABHR5cGV0ACdMb3J"
      + "nL29wZW5zZWFyY2gvc3FsL2RhdGEvdHlwZS9FeHByVHlwZTt4cHQABHN0cjJzcgAaamF2YS51dGlsLkFycmF5cyRB"
      + "cnJheUxpc3TZpDy+zYgG0gIAAVsAAWF0ABNbTGphdmEvbGFuZy9PYmplY3Q7eHB1cgATW0xqYXZhLmxhbmcuU3Rya"
      + "W5nO63SVufpHXtHAgAAeHAAAAABcQB+AAh+cgApb3JnLm9wZW5zZWFyY2guc3FsLmRhdGEudHlwZS5FeHByQ29yZV"
      + "R5cGUAAAAAAAAAABIAAHhyAA5qYXZhLmxhbmcuRW51bQAAAAAAAAAAEgAAeHB0AAZTVFJJTkdxAH4ACA==,rO0ABX"
      + "NyAC1vcmcub3BlbnNlYXJjaC5zcWwuZXhwcmVzc2lvbi5OYW1lZEV4cHJlc3Npb274hhKW/q2YQQIAA0wABWFsaWF"
      + "zdAASTGphdmEvbGFuZy9TdHJpbmc7TAAJZGVsZWdhdGVkdAAqTG9yZy9vcGVuc2VhcmNoL3NxbC9leHByZXNzaW9u"
      + "L0V4cHJlc3Npb247TAAEbmFtZXEAfgABeHBwc3IAMW9yZy5vcGVuc2VhcmNoLnNxbC5leHByZXNzaW9uLlJlZmVyZ"
      + "W5jZUV4cHJlc3Npb274AO0rxWvMkAIAA0wABGF0dHJxAH4AAUwABXBhdGhzdAAQTGphdmEvdXRpbC9MaXN0O0wABH"
      + "R5cGV0ACdMb3JnL29wZW5zZWFyY2gvc3FsL2RhdGEvdHlwZS9FeHByVHlwZTt4cHQABXRpbWUwc3IAGmphdmEudXR"
      + "pbC5BcnJheXMkQXJyYXlMaXN02aQ8vs2IBtICAAFbAAFhdAATW0xqYXZhL2xhbmcvT2JqZWN0O3hwdXIAE1tMamF2"
      + "YS5sYW5nLlN0cmluZzut0lbn6R17RwIAAHhwAAAAAXEAfgAIfnIAKW9yZy5vcGVuc2VhcmNoLnNxbC5kYXRhLnR5c"
      + "GUuRXhwckNvcmVUeXBlAAAAAAAAAAASAAB4cgAOamF2YS5sYW5nLkVudW0AAAAAAAAAABIAAHhwdAAJVElNRVNUQU"
      + "1QcQB+AAg=,rO0ABXNyAC1vcmcub3BlbnNlYXJjaC5zcWwuZXhwcmVzc2lvbi5OYW1lZEV4cHJlc3Npb274hhKW/q"
      + "2YQQIAA0wABWFsaWFzdAASTGphdmEvbGFuZy9TdHJpbmc7TAAJZGVsZWdhdGVkdAAqTG9yZy9vcGVuc2VhcmNoL3N"
      + "xbC9leHByZXNzaW9uL0V4cHJlc3Npb247TAAEbmFtZXEAfgABeHBwc3IAMW9yZy5vcGVuc2VhcmNoLnNxbC5leHBy"
      + "ZXNzaW9uLlJlZmVyZW5jZUV4cHJlc3Npb274AO0rxWvMkAIAA0wABGF0dHJxAH4AAUwABXBhdGhzdAAQTGphdmEvd"
      + "XRpbC9MaXN0O0wABHR5cGV0ACdMb3JnL29wZW5zZWFyY2gvc3FsL2RhdGEvdHlwZS9FeHByVHlwZTt4cHQACWRhdG"
      + "V0aW1lMHNyABpqYXZhLnV0aWwuQXJyYXlzJEFycmF5TGlzdNmkPL7NiAbSAgABWwABYXQAE1tMamF2YS9sYW5nL09"
      + "iamVjdDt4cHVyABNbTGphdmEubGFuZy5TdHJpbmc7rdJW5+kde0cCAAB4cAAAAAFxAH4ACH5yAClvcmcub3BlbnNl"
      + "YXJjaC5zcWwuZGF0YS50eXBlLkV4cHJDb3JlVHlwZQAAAAAAAAAAEgAAeHIADmphdmEubGFuZy5FbnVtAAAAAAAAA"
      + "AASAAB4cHQACVRJTUVTVEFNUHEAfgAI,rO0ABXNyAC1vcmcub3BlbnNlYXJjaC5zcWwuZXhwcmVzc2lvbi5OYW1lZ"
      + "EV4cHJlc3Npb274hhKW/q2YQQIAA0wABWFsaWFzdAASTGphdmEvbGFuZy9TdHJpbmc7TAAJZGVsZWdhdGVkdAAqTG"
      + "9yZy9vcGVuc2VhcmNoL3NxbC9leHByZXNzaW9uL0V4cHJlc3Npb247TAAEbmFtZXEAfgABeHBwc3IAMW9yZy5vcGV"
      + "uc2VhcmNoLnNxbC5leHByZXNzaW9uLlJlZmVyZW5jZUV4cHJlc3Npb274AO0rxWvMkAIAA0wABGF0dHJxAH4AAUwA"
      + "BXBhdGhzdAAQTGphdmEvdXRpbC9MaXN0O0wABHR5cGV0ACdMb3JnL29wZW5zZWFyY2gvc3FsL2RhdGEvdHlwZS9Fe"
      + "HByVHlwZTt4cHQABG51bTFzcgAaamF2YS51dGlsLkFycmF5cyRBcnJheUxpc3TZpDy+zYgG0gIAAVsAAWF0ABNbTG"
      + "phdmEvbGFuZy9PYmplY3Q7eHB1cgATW0xqYXZhLmxhbmcuU3RyaW5nO63SVufpHXtHAgAAeHAAAAABcQB+AAh+cgA"
      + "pb3JnLm9wZW5zZWFyY2guc3FsLmRhdGEudHlwZS5FeHByQ29yZVR5cGUAAAAAAAAAABIAAHhyAA5qYXZhLmxhbmcu"
      + "RW51bQAAAAAAAAAAEgAAeHB0AAZET1VCTEVxAH4ACA==,rO0ABXNyAC1vcmcub3BlbnNlYXJjaC5zcWwuZXhwcmVz"
      + "c2lvbi5OYW1lZEV4cHJlc3Npb274hhKW/q2YQQIAA0wABWFsaWFzdAASTGphdmEvbGFuZy9TdHJpbmc7TAAJZGVsZ"
      + "WdhdGVkdAAqTG9yZy9vcGVuc2VhcmNoL3NxbC9leHByZXNzaW9uL0V4cHJlc3Npb247TAAEbmFtZXEAfgABeHBwc3"
      + "IAMW9yZy5vcGVuc2VhcmNoLnNxbC5leHByZXNzaW9uLlJlZmVyZW5jZUV4cHJlc3Npb274AO0rxWvMkAIAA0wABGF"
      + "0dHJxAH4AAUwABXBhdGhzdAAQTGphdmEvdXRpbC9MaXN0O0wABHR5cGV0ACdMb3JnL29wZW5zZWFyY2gvc3FsL2Rh"
      + "dGEvdHlwZS9FeHByVHlwZTt4cHQABG51bTBzcgAaamF2YS51dGlsLkFycmF5cyRBcnJheUxpc3TZpDy+zYgG0gIAA"
      + "VsAAWF0ABNbTGphdmEvbGFuZy9PYmplY3Q7eHB1cgATW0xqYXZhLmxhbmcuU3RyaW5nO63SVufpHXtHAgAAeHAAAA"
      + "ABcQB+AAh+cgApb3JnLm9wZW5zZWFyY2guc3FsLmRhdGEudHlwZS5FeHByQ29yZVR5cGUAAAAAAAAAABIAAHhyAA5"
      + "qYXZhLmxhbmcuRW51bQAAAAAAAAAAEgAAeHB0AAZET1VCTEVxAH4ACA==,rO0ABXNyAC1vcmcub3BlbnNlYXJjaC5"
      + "zcWwuZXhwcmVzc2lvbi5OYW1lZEV4cHJlc3Npb274hhKW/q2YQQIAA0wABWFsaWFzdAASTGphdmEvbGFuZy9TdHJp"
      + "bmc7TAAJZGVsZWdhdGVkdAAqTG9yZy9vcGVuc2VhcmNoL3NxbC9leHByZXNzaW9uL0V4cHJlc3Npb247TAAEbmFtZ"
      + "XEAfgABeHBwc3IAMW9yZy5vcGVuc2VhcmNoLnNxbC5leHByZXNzaW9uLlJlZmVyZW5jZUV4cHJlc3Npb274AO0rxW"
      + "vMkAIAA0wABGF0dHJxAH4AAUwABXBhdGhzdAAQTGphdmEvdXRpbC9MaXN0O0wABHR5cGV0ACdMb3JnL29wZW5zZWF"
      + "yY2gvc3FsL2RhdGEvdHlwZS9FeHByVHlwZTt4cHQACWRhdGV0aW1lMXNyABpqYXZhLnV0aWwuQXJyYXlzJEFycmF5"
      + "TGlzdNmkPL7NiAbSAgABWwABYXQAE1tMamF2YS9sYW5nL09iamVjdDt4cHVyABNbTGphdmEubGFuZy5TdHJpbmc7r"
      + "dJW5+kde0cCAAB4cAAAAAFxAH4ACH5yAClvcmcub3BlbnNlYXJjaC5zcWwuZGF0YS50eXBlLkV4cHJDb3JlVHlwZQ"
      + "AAAAAAAAAAEgAAeHIADmphdmEubGFuZy5FbnVtAAAAAAAAAAASAAB4cHQACVRJTUVTVEFNUHEAfgAI,rO0ABXNyAC"
      + "1vcmcub3BlbnNlYXJjaC5zcWwuZXhwcmVzc2lvbi5OYW1lZEV4cHJlc3Npb274hhKW/q2YQQIAA0wABWFsaWFzdAA"
      + "STGphdmEvbGFuZy9TdHJpbmc7TAAJZGVsZWdhdGVkdAAqTG9yZy9vcGVuc2VhcmNoL3NxbC9leHByZXNzaW9uL0V4"
      + "cHJlc3Npb247TAAEbmFtZXEAfgABeHBwc3IAMW9yZy5vcGVuc2VhcmNoLnNxbC5leHByZXNzaW9uLlJlZmVyZW5jZ"
      + "UV4cHJlc3Npb274AO0rxWvMkAIAA0wABGF0dHJxAH4AAUwABXBhdGhzdAAQTGphdmEvdXRpbC9MaXN0O0wABHR5cG"
      + "V0ACdMb3JnL29wZW5zZWFyY2gvc3FsL2RhdGEvdHlwZS9FeHByVHlwZTt4cHQABG51bTRzcgAaamF2YS51dGlsLkF"
      + "ycmF5cyRBcnJheUxpc3TZpDy+zYgG0gIAAVsAAWF0ABNbTGphdmEvbGFuZy9PYmplY3Q7eHB1cgATW0xqYXZhLmxh"
      + "bmcuU3RyaW5nO63SVufpHXtHAgAAeHAAAAABcQB+AAh+cgApb3JnLm9wZW5zZWFyY2guc3FsLmRhdGEudHlwZS5Fe"
      + "HByQ29yZVR5cGUAAAAAAAAAABIAAHhyAA5qYXZhLmxhbmcuRW51bQAAAAAAAAAAEgAAeHB0AAZET1VCTEVxAH4ACA"
      + "==,rO0ABXNyAC1vcmcub3BlbnNlYXJjaC5zcWwuZXhwcmVzc2lvbi5OYW1lZEV4cHJlc3Npb274hhKW/q2YQQIAA0"
      + "wABWFsaWFzdAASTGphdmEvbGFuZy9TdHJpbmc7TAAJZGVsZWdhdGVkdAAqTG9yZy9vcGVuc2VhcmNoL3NxbC9leHB"
      + "yZXNzaW9uL0V4cHJlc3Npb247TAAEbmFtZXEAfgABeHBwc3IAMW9yZy5vcGVuc2VhcmNoLnNxbC5leHByZXNzaW9u"
      + "LlJlZmVyZW5jZUV4cHJlc3Npb274AO0rxWvMkAIAA0wABGF0dHJxAH4AAUwABXBhdGhzdAAQTGphdmEvdXRpbC9Ma"
      + "XN0O0wABHR5cGV0ACdMb3JnL29wZW5zZWFyY2gvc3FsL2RhdGEvdHlwZS9FeHByVHlwZTt4cHQABWJvb2wxc3IAGm"
      + "phdmEudXRpbC5BcnJheXMkQXJyYXlMaXN02aQ8vs2IBtICAAFbAAFhdAATW0xqYXZhL2xhbmcvT2JqZWN0O3hwdXI"
      + "AE1tMamF2YS5sYW5nLlN0cmluZzut0lbn6R17RwIAAHhwAAAAAXEAfgAIfnIAKW9yZy5vcGVuc2VhcmNoLnNxbC5k"
      + "YXRhLnR5cGUuRXhwckNvcmVUeXBlAAAAAAAAAAASAAB4cgAOamF2YS5sYW5nLkVudW0AAAAAAAAAABIAAHhwdAAHQ"
      + "k9PTEVBTnEAfgAI,rO0ABXNyAC1vcmcub3BlbnNlYXJjaC5zcWwuZXhwcmVzc2lvbi5OYW1lZEV4cHJlc3Npb274h"
      + "hKW/q2YQQIAA0wABWFsaWFzdAASTGphdmEvbGFuZy9TdHJpbmc7TAAJZGVsZWdhdGVkdAAqTG9yZy9vcGVuc2Vhcm"
      + "NoL3NxbC9leHByZXNzaW9uL0V4cHJlc3Npb247TAAEbmFtZXEAfgABeHBwc3IAMW9yZy5vcGVuc2VhcmNoLnNxbC5"
      + "leHByZXNzaW9uLlJlZmVyZW5jZUV4cHJlc3Npb274AO0rxWvMkAIAA0wABGF0dHJxAH4AAUwABXBhdGhzdAAQTGph"
      + "dmEvdXRpbC9MaXN0O0wABHR5cGV0ACdMb3JnL29wZW5zZWFyY2gvc3FsL2RhdGEvdHlwZS9FeHByVHlwZTt4cHQAA"
      + "2tleXNyABpqYXZhLnV0aWwuQXJyYXlzJEFycmF5TGlzdNmkPL7NiAbSAgABWwABYXQAE1tMamF2YS9sYW5nL09iam"
      + "VjdDt4cHVyABNbTGphdmEubGFuZy5TdHJpbmc7rdJW5+kde0cCAAB4cAAAAAFxAH4ACH5yAClvcmcub3BlbnNlYXJ"
      + "jaC5zcWwuZGF0YS50eXBlLkV4cHJDb3JlVHlwZQAAAAAAAAAAEgAAeHIADmphdmEubGFuZy5FbnVtAAAAAAAAAAAS"
      + "AAB4cHQABlNUUklOR3EAfgAI,rO0ABXNyAC1vcmcub3BlbnNlYXJjaC5zcWwuZXhwcmVzc2lvbi5OYW1lZEV4cHJl"
      + "c3Npb274hhKW/q2YQQIAA0wABWFsaWFzdAASTGphdmEvbGFuZy9TdHJpbmc7TAAJZGVsZWdhdGVkdAAqTG9yZy9vc"
      + "GVuc2VhcmNoL3NxbC9leHByZXNzaW9uL0V4cHJlc3Npb247TAAEbmFtZXEAfgABeHBwc3IAMW9yZy5vcGVuc2Vhcm"
      + "NoLnNxbC5leHByZXNzaW9uLlJlZmVyZW5jZUV4cHJlc3Npb274AO0rxWvMkAIAA0wABGF0dHJxAH4AAUwABXBhdGh"
      + "zdAAQTGphdmEvdXRpbC9MaXN0O0wABHR5cGV0ACdMb3JnL29wZW5zZWFyY2gvc3FsL2RhdGEvdHlwZS9FeHByVHlw"
      + "ZTt4cHQABG51bTNzcgAaamF2YS51dGlsLkFycmF5cyRBcnJheUxpc3TZpDy+zYgG0gIAAVsAAWF0ABNbTGphdmEvb"
      + "GFuZy9PYmplY3Q7eHB1cgATW0xqYXZhLmxhbmcuU3RyaW5nO63SVufpHXtHAgAAeHAAAAABcQB+AAh+cgApb3JnLm"
      + "9wZW5zZWFyY2guc3FsLmRhdGEudHlwZS5FeHByQ29yZVR5cGUAAAAAAAAAABIAAHhyAA5qYXZhLmxhbmcuRW51bQA"
      + "AAAAAAAAAEgAAeHB0AAZET1VCTEVxAH4ACA==,rO0ABXNyAC1vcmcub3BlbnNlYXJjaC5zcWwuZXhwcmVzc2lvbi5"
      + "OYW1lZEV4cHJlc3Npb274hhKW/q2YQQIAA0wABWFsaWFzdAASTGphdmEvbGFuZy9TdHJpbmc7TAAJZGVsZWdhdGVk"
      + "dAAqTG9yZy9vcGVuc2VhcmNoL3NxbC9leHByZXNzaW9uL0V4cHJlc3Npb247TAAEbmFtZXEAfgABeHBwc3IAMW9yZ"
      + "y5vcGVuc2VhcmNoLnNxbC5leHByZXNzaW9uLlJlZmVyZW5jZUV4cHJlc3Npb274AO0rxWvMkAIAA0wABGF0dHJxAH"
      + "4AAUwABXBhdGhzdAAQTGphdmEvdXRpbC9MaXN0O0wABHR5cGV0ACdMb3JnL29wZW5zZWFyY2gvc3FsL2RhdGEvdHl"
      + "wZS9FeHByVHlwZTt4cHQABWJvb2wwc3IAGmphdmEudXRpbC5BcnJheXMkQXJyYXlMaXN02aQ8vs2IBtICAAFbAAFh"
      + "dAATW0xqYXZhL2xhbmcvT2JqZWN0O3hwdXIAE1tMamF2YS5sYW5nLlN0cmluZzut0lbn6R17RwIAAHhwAAAAAXEAf"
      + "gAIfnIAKW9yZy5vcGVuc2VhcmNoLnNxbC5kYXRhLnR5cGUuRXhwckNvcmVUeXBlAAAAAAAAAAASAAB4cgAOamF2YS"
      + "5sYW5nLkVudW0AAAAAAAAAABIAAHhwdAAHQk9PTEVBTnEAfgAI,rO0ABXNyAC1vcmcub3BlbnNlYXJjaC5zcWwuZX"
      + "hwcmVzc2lvbi5OYW1lZEV4cHJlc3Npb274hhKW/q2YQQIAA0wABWFsaWFzdAASTGphdmEvbGFuZy9TdHJpbmc7TAA"
      + "JZGVsZWdhdGVkdAAqTG9yZy9vcGVuc2VhcmNoL3NxbC9leHByZXNzaW9uL0V4cHJlc3Npb247TAAEbmFtZXEAfgAB"
      + "eHBwc3IAMW9yZy5vcGVuc2VhcmNoLnNxbC5leHByZXNzaW9uLlJlZmVyZW5jZUV4cHJlc3Npb274AO0rxWvMkAIAA"
      + "0wABGF0dHJxAH4AAUwABXBhdGhzdAAQTGphdmEvdXRpbC9MaXN0O0wABHR5cGV0ACdMb3JnL29wZW5zZWFyY2gvc3"
      + "FsL2RhdGEvdHlwZS9FeHByVHlwZTt4cHQABG51bTJzcgAaamF2YS51dGlsLkFycmF5cyRBcnJheUxpc3TZpDy+zYg"
      + "G0gIAAVsAAWF0ABNbTGphdmEvbGFuZy9PYmplY3Q7eHB1cgATW0xqYXZhLmxhbmcuU3RyaW5nO63SVufpHXtHAgAA"
      + "eHAAAAABcQB+AAh+cgApb3JnLm9wZW5zZWFyY2guc3FsLmRhdGEudHlwZS5FeHByQ29yZVR5cGUAAAAAAAAAABIAA"
      + "HhyAA5qYXZhLmxhbmcuRW51bQAAAAAAAAAAEgAAeHB0AAZET1VCTEVxAH4ACA==,rO0ABXNyAC1vcmcub3BlbnNlY"
      + "XJjaC5zcWwuZXhwcmVzc2lvbi5OYW1lZEV4cHJlc3Npb274hhKW/q2YQQIAA0wABWFsaWFzdAASTGphdmEvbGFuZy"
      + "9TdHJpbmc7TAAJZGVsZWdhdGVkdAAqTG9yZy9vcGVuc2VhcmNoL3NxbC9leHByZXNzaW9uL0V4cHJlc3Npb247TAA"
      + "EbmFtZXEAfgABeHBwc3IAMW9yZy5vcGVuc2VhcmNoLnNxbC5leHByZXNzaW9uLlJlZmVyZW5jZUV4cHJlc3Npb274"
      + "AO0rxWvMkAIAA0wABGF0dHJxAH4AAUwABXBhdGhzdAAQTGphdmEvdXRpbC9MaXN0O0wABHR5cGV0ACdMb3JnL29wZ"
      + "W5zZWFyY2gvc3FsL2RhdGEvdHlwZS9FeHByVHlwZTt4cHQABHN0cjBzcgAaamF2YS51dGlsLkFycmF5cyRBcnJheU"
      + "xpc3TZpDy+zYgG0gIAAVsAAWF0ABNbTGphdmEvbGFuZy9PYmplY3Q7eHB1cgATW0xqYXZhLmxhbmcuU3RyaW5nO63"
      + "SVufpHXtHAgAAeHAAAAABcQB+AAh+cgApb3JnLm9wZW5zZWFyY2guc3FsLmRhdGEudHlwZS5FeHByQ29yZVR5cGUA"
      + "AAAAAAAAABIAAHhyAA5qYXZhLmxhbmcuRW51bQAAAAAAAAAAEgAAeHB0AAZTVFJJTkdxAH4ACA==,rO0ABXNyAC1v"
      + "cmcub3BlbnNlYXJjaC5zcWwuZXhwcmVzc2lvbi5OYW1lZEV4cHJlc3Npb274hhKW/q2YQQIAA0wABWFsaWFzdAAST"
      + "GphdmEvbGFuZy9TdHJpbmc7TAAJZGVsZWdhdGVkdAAqTG9yZy9vcGVuc2VhcmNoL3NxbC9leHByZXNzaW9uL0V4cH"
      + "Jlc3Npb247TAAEbmFtZXEAfgABeHBwc3IAMW9yZy5vcGVuc2VhcmNoLnNxbC5leHByZXNzaW9uLlJlZmVyZW5jZUV"
      + "4cHJlc3Npb274AO0rxWvMkAIAA0wABGF0dHJxAH4AAUwABXBhdGhzdAAQTGphdmEvdXRpbC9MaXN0O0wABHR5cGV0"
      + "ACdMb3JnL29wZW5zZWFyY2gvc3FsL2RhdGEvdHlwZS9FeHByVHlwZTt4cHQABWRhdGUzc3IAGmphdmEudXRpbC5Bc"
      + "nJheXMkQXJyYXlMaXN02aQ8vs2IBtICAAFbAAFhdAATW0xqYXZhL2xhbmcvT2JqZWN0O3hwdXIAE1tMamF2YS5sYW"
      + "5nLlN0cmluZzut0lbn6R17RwIAAHhwAAAAAXEAfgAIfnIAKW9yZy5vcGVuc2VhcmNoLnNxbC5kYXRhLnR5cGUuRXh"
      + "wckNvcmVUeXBlAAAAAAAAAAASAAB4cgAOamF2YS5sYW5nLkVudW0AAAAAAAAAABIAAHhwdAAJVElNRVNUQU1QcQB+"
      + "AAg=,rO0ABXNyAC1vcmcub3BlbnNlYXJjaC5zcWwuZXhwcmVzc2lvbi5OYW1lZEV4cHJlc3Npb274hhKW/q2YQQIA"
      + "A0wABWFsaWFzdAASTGphdmEvbGFuZy9TdHJpbmc7TAAJZGVsZWdhdGVkdAAqTG9yZy9vcGVuc2VhcmNoL3NxbC9le"
      + "HByZXNzaW9uL0V4cHJlc3Npb247TAAEbmFtZXEAfgABeHBwc3IAMW9yZy5vcGVuc2VhcmNoLnNxbC5leHByZXNzaW"
      + "9uLlJlZmVyZW5jZUV4cHJlc3Npb274AO0rxWvMkAIAA0wABGF0dHJxAH4AAUwABXBhdGhzdAAQTGphdmEvdXRpbC9"
      + "MaXN0O0wABHR5cGV0ACdMb3JnL29wZW5zZWFyY2gvc3FsL2RhdGEvdHlwZS9FeHByVHlwZTt4cHQABWRhdGUyc3IA"
      + "GmphdmEudXRpbC5BcnJheXMkQXJyYXlMaXN02aQ8vs2IBtICAAFbAAFhdAATW0xqYXZhL2xhbmcvT2JqZWN0O3hwd"
      + "XIAE1tMamF2YS5sYW5nLlN0cmluZzut0lbn6R17RwIAAHhwAAAAAXEAfgAIfnIAKW9yZy5vcGVuc2VhcmNoLnNxbC"
      + "5kYXRhLnR5cGUuRXhwckNvcmVUeXBlAAAAAAAAAAASAAB4cgAOamF2YS5sYW5nLkVudW0AAAAAAAAAABIAAHhwdAA"
      + "JVElNRVNUQU1QcQB+AAg=,rO0ABXNyAC1vcmcub3BlbnNlYXJjaC5zcWwuZXhwcmVzc2lvbi5OYW1lZEV4cHJlc3N"
      + "pb274hhKW/q2YQQIAA0wABWFsaWFzdAASTGphdmEvbGFuZy9TdHJpbmc7TAAJZGVsZWdhdGVkdAAqTG9yZy9vcGVu"
      + "c2VhcmNoL3NxbC9leHByZXNzaW9uL0V4cHJlc3Npb247TAAEbmFtZXEAfgABeHBwc3IAMW9yZy5vcGVuc2VhcmNoL"
      + "nNxbC5leHByZXNzaW9uLlJlZmVyZW5jZUV4cHJlc3Npb274AO0rxWvMkAIAA0wABGF0dHJxAH4AAUwABXBhdGhzdA"
      + "AQTGphdmEvdXRpbC9MaXN0O0wABHR5cGV0ACdMb3JnL29wZW5zZWFyY2gvc3FsL2RhdGEvdHlwZS9FeHByVHlwZTt"
      + "4cHQABWRhdGUxc3IAGmphdmEudXRpbC5BcnJheXMkQXJyYXlMaXN02aQ8vs2IBtICAAFbAAFhdAATW0xqYXZhL2xh"
      + "bmcvT2JqZWN0O3hwdXIAE1tMamF2YS5sYW5nLlN0cmluZzut0lbn6R17RwIAAHhwAAAAAXEAfgAIfnIAKW9yZy5vc"
      + "GVuc2VhcmNoLnNxbC5kYXRhLnR5cGUuRXhwckNvcmVUeXBlAAAAAAAAAAASAAB4cgAOamF2YS5sYW5nLkVudW0AAA"
      + "AAAAAAABIAAHhwdAAJVElNRVNUQU1QcQB+AAg=,rO0ABXNyAC1vcmcub3BlbnNlYXJjaC5zcWwuZXhwcmVzc2lvbi"
      + "5OYW1lZEV4cHJlc3Npb274hhKW/q2YQQIAA0wABWFsaWFzdAASTGphdmEvbGFuZy9TdHJpbmc7TAAJZGVsZWdhdGV"
      + "kdAAqTG9yZy9vcGVuc2VhcmNoL3NxbC9leHByZXNzaW9uL0V4cHJlc3Npb247TAAEbmFtZXEAfgABeHBwc3IAMW9y"
      + "Zy5vcGVuc2VhcmNoLnNxbC5leHByZXNzaW9uLlJlZmVyZW5jZUV4cHJlc3Npb274AO0rxWvMkAIAA0wABGF0dHJxA"
      + "H4AAUwABXBhdGhzdAAQTGphdmEvdXRpbC9MaXN0O0wABHR5cGV0ACdMb3JnL29wZW5zZWFyY2gvc3FsL2RhdGEvdH"
      + "lwZS9FeHByVHlwZTt4cHQABWRhdGUwc3IAGmphdmEudXRpbC5BcnJheXMkQXJyYXlMaXN02aQ8vs2IBtICAAFbAAF"
      + "hdAATW0xqYXZhL2xhbmcvT2JqZWN0O3hwdXIAE1tMamF2YS5sYW5nLlN0cmluZzut0lbn6R17RwIAAHhwAAAAAXEA"
      + "fgAIfnIAKW9yZy5vcGVuc2VhcmNoLnNxbC5kYXRhLnR5cGUuRXhwckNvcmVUeXBlAAAAAAAAAAASAAB4cgAOamF2Y"
      + "S5sYW5nLkVudW0AAAAAAAAAABIAAHhwdAAJVElNRVNUQU1QcQB+AAg=,rO0ABXNyAC1vcmcub3BlbnNlYXJjaC5zc"
      + "WwuZXhwcmVzc2lvbi5OYW1lZEV4cHJlc3Npb274hhKW/q2YQQIAA0wABWFsaWFzdAASTGphdmEvbGFuZy9TdHJpbm"
      + "c7TAAJZGVsZWdhdGVkdAAqTG9yZy9vcGVuc2VhcmNoL3NxbC9leHByZXNzaW9uL0V4cHJlc3Npb247TAAEbmFtZXE"
      + "AfgABeHBwc3IAMW9yZy5vcGVuc2VhcmNoLnNxbC5leHByZXNzaW9uLlJlZmVyZW5jZUV4cHJlc3Npb274AO0rxWvM"
      + "kAIAA0wABGF0dHJxAH4AAUwABXBhdGhzdAAQTGphdmEvdXRpbC9MaXN0O0wABHR5cGV0ACdMb3JnL29wZW5zZWFyY"
      + "2gvc3FsL2RhdGEvdHlwZS9FeHByVHlwZTt4cHQAA3p6enNyABpqYXZhLnV0aWwuQXJyYXlzJEFycmF5TGlzdNmkPL"
      + "7NiAbSAgABWwABYXQAE1tMamF2YS9sYW5nL09iamVjdDt4cHVyABNbTGphdmEubGFuZy5TdHJpbmc7rdJW5+kde0c"
      + "CAAB4cAAAAAFxAH4ACH5yAClvcmcub3BlbnNlYXJjaC5zcWwuZGF0YS50eXBlLkV4cHJDb3JlVHlwZQAAAAAAAAAA"
      + "EgAAeHIADmphdmEubGFuZy5FbnVtAAAAAAAAAAASAAB4cHQABlNUUklOR3EAfgAI),(OpenSearchPagedIndexSc"
      + "an,calcs,FGluY2x1ZGVfY29udGV4dF91dWlkDXF1ZXJ5QW5kRmV0Y2gBFndYQmJZcHpxU3dtc1hUVkhhYU1uLVEA"
      + "AAAAAAAADRY4RzRudHZqbFI0dTBFdkJNZEpCaDd3)))";

  private static final String testIndexName = "dummyIndex";
  private static final String testScroll = "dummyScroll";

  @BeforeEach
  void setUp() {
    storageEngine = mock(StorageEngine.class);
    when(storageEngine.getTableScan(anyString(), anyString()))
        .thenReturn(new MockedTableScanOperator());
    planCache = new PaginatedPlanCache(storageEngine);
  }

  @Test
  void canConvertToCursor_relation() {
    assertTrue(planCache.canConvertToCursor(AstDSL.relation("Table")));
  }

  @Test
  void canConvertToCursor_project_allFields_relation() {
    var unresolvedPlan = AstDSL.project(AstDSL.relation("table"), AstDSL.allFields());
    assertTrue(planCache.canConvertToCursor(unresolvedPlan));
  }

  @Test
  void canConvertToCursor_project_some_fields_relation() {
    var unresolvedPlan = AstDSL.project(AstDSL.relation("table"), AstDSL.field("rando"));
    Assertions.assertFalse(planCache.canConvertToCursor(unresolvedPlan));
  }

  @ParameterizedTest
  @ValueSource(strings = {"pewpew", "asdkfhashdfjkgakgfwuigfaijkb", testCursor})
  void compress_decompress(String input) {
    var compressed = compress(input);
    assertEquals(input, decompress(compressed));
    if (input.length() > 200) {
      // Compression of short strings isn't profitable, because encoding into string and gzip
      // headers add more bytes than input string has.
      assertTrue(compressed.length() < input.length());
    }
  }

  @Test
  // should never happen actually, at least for compress
  void compress_decompress_null_or_empty_string() {
    assertAll(
        () -> assertTrue(compress(null).isEmpty()),
        () -> assertTrue(compress("").isEmpty()),
        () -> assertTrue(decompress(null).isEmpty()),
        () -> assertTrue(decompress("").isEmpty())
    );
  }

  @Test
  // test added for coverage only
  void compress_throws() {
    var mock = Mockito.mockConstructionWithAnswer(GZIPOutputStream.class, invocation -> null);
    assertThrows(Throwable.class, () -> compress("\\_(`v`)_/"));
    mock.close();
  }

  @Test
  void decompress_throws() {
    assertAll(
        // from gzip - damaged header
        () -> assertThrows(Throwable.class, () -> decompress("00")),
        // from HashCode::fromString
        () -> assertThrows(Throwable.class, () -> decompress("000"))
    );
  }

  @Test
  @SneakyThrows
  void convert_deconvert_cursor() {
    var cursor = buildCursor(Map.of());
    var plan = planCache.convertToPlan(cursor);
    // `PaginateOperator::toCursor` shifts cursor to the next page. To have this test consistent
    // we have to enforce it staying on the same page. This allows us to get same cursor strings.
    var pageNum = (int)FieldUtils.readField(plan, "pageIndex", true);
    FieldUtils.writeField(plan, "pageIndex", pageNum - 1, true);
    var convertedCursor = planCache.convertToCursor(plan).toString();
    // Then we have to restore page num into the plan, otherwise comparison would fail due to this.
    FieldUtils.writeField(plan, "pageIndex", pageNum, true);
    var convertedPlan = planCache.convertToPlan(convertedCursor);
    assertEquals(cursor, convertedCursor);
    // TODO compare plans
  }

  @Test
  @SneakyThrows
  void convertToCursor_cant_convert() {
    var plan = mock(MockedTableScanOperator.class);
    assertEquals(Cursor.None, planCache.convertToCursor(plan));
    when(plan.toCursor()).thenReturn("");
    assertEquals(Cursor.None, planCache.convertToCursor(
        new PaginateOperator(plan, 1, 2)));
  }

  @Test
  void converted_plan_is_executable() {
    // planCache.convertToPlan(buildCursor(Map.of()));
    var plan = planCache.convertToPlan("n:" + compress(testCursor));
    // TODO
  }

  @ParameterizedTest
  @MethodSource("generateIncorrectCursors")
  void throws_on_parsing_damaged_cursor(String cursor) {
    assertThrows(Throwable.class, () -> planCache.convertToPlan(cursor));
  }

  private static Stream<Arguments> generateIncorrectCursors() {
    return Stream.of(
        compress(testCursor), // a valid cursor, but without "n:" prefix
        "n:" + testCursor, // a valid, but uncompressed cursor
        buildCursor(Map.of("prefix", "g:")), // incorrect prefix
        buildCursor(Map.of("header: paginate", "ORDER BY")), // incorrect header
        buildCursor(Map.of("pageIndex", "")), // incorrect page #
        buildCursor(Map.of("pageIndex", "abc")), // incorrect page #
        buildCursor(Map.of("pageSize", "abc")), // incorrect page size
        buildCursor(Map.of("pageSize", "null")), // incorrect page size
        buildCursor(Map.of("pageSize", "10 ")), // incorrect page size
        buildCursor(Map.of("header: project", "")), // incorrect header
        buildCursor(Map.of("header: namedParseExpressions", "ololo")), // incorrect header
        buildCursor(Map.of("namedParseExpressions", "pewpew")), // incorrect (unparsable) npes
        buildCursor(Map.of("namedParseExpressions", "rO0ABXA=,")), // incorrect npes (extra comma)
        buildCursor(Map.of("header: projectList", "")), // incorrect header
        buildCursor(Map.of("projectList", "\0\0\0\0")), // incorrect project
        buildCursor(Map.of("header: OpenSearchPagedIndexScan", "42")) // incorrect header
    ).map(Arguments::of);
  }


  /**
   * Function puts default valid values into generated cursor string.
   * Values could be redefined.
   * @param values A map of non-default values to use.
   * @return A compressed cursor string.
   */
  public static String buildCursor(Map<String, String> values) {
    String prefix = values.getOrDefault("prefix", "n:");
    String headerPaginate = values.getOrDefault("header: paginate", "Paginate");
    String pageIndex = values.getOrDefault("pageIndex", "1");
    String pageSize = values.getOrDefault("pageSize", "2");
    String headerProject = values.getOrDefault("header: project", "Project");
    String headerNpes = values.getOrDefault("header: namedParseExpressions",
        "namedParseExpressions");
    String namedParseExpressions = values.getOrDefault("namedParseExpressions", "");
    String headerProjectList = values.getOrDefault("header: projectList", "projectList");
    String projectList = values.getOrDefault("projectList", "rO0ABXA="); // serialized `null`
    String headerOspis = values.getOrDefault("header: OpenSearchPagedIndexScan",
        "OpenSearchPagedIndexScan");
    String indexName = values.getOrDefault("indexName", testIndexName);
    String scrollId = values.getOrDefault("scrollId", testScroll);
    var cursor = String.format("(%s,%s,%s,(%s,(%s,%s),(%s,%s),(%s,%s,%s)))", headerPaginate,
        pageIndex, pageSize, headerProject, headerNpes, namedParseExpressions, headerProjectList,
        projectList, headerOspis, indexName, scrollId);
    return prefix + compress(cursor);
  }

  private static class MockedTableScanOperator extends TableScanOperator {
    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public ExprValue next() {
      return null;
    }

    @Override
    public String explain() {
      return null;
    }

    @Override
    public String toCursor() {
      return createSection("OpenSearchPagedIndexScan", testIndexName, testScroll);
    }
  }

  @SneakyThrows
  private static String compress(String input) {
    return new PaginatedPlanCache(null).compress(input);
  }

  @SneakyThrows
  private static String decompress(String input) {
    return new PaginatedPlanCache(null).decompress(input);
  }
}
