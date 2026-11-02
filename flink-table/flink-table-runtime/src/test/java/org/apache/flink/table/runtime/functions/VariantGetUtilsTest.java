/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.functions;

import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.runtime.functions.VariantGetUtils.ParsedPath;
import org.apache.flink.types.variant.BinaryVariantBuilder;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.types.variant.VariantBuilder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link VariantGetUtils}. */
class VariantGetUtilsTest {

    private final VariantBuilder builder = new BinaryVariantBuilder();

    @Test
    void testRootPath() {
        final Variant variant = builder.object().add("name", builder.of("value")).build();
        assertThat(VariantGetUtils.parsePath("$").extract(variant)).isSameAs(variant);
    }

    @Test
    void testParsedPathReuse() {
        final ParsedPath path = VariantGetUtils.parsePath("$.items[0].name");
        final Variant first =
                builder.object()
                        .add(
                                "items",
                                builder.array()
                                        .add(
                                                builder.object()
                                                        .add("name", builder.of("first"))
                                                        .build())
                                        .build())
                        .build();
        final Variant second =
                builder.object()
                        .add(
                                "items",
                                builder.array()
                                        .add(
                                                builder.object()
                                                        .add("name", builder.of("second"))
                                                        .build())
                                        .build())
                        .build();
        assertThat(path.extract(first))
                .isNotNull()
                .satisfies(value -> assertThat(value.getString()).isEqualTo("first"));
        assertThat(path.extract(null)).isNull();
        assertThat(path.extract(builder.object().build())).isNull();
        assertThat(path.extract(builder.of(1))).isNull();
        assertThat(path.extract(second))
                .isNotNull()
                .satisfies(value -> assertThat(value.getString()).isEqualTo("second"));
        assertThat(path.extract(first))
                .isNotNull()
                .satisfies(value -> assertThat(value.getString()).isEqualTo("first"));
    }

    @Test
    void testParsedRootPath() {
        final ParsedPath path = VariantGetUtils.parsePath("$");
        final Variant value = builder.ofNull();
        assertThat(path.extract(null)).isNull();
        assertThat(path.extract(value)).isSameAs(value);
    }

    @ParameterizedTest
    @MethodSource("objectPaths")
    void testObjectField(String path, String expectedKey) {
        final Variant variant = builder.object().add(expectedKey, builder.of("value")).build();
        assertThat(VariantGetUtils.parsePath(path).extract(variant))
                .isNotNull()
                .satisfies(value -> assertThat(value.getString()).isEqualTo("value"));
    }

    private static Stream<Arguments> objectPaths() {
        return Stream.of(
                Arguments.of("$.name", "name"),
                Arguments.of("$['name']", "name"),
                Arguments.of("$[\"name\"]", "name"),
                Arguments.of("$['a.b[0]']", "a.b[0]"),
                Arguments.of("$[\"a.b[0]\"]", "a.b[0]"),
                Arguments.of("$['']", ""),
                Arguments.of("$[\"\"]", ""),
                Arguments.of("$['123']", "123"),
                Arguments.of("$['a\"b']", "a\"b"),
                Arguments.of("$[\"a'b\"]", "a'b"),
                Arguments.of("$['a\\nb']", "a\\nb"),
                Arguments.of("$[\"a\\nb\"]", "a\\nb"),
                Arguments.of("$. first name ", " first name "),
                Arguments.of("$.*", "*"));
    }

    @ParameterizedTest
    @MethodSource("arrayPaths")
    void testArrayIndex(String path, int expectedValue) {
        final Variant variant =
                builder.array().add(builder.of(10)).add(builder.of(20)).add(builder.of(30)).build();
        assertThat(VariantGetUtils.parsePath(path).extract(variant))
                .isNotNull()
                .satisfies(value -> assertThat(value.getInt()).isEqualTo(expectedValue));
    }

    private static Stream<Arguments> arrayPaths() {
        return Stream.of(
                Arguments.of("$[0]", 10),
                Arguments.of("$[1]", 20),
                Arguments.of("$[2]", 30),
                Arguments.of("$[0002]", 30));
    }

    @Test
    void testMixedPath() {
        final Variant leaf = builder.object().add("name", builder.of("value")).build();
        final Variant item = builder.object().add("a.b", leaf).build();
        final Variant items =
                builder.array().add(builder.ofNull()).add(builder.of(0)).add(item).build();
        final Variant variant = builder.object().add("items", items).build();
        assertThat(VariantGetUtils.parsePath("$.items[2]['a.b'][\"name\"]").extract(variant))
                .isNotNull()
                .satisfies(value -> assertThat(value.getString()).isEqualTo("value"));
    }

    @Test
    void testNestedArrays() {
        final Variant item = builder.object().add("name", builder.of("value")).build();
        final Variant nested = builder.array().add(builder.ofNull()).add(item).build();
        final Variant variant = builder.array().add(nested).build();
        assertThat(VariantGetUtils.parsePath("$[0][1].name").extract(variant))
                .isNotNull()
                .satisfies(value -> assertThat(value.getString()).isEqualTo("value"));
    }

    @Test
    void testNullInput() {
        assertThat(VariantGetUtils.parsePath("$.name").extract(null)).isNull();
    }

    @Test
    void testVariantNull() {
        final Variant value = builder.ofNull();
        assertThat(VariantGetUtils.parsePath("$").extract(value)).isSameAs(value);
        final Variant variant = builder.object().add("value", value).build();
        assertThat(VariantGetUtils.parsePath("$.value").extract(variant))
                .isNotNull()
                .satisfies(result -> assertThat(result.isNull()).isTrue());
        assertThat(VariantGetUtils.parsePath("$.value.name").extract(variant)).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"$.missing", "$.missing.name", "$.missing[0]"})
    void testMissingField(String path) {
        assertThat(VariantGetUtils.parsePath(path).extract(builder.object().build())).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"$[1]", "$[123]", "$[2147483647]", "$[1].name", "$[1][0]"})
    void testArrayIndexOutOfBounds(String path) {
        final Variant variant = builder.array().add(builder.of(10)).build();
        assertThat(VariantGetUtils.parsePath(path).extract(variant)).isNull();
    }

    @Test
    void testContainerTypeMismatch() {
        assertThat(VariantGetUtils.parsePath("$[0]").extract(builder.object().build())).isNull();
        assertThat(VariantGetUtils.parsePath("$.name").extract(builder.array().build())).isNull();
        assertThat(VariantGetUtils.parsePath("$.name").extract(builder.of(1))).isNull();
        assertThat(VariantGetUtils.parsePath("$[0]").extract(builder.of(1))).isNull();
    }

    @Test
    void testExtractContainer() {
        final Variant item = builder.object().add("name", builder.of("value")).build();
        final Variant items = builder.array().add(item).build();
        final Variant variant = builder.object().add("items", items).build();
        assertThat(VariantGetUtils.parsePath("$.items").extract(variant))
                .isNotNull()
                .satisfies(
                        result -> assertThat(result.toJson()).isEqualTo("[{\"name\":\"value\"}]"));
        assertThat(VariantGetUtils.parsePath("$.items[0]").extract(variant))
                .isNotNull()
                .satisfies(result -> assertThat(result.toJson()).isEqualTo("{\"name\":\"value\"}"));
    }

    @ParameterizedTest
    @EmptySource
    @ValueSource(
            strings = {
                "name",
                ".name",
                "[0]",
                "$name",
                "$$",
                " $",
                "$ ",
                "$.",
                "$..name",
                "$.name.",
                "$[",
                "$[]",
                "$[-1]",
                "$[+1]",
                "$[1.0]",
                "$[ 0]",
                "$[0 ]",
                "$[2147483648]",
                "$[999999999999999999999999]",
                "$[*]",
                "$[0:2]",
                "$[0,1]",
                "$[?(@.a)]",
                "$['name]",
                "$[\"name]",
                "$['name'",
                "$[\"name\"",
                "$['a\\'b']",
                "$[\"a\\\"b\"]",
                "$[0]junk",
                "$[0] .name",
                "$['name']junk",
                "$.items[2]..name",
                "$.items[2]['name']?"
            })
    void testInvalidPath(String path) {
        assertThatThrownBy(() -> VariantGetUtils.parsePath(path))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessage("Failed to parse this path: %s", path);
    }
}
