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

package org.apache.flink.table.planner.functions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_LEGACY_CAST_BEHAVIOUR;

/** Tests for VARIANT extraction functions with legacy cast mode enabled. */
class VariantFunctionsLegacyCastITCase extends BuiltInFunctionTestBase {

    private static final String JSON =
            "{\"count\":42,\"name\":\"bad\",\"large\":2147483648,"
                    + "\"items\":[10,20],\"badItems\":[10,\"bad\"]}";

    @Override
    Configuration getConfiguration() {
        return super.getConfiguration()
                .set(
                        TABLE_EXEC_LEGACY_CAST_BEHAVIOUR,
                        ExecutionConfigOptions.LegacyCastBehaviour.ENABLED);
    }

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(variantGetSpec(), tryVariantGetSpec());
    }

    private static TestSetSpec variantGetSpec() {
        return TestSetSpec.forFunction(BuiltInFunctionDefinitions.VARIANT_GET)
                .onFieldsWithData(JSON, null)
                .andDataTypes(STRING().notNull(), STRING())
                .testResult(
                        $("f0").parseJson().variantGet("$.count", INT()),
                        "VARIANT_GET(PARSE_JSON(f0), '$.count', 'INT')",
                        42,
                        INT())
                .testResult(
                        $("f0").parseJson().variantGet("$.items", ARRAY(INT())),
                        "VARIANT_GET(PARSE_JSON(f0), '$.items', 'ARRAY<INT>')",
                        new Integer[] {10, 20},
                        ARRAY(INT()))
                .testResult(
                        $("f0").parseJson().variantGet("$.name", INT()),
                        "VARIANT_GET(PARSE_JSON(f0), '$.name', 'INT')",
                        null,
                        INT())
                .testResult(
                        $("f0").parseJson().variantGet("$.large", INT()),
                        "VARIANT_GET(PARSE_JSON(f0), '$.large', 'INT')",
                        null,
                        INT())
                .testResult(
                        $("f0").parseJson().variantGet("$.badItems", ARRAY(INT())),
                        "VARIANT_GET(PARSE_JSON(f0), '$.badItems', 'ARRAY<INT>')",
                        null,
                        ARRAY(INT()))
                .testResult(
                        $("f1").parseJson().variantGet("$.count", INT()),
                        "VARIANT_GET(PARSE_JSON(f1), '$.count', 'INT')",
                        null,
                        INT())
                .testSqlRuntimeError(
                        "VARIANT_GET(PARSE_JSON(f1), '$[', 'INT')", "Failed to parse this path")
                .testTableApiRuntimeError(
                        $("f1").parseJson().variantGet("$[", INT()), "Failed to parse this path");
    }

    private static TestSetSpec tryVariantGetSpec() {
        return TestSetSpec.forFunction(BuiltInFunctionDefinitions.TRY_VARIANT_GET)
                .onFieldsWithData(JSON, null)
                .andDataTypes(STRING().notNull(), STRING())
                .testResult(
                        $("f0").parseJson().tryVariantGet("$.count", INT()),
                        "TRY_VARIANT_GET(PARSE_JSON(f0), '$.count', 'INT')",
                        42,
                        INT())
                .testResult(
                        $("f0").parseJson().tryVariantGet("$.items", ARRAY(INT())),
                        "TRY_VARIANT_GET(PARSE_JSON(f0), '$.items', 'ARRAY<INT>')",
                        new Integer[] {10, 20},
                        ARRAY(INT()))
                .testResult(
                        $("f0").parseJson().tryVariantGet("$.name", INT()),
                        "TRY_VARIANT_GET(PARSE_JSON(f0), '$.name', 'INT')",
                        null,
                        INT())
                .testResult(
                        $("f0").parseJson().tryVariantGet("$.large", INT()),
                        "TRY_VARIANT_GET(PARSE_JSON(f0), '$.large', 'INT')",
                        null,
                        INT())
                .testResult(
                        $("f0").parseJson().tryVariantGet("$.badItems", ARRAY(INT())),
                        "TRY_VARIANT_GET(PARSE_JSON(f0), '$.badItems', 'ARRAY<INT>')",
                        null,
                        ARRAY(INT()))
                .testResult(
                        $("f1").parseJson().tryVariantGet("$.count", INT()),
                        "TRY_VARIANT_GET(PARSE_JSON(f1), '$.count', 'INT')",
                        null,
                        INT())
                .testSqlRuntimeError(
                        "TRY_VARIANT_GET(PARSE_JSON(f1), '$[', 'INT')", "Failed to parse this path")
                .testTableApiRuntimeError(
                        $("f1").parseJson().tryVariantGet("$[", INT()),
                        "Failed to parse this path");
    }
}
