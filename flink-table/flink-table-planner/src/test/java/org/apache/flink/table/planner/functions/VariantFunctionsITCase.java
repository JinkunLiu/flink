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

import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.Expressions.$;

/** Tests for built-in VARIANT functions. */
class VariantFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(variantGetSpec());
    }

    private static TestSetSpec variantGetSpec() {
        final String json =
                "{\"name\":\"Flink\",\"count\":42,\"enabled\":true,"
                        + "\"nested\":{\"value\":7},\"items\":[10,20],\"nothing\":null}";

        return TestSetSpec.forFunction(BuiltInFunctionDefinitions.VARIANT_GET)
                .onFieldsWithData(json, null)
                .andDataTypes(STRING().notNull(), STRING())
                .testSqlResult(
                        "JSON_STRING(VARIANT_GET(PARSE_JSON(f0), '$.nested'))",
                        "{\"value\":7}",
                        STRING())
                .testSqlResult("VARIANT_GET(PARSE_JSON(f0), '$.count', 'INT')", 42, INT())
                .testTableApiResult(
                        $("f0").parseJson().variantGet("$.name", STRING()), "Flink", STRING())
                .testTableApiResult($("f0").parseJson().variantGet("$.count", INT()), 42, INT())
                .testTableApiResult(
                        $("f0").parseJson().variantGet("$.enabled", BOOLEAN()), true, BOOLEAN())
                .testTableApiResult($("f0").parseJson().variantGet("$.items[1]", INT()), 20, INT())
                .testTableApiResult(
                        $("f0").parseJson().variantGet("$.missing", STRING()), null, STRING())
                .testTableApiResult(
                        $("f0").parseJson().variantGet("$.nothing", STRING()), null, STRING())
                .testTableApiResult(
                        $("f1").parseJson().variantGet("$.name", STRING()), null, STRING())
                .testTableApiRuntimeError(
                        $("f0").parseJson().variantGet("$[", STRING()), "Failed to parse this path")
                .testSqlRuntimeError(
                        "VARIANT_GET(PARSE_JSON(f0), '$[')", "Failed to parse this path");
    }
}
