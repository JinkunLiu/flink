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

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.runtime.functions.VariantGetUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;

import static org.apache.flink.table.api.Expressions.$;

/** Implementation of {@link BuiltInFunctionDefinitions#VARIANT_GET}. */
@Internal
public class VariantGetFunction extends BuiltInScalarFunction {

    private final @Nullable SpecializedFunction.ExpressionEvaluator castEvaluator;
    private final @Nullable DataStructureConverter<Object, Object> castResultConverter;
    private transient @Nullable MethodHandle castHandle;

    public VariantGetFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.VARIANT_GET, context);

        if (getOutputDataType().getLogicalType().is(LogicalTypeRoot.VARIANT)) {
            castEvaluator = null;
            castResultConverter = null;
        } else {
            final DataType targetType =
                    context.getCallContext()
                            .getOutputDataType()
                            .orElseThrow(IllegalStateException::new);
            castEvaluator =
                    context.createEvaluator(
                            $("value").cast(targetType),
                            targetType,
                            DataTypes.FIELD("value", DataTypes.VARIANT().toInternal()));
            castResultConverter = DataStructureConverters.getConverter(targetType);
        }
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        if (castEvaluator != null) {
            castHandle = castEvaluator.open(context);
            castResultConverter.open(context.getUserCodeClassLoader());
        }
    }

    public @Nullable Object eval(@Nullable Variant variant, @Nullable StringData path) {
        final Variant extracted =
                VariantGetUtils.variantGet(variant, path == null ? null : path.toString());
        if (extracted == null) {
            return null;
        }
        if (castEvaluator == null) {
            return extracted;
        }

        try {
            return castResultConverter.toInternalOrNull(castHandle.invoke(extracted)); // todo：liujinkun02，确认下段代码
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    public @Nullable Object eval(
            @Nullable Variant variant, @Nullable StringData path, @Nullable StringData targetType) {
        return eval(variant, path);
    }

    @Override
    public void close() throws Exception {
        if (castEvaluator != null) {
            castEvaluator.close();
        }
    }
}
