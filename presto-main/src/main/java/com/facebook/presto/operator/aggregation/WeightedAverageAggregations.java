/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.operator.aggregation.state.FractionState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;

@AggregationFunction("weighted_avg")
public final class WeightedAverageAggregations
{
    private WeightedAverageAggregations() {}

    @InputFunction
    public static void input(@AggregationState FractionState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.DOUBLE) double weight)
    {
        state.setDenominator(state.getNumerator() + weight);
        state.setNumerator(state.getNumerator() + (weight * value));
    }

    @InputFunction
    public static void input(@AggregationState FractionState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.DOUBLE) double weight)
    {
        state.setDenominator(state.getDenominator() + weight);
        state.setNumerator(state.getNumerator() + (weight * value));
    }

    @InputFunction
    public static void input(@AggregationState FractionState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.BIGINT) long weight)
    {
        state.setDenominator(state.getNumerator() + weight);
        state.setNumerator(state.getNumerator() + (weight * value));
    }

    @InputFunction
    public static void input(@AggregationState FractionState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.BIGINT) long weight)
    {
        state.setDenominator(state.getDenominator() + weight);
        state.setNumerator(state.getNumerator() + (weight * value));
    }

    @CombineFunction
    public static void combine(@AggregationState FractionState state, @AggregationState FractionState otherState)
    {
        state.setDenominator(state.getDenominator() + otherState.getDenominator());
        state.setNumerator(state.getNumerator() + otherState.getNumerator());
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(@AggregationState FractionState state, BlockBuilder out)
    {
        double denominator = state.getDenominator();
        if (denominator == 0) {
            out.appendNull();
        }
        else {
            double numerator = state.getNumerator();
            DOUBLE.writeDouble(out, numerator / denominator);
        }
    }
}
