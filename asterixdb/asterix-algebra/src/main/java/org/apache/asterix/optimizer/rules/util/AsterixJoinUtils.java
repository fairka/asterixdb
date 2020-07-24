/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.optimizer.rules.util;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.asterix.common.annotations.RangeAnnotation;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;

public class AsterixJoinUtils {

    private static final int LEFT = 0;
    private static final int RIGHT = 1;

    private static final Logger LOGGER = Logger.getLogger(AsterixJoinUtils.class.getName());

    private AsterixJoinUtils() {
    }

    public static void setJoinAlgorithmAndExchangeAlgo(AbstractBinaryJoinOperator op, Boolean topLevelOp,
            IOptimizationContext context) throws AlgebricksException {
        if (!topLevelOp) {
            throw new IllegalStateException("Micro operator not implemented for: " + op.getOperatorTag());
        }
        ILogicalExpression conditionLE = op.getCondition().getValue();
        if (conditionLE.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return;
        }
        List<LogicalVariable> sideLeft = new ArrayList<>(1);
        List<LogicalVariable> sideRight = new ArrayList<>(1);
        List<LogicalVariable> varsLeft = op.getInputs().get(LEFT).getValue().getSchema();
        List<LogicalVariable> varsRight = op.getInputs().get(RIGHT).getValue().getSchema();
        AbstractFunctionCallExpression fexp = (AbstractFunctionCallExpression) conditionLE;
        FunctionIdentifier fi =
                IntervalJoinUtils.isIntervalJoinCondition(fexp, varsLeft, varsRight, sideLeft, sideRight);
        if (fi == null) {
            return;
        }
        RangeAnnotation rangeAnnotation = IntervalJoinUtils.IntervalJoinRangeMapAnnotation(fexp);
        if (rangeAnnotation == null) {
            return;
        }
        //Check RangeMap type
        RangeMap rangeMap = (RangeMap) rangeAnnotation.getObject();
        if (rangeMap.getTag(0, 0) != ATypeTag.DATETIME.serialize() && rangeMap.getTag(0, 0) != ATypeTag.DATE.serialize()
                && rangeMap.getTag(0, 0) != ATypeTag.TIME.serialize()) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, op.getSourceLocation(),
                    "Only DATE, TIME, and DATETIME type rangemaps have been "
                            + "implemented for this interval operation.");
        }
        //        LOGGER.fine("Interval Join - Forward Scan");
        IntervalPartitions intervalPartitions =
                IntervalJoinUtils.getIntervalPartitions(op, fi, sideLeft, sideRight, rangeMap, context);
        //IntervalJoinUtils.setIntervalForwardScanJoinOp(op, fi, sideLeft, sideRight, context, intervalPartitions);
        IntervalJoinUtils.setSortMergeIntervalJoinOp(op, fi, sideLeft, sideRight, context, intervalPartitions);
    }
}
