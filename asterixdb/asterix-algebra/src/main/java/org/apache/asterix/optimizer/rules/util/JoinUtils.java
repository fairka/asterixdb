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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.asterix.algebra.operators.physical.IntervalForwardScanJoinPOperator;
import org.apache.asterix.common.annotations.IntervalJoinExpressionAnnotation;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.runtime.operators.joins.AfterIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.BeforeIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.CoveredByIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.CoversIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.EndedByIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.EndsIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.MeetsIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.MetByIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.OverlappedByIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.OverlappingIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.OverlapsIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.StartedByIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.StartsIntervalMergeJoinCheckerFactory;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator.JoinPartitioningType;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;

public class JoinUtils {

    private static final int LEFT = 0;
    private static final int RIGHT = 1;

    private static final Logger LOGGER = Logger.getLogger(JoinUtils.class.getName());

    private static final Map<FunctionIdentifier, FunctionIdentifier> INTERVAL_JOIN_CONDITIONS = new HashMap<>();
    static {
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_AFTER, BuiltinFunctions.INTERVAL_BEFORE);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_BEFORE, BuiltinFunctions.INTERVAL_AFTER);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_COVERED_BY, BuiltinFunctions.INTERVAL_COVERS);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_COVERS, BuiltinFunctions.INTERVAL_COVERED_BY);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_ENDED_BY, BuiltinFunctions.INTERVAL_ENDS);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_ENDS, BuiltinFunctions.INTERVAL_ENDED_BY);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_MEETS, BuiltinFunctions.INTERVAL_MET_BY);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_MET_BY, BuiltinFunctions.INTERVAL_MEETS);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_OVERLAPPED_BY, BuiltinFunctions.INTERVAL_OVERLAPS);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_OVERLAPPING, BuiltinFunctions.INTERVAL_OVERLAPPING);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_OVERLAPS, BuiltinFunctions.INTERVAL_OVERLAPPED_BY);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_STARTED_BY, BuiltinFunctions.INTERVAL_STARTS);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_STARTS, BuiltinFunctions.INTERVAL_STARTED_BY);
    }

    private JoinUtils() {
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
        List<LogicalVariable> sideLeft = new LinkedList<>();
        List<LogicalVariable> sideRight = new LinkedList<>();
        List<LogicalVariable> varsLeft = op.getInputs().get(LEFT).getValue().getSchema();
        List<LogicalVariable> varsRight = op.getInputs().get(RIGHT).getValue().getSchema();
        AbstractFunctionCallExpression fexp = (AbstractFunctionCallExpression) conditionLE;
        FunctionIdentifier fi = isIntervalJoinCondition(fexp, varsLeft, varsRight, sideLeft, sideRight);
        if (fi != null) {
            RangeMap rangeMap = null;
            LOGGER.fine("Interval Join - Forward Scan");
            setIntervalForwardScanJoinOp(op, fi, sideLeft, sideRight, rangeMap, context);
        }
    }

    private static IntervalJoinExpressionAnnotation getIntervalJoinAnnotation(AbstractFunctionCallExpression fexp) {
        Iterator<IExpressionAnnotation> annotationIter = fexp.getAnnotations().values().iterator();
        while (annotationIter.hasNext()) {
            IExpressionAnnotation annotation = annotationIter.next();
            if (annotation instanceof IntervalJoinExpressionAnnotation) {
                return (IntervalJoinExpressionAnnotation) annotation;
            }
        }
        return null;
    }

    private static void setIntervalForwardScanJoinOp(AbstractBinaryJoinOperator op, FunctionIdentifier fi,
            List<LogicalVariable> sideLeft, List<LogicalVariable> sideRight, RangeMap rangeMap,
            IOptimizationContext context) throws AlgebricksException {
        IIntervalMergeJoinCheckerFactory mjcf = getIntervalMergeJoinCheckerFactory(fi, rangeMap);
        op.setPhysicalOperator(new IntervalForwardScanJoinPOperator(op.getJoinKind(), JoinPartitioningType.BROADCAST,
                sideLeft, sideRight, context.getPhysicalOptimizationConfig().getMaxFramesForJoin(), mjcf, rangeMap));
    }

    private static FunctionIdentifier isIntervalJoinCondition(ILogicalExpression e,
            Collection<LogicalVariable> inLeftAll, Collection<LogicalVariable> inRightAll,
            Collection<LogicalVariable> outLeftFields, Collection<LogicalVariable> outRightFields) {
        FunctionIdentifier fiReturn;
        boolean switchArguments = false;
        if (e.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression fexp = (AbstractFunctionCallExpression) e;
            FunctionIdentifier fi = fexp.getFunctionIdentifier();
            if (isIntervalFunction(fi)) {
                fiReturn = fi;
            } else {
                return null;
            }
            ILogicalExpression opLeft = fexp.getArguments().get(LEFT).getValue();
            ILogicalExpression opRight = fexp.getArguments().get(RIGHT).getValue();
            if (opLeft.getExpressionTag() != LogicalExpressionTag.VARIABLE
                    || opRight.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                return null;
            }
            LogicalVariable var1 = ((VariableReferenceExpression) opLeft).getVariableReference();
            if (inLeftAll.contains(var1) && !outLeftFields.contains(var1)) {
                outLeftFields.add(var1);
            } else if (inRightAll.contains(var1) && !outRightFields.contains(var1)) {
                outRightFields.add(var1);
                fiReturn = reverseIntervalExpression(fi);
                switchArguments = true;
            } else {
                return null;
            }
            LogicalVariable var2 = ((VariableReferenceExpression) opRight).getVariableReference();
            if (inLeftAll.contains(var2) && !outLeftFields.contains(var2) && switchArguments) {
                outLeftFields.add(var2);
            } else if (inRightAll.contains(var2) && !outRightFields.contains(var2) && !switchArguments) {
                outRightFields.add(var2);
            } else {
                return null;
            }
            return fiReturn;
        } else {
            return null;
        }
    }

    private static IIntervalMergeJoinCheckerFactory getIntervalMergeJoinCheckerFactory(FunctionIdentifier fi,
            RangeMap rangeMap) {
        IIntervalMergeJoinCheckerFactory mjcf = new OverlappingIntervalMergeJoinCheckerFactory(rangeMap);
        if (fi.equals(BuiltinFunctions.INTERVAL_OVERLAPPED_BY)) {
            mjcf = new OverlappedByIntervalMergeJoinCheckerFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_OVERLAPS)) {
            mjcf = new OverlapsIntervalMergeJoinCheckerFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_COVERS)) {
            mjcf = new CoversIntervalMergeJoinCheckerFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_COVERED_BY)) {
            mjcf = new CoveredByIntervalMergeJoinCheckerFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_STARTS)) {
            mjcf = new StartsIntervalMergeJoinCheckerFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_STARTED_BY)) {
            mjcf = new StartedByIntervalMergeJoinCheckerFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_ENDS)) {
            mjcf = new EndsIntervalMergeJoinCheckerFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_ENDED_BY)) {
            mjcf = new EndedByIntervalMergeJoinCheckerFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_MEETS)) {
            mjcf = new MeetsIntervalMergeJoinCheckerFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_MET_BY)) {
            mjcf = new MetByIntervalMergeJoinCheckerFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_BEFORE)) {
            mjcf = new BeforeIntervalMergeJoinCheckerFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_AFTER)) {
            mjcf = new AfterIntervalMergeJoinCheckerFactory();
        }
        return mjcf;
    }

    private static boolean isIntervalFunction(FunctionIdentifier fi) {
        return INTERVAL_JOIN_CONDITIONS.containsKey(fi);
    }

    private static FunctionIdentifier reverseIntervalExpression(FunctionIdentifier fi) {
        if (INTERVAL_JOIN_CONDITIONS.containsKey(fi)) {
            return INTERVAL_JOIN_CONDITIONS.get(fi);
        }
        return null;
    }

}