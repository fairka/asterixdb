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
package org.apache.asterix.optimizer.rules.subplan;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;

/**
 * For use in writing a "throwaway" branch which removes NTS and subplan operators. The result of this invocation is to
 * be given to the {@code IntroduceJoinAccessMethodRule} to check if an array index can be used.
 * <br>
 * If we are given the pattern (an existential quantification over a cross product):
 * <pre>
 * SELECT_1(some variable)
 * SUBPLAN_1 -----------------------|
 * |                      AGGREGATE(NON-EMPTY-STREAM)
 * |                      SELECT_2(some predicate)
 * |                      (UNNEST/ASSIGN)*
 * |                      UNNEST(on variable)
 * |                      NESTED-TUPLE-SOURCE
 * JOIN(true)
 * |     |----------------- (potential) index branch ...
 * |----------------- probe branch ...
 * </pre>
 * We return the following branch:
 * <pre>
 * JOIN(some predicate from SELECT_2)
 * |     |----------------- (UNNEST/ASSIGN)*
 * |                        UNNEST(on variable)
 * |                        (potential) index branch ...
 * |----------------- probe branch ...
 * </pre>
 *
 * If we are given the pattern (a universal quantification over a cross product):
 * <pre>
 * SELECT_1(some variable AND array is not empty)
 * SUBPLAN_1 -----------------------|
 * |                      AGGREGATE(EMPTY-STREAM)
 * |                      SELECT_2(NOT(IF-MISSING-OR-NULL(some optimizable predicate)))
 * |                      (UNNEST/ASSIGN)*
 * |                      UNNEST(on variable)
 * |                      NESTED-TUPLE-SOURCE
 * JOIN(true)
 * |     |----------------- (potential) index branch ...
 * |----------------- probe branch ...
 * </pre>
 * We return the following branch:
 * <pre>
 * JOIN(some optimizable predicate)  <--- removed the NOT(IF-MISSING-OR-NULL(...))!
 * |     |----------------- (UNNEST/ASSIGN)*
 * |                        UNNEST(on variable)
 * |                        (potential) index branch ...
 * |----------------- probe branch ...
 * </pre>
 *
 * In the case of nested-subplans, we return a copy of the innermost SELECT followed by all relevant UNNEST/ASSIGNs.
 */
public class JoinFromSubplanCreator extends AbstractOperatorFromSubplanCreator<AbstractBinaryJoinOperator> {
    private final static Set<FunctionIdentifier> optimizableFunctions = new HashSet<>();
    private final Deque<JoinFromSubplanContext> contextStack = new ArrayDeque<>();

    /**
     * Add an optimizable function from an access method that can take advantage of this throwaway branch.
     */
    public static void addOptimizableFunction(FunctionIdentifier functionIdentifier) {
        optimizableFunctions.add(functionIdentifier);
    }

    /**
     * The subplan we want to push to the JOIN operator is located *above/after* the JOIN itself.
     */
    public void findAfterSubplanSelectOperator(List<Mutable<ILogicalOperator>> afterJoinRefs)
            throws AlgebricksException {
        JoinFromSubplanContext joinContext = new JoinFromSubplanContext();
        contextStack.push(joinContext);

        // Minimally, we need to have a DISTRIBUTE <- SELECT <- SUBPLAN.
        if (afterJoinRefs.size() < 3) {
            return;
        }

        // We expect a) the operator immediately above to be a SUBPLAN, and b) the next operator above to be a SELECT.
        Mutable<ILogicalOperator> afterJoinOpRef1 = afterJoinRefs.get(afterJoinRefs.size() - 1);
        Mutable<ILogicalOperator> afterJoinOpRef2 = afterJoinRefs.get(afterJoinRefs.size() - 2);
        Mutable<ILogicalOperator> afterJoinOpRef3 = afterJoinRefs.get(afterJoinRefs.size() - 3);
        ILogicalOperator afterJoinOp1 = afterJoinOpRef1.getValue();
        ILogicalOperator afterJoinOp2 = afterJoinOpRef2.getValue();
        ILogicalOperator afterJoinOp3 = afterJoinOpRef3.getValue();
        if (!afterJoinOp1.getOperatorTag().equals(LogicalOperatorTag.SUBPLAN)
                || !afterJoinOp2.getOperatorTag().equals(LogicalOperatorTag.SELECT)) {
            return;
        }

        // Additionally, verify that our SELECT is conditioning on a variable.
        joinContext.selectAfterSubplan = (SelectOperator) afterJoinOp2;
        if (getConditioningVariable(joinContext.selectAfterSubplan.getCondition().getValue()) == null) {
            return;
        }

        // Modify the given after-join operators. We will reconnect these after the join-rule transformation.
        joinContext.removedAfterJoinOperators = new ArrayList<>();
        joinContext.removedAfterJoinOperators.add(afterJoinOpRef2);
        joinContext.removedAfterJoinOperators.add(afterJoinOpRef1);
        afterJoinRefs.remove(afterJoinOpRef2);
        afterJoinRefs.remove(afterJoinOpRef1);

        // Connect our inputs here. We will compute the type environment for this copy in {@code createOperator}.
        joinContext.afterJoinOpForRewrite = OperatorManipulationUtil.deepCopy(afterJoinOp3);
        joinContext.afterJoinOpForRewrite.getInputs().clear();
        joinContext.afterJoinOpForRewrite.getInputs().addAll(afterJoinOp3.getInputs());
    }

    /**
     * Create a new branch to match that of the form:
     *
     * <pre>
     * JOIN(...)
     * |     |----------------- (UNNEST/ASSIGN)*
     * |                        UNNEST
     * |                        (potential) index branch ...
     * |----------------- probe branch ...
     * </pre>
     * <p>
     * Operators are *created* here, rather than just reconnected from the original branch.
     */
    @Override
    public AbstractBinaryJoinOperator createOperator(AbstractBinaryJoinOperator originalOperator,
            IOptimizationContext context) throws AlgebricksException {
        // Reset our context.
        this.reset(originalOperator.getSourceLocation(), context, optimizableFunctions);
        JoinFromSubplanContext joinContext = contextStack.getFirst();
        joinContext.originalJoinRoot = originalOperator;
        if (joinContext.removedAfterJoinOperators == null) {
            return null;
        }

        // Traverse our subplan and generate a SELECT branch if applicable.
        SubplanOperator subplanOperator =
                (SubplanOperator) joinContext.selectAfterSubplan.getInputs().get(0).getValue();
        Pair<SelectOperator, UnnestOperator> traversalOutput =
                traverseSubplanBranch(subplanOperator, originalOperator.getInputs().get(1).getValue());
        if (traversalOutput == null) {
            return null;
        }

        // We have successfully generated a SELECT branch. Create the new JOIN operator.
        if (originalOperator.getOperatorTag().equals(LogicalOperatorTag.INNERJOIN)) {
            joinContext.newJoinRoot = new InnerJoinOperator(traversalOutput.first.getCondition());

        } else { // originalOperator.getOperatorTag().equals(LogicalOperatorTag.LEFTOUTERJOIN)
            joinContext.newJoinRoot = new LeftOuterJoinOperator(traversalOutput.first.getCondition());
        }
        joinContext.newJoinRoot.getInputs().add(0, originalOperator.getInputs().get(0));

        // Create the index join branch.
        traversalOutput.second.getInputs().clear();
        traversalOutput.second.getInputs().add(originalOperator.getInputs().get(1));
        context.computeAndSetTypeEnvironmentForOperator(traversalOutput.second);
        joinContext.newJoinRoot.getInputs().add(1, traversalOutput.first.getInputs().get(0));
        context.computeAndSetTypeEnvironmentForOperator(joinContext.newJoinRoot);

        // Reconnect our after-join operator to our new join.
        OperatorManipulationUtil.substituteOpInInput(joinContext.afterJoinOpForRewrite,
                joinContext.removedAfterJoinOperators.get(0).getValue(), new MutableObject<>(joinContext.newJoinRoot));
        context.computeAndSetTypeEnvironmentForOperator(joinContext.afterJoinOpForRewrite);
        return joinContext.newJoinRoot;
    }

    /**
     * To undo this process is to return what was passed to us at {@code createOperator} time. If we removed any
     * after-join references, add them back in the order they were originally given.
     */
    @Override
    public AbstractBinaryJoinOperator restoreBeforeRewrite(List<Mutable<ILogicalOperator>> afterOperatorRefs,
            IOptimizationContext context) throws AlgebricksException {
        JoinFromSubplanContext joinContext = contextStack.pop();
        if (joinContext.removedAfterJoinOperators != null) {
            afterOperatorRefs.addAll(joinContext.removedAfterJoinOperators);
        }

        return joinContext.originalJoinRoot;
    }

    /**
     * All state associated with a single call of {@code createOperator}.
     */
    private static class JoinFromSubplanContext {
        private List<Mutable<ILogicalOperator>> removedAfterJoinOperators;
        private AbstractBinaryJoinOperator originalJoinRoot;
        private AbstractBinaryJoinOperator newJoinRoot;
        private SelectOperator selectAfterSubplan;
        private ILogicalOperator afterJoinOpForRewrite;
    }
}
