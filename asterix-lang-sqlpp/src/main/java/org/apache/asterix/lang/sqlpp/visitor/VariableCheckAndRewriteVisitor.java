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
package org.apache.asterix.lang.sqlpp.visitor;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.config.MetadataConstants;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.LimitClause;
import org.apache.asterix.lang.common.clause.OrderbyClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.context.Scope;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.IfExpr;
import org.apache.asterix.lang.common.expression.IndexAccessor;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.QuantifiedExpression;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.UnaryExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.parser.ScopeChecker;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.QuantifiedPair;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.HavingClause;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.NestClause;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectRegular;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppQueryExpressionVisitor;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.base.Counter;

public class VariableCheckAndRewriteVisitor extends AbstractSqlppQueryExpressionVisitor<Expression, Expression> {

    protected final ScopeChecker scopeChecker = new ScopeChecker();
    protected final LangRewritingContext context;
    protected final boolean overwrite;
    protected final AqlMetadataProvider metadataProvider;

    /**
     * @param context,
     *            manages ids of variables and guarantees uniqueness of variables.
     * @param overwrite,
     *            whether rewrite unbounded variables to dataset function calls.
     *            This flag can only be true for rewriting a top-level query.
     *            It should be false for rewriting the body expression of a user-defined function.
     */
    public VariableCheckAndRewriteVisitor(LangRewritingContext context, boolean overwrite,
            AqlMetadataProvider metadataProvider) {
        this.context = context;
        scopeChecker.setVarCounter(new Counter(context.getVarCounter()));
        this.overwrite = overwrite;
        this.metadataProvider = metadataProvider;
    }

    @Override
    public Expression visit(FromClause fromClause, Expression arg) throws AsterixException {
        scopeChecker.extendCurrentScope();
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            fromTerm.accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(FromTerm fromTerm, Expression arg) throws AsterixException {
        scopeChecker.createNewScope();
        // Visit the left expression of a from term.
        fromTerm.setLeftExpression(fromTerm.getLeftExpression().accept(this, arg));

        // Registers the data item variable.
        VariableExpr leftVar = fromTerm.getLeftVariable();
        scopeChecker.getCurrentScope().addNewVarSymbolToScope(leftVar.getVar());

        // Registers the positional variable
        if (fromTerm.hasPositionalVariable()) {
            VariableExpr posVar = fromTerm.getPositionalVariable();
            scopeChecker.getCurrentScope().addNewVarSymbolToScope(posVar.getVar());
        }
        // Visits join/unnest/nest clauses.
        for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
            correlateClause.accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(JoinClause joinClause, Expression arg) throws AsterixException {
        Scope backupScope = scopeChecker.removeCurrentScope();
        Scope parentScope = scopeChecker.getCurrentScope();
        scopeChecker.createNewScope();
        // NOTE: the two join branches cannot be correlated, instead of checking
        // the correlation here,
        // we defer the check to the query optimizer.
        joinClause.setRightExpression(joinClause.getRightExpression().accept(this, arg));

        // Registers the data item variable.
        VariableExpr rightVar = joinClause.getRightVariable();
        scopeChecker.getCurrentScope().addNewVarSymbolToScope(rightVar.getVar());

        if (joinClause.hasPositionalVariable()) {
            // Registers the positional variable.
            VariableExpr posVar = joinClause.getPositionalVariable();
            scopeChecker.getCurrentScope().addNewVarSymbolToScope(posVar.getVar());
        }

        Scope rightScope = scopeChecker.removeCurrentScope();
        Scope mergedScope = new Scope(scopeChecker, parentScope);
        mergedScope.merge(backupScope);
        mergedScope.merge(rightScope);
        scopeChecker.pushExistingScope(mergedScope);
        // The condition expression can refer to the just registered variables
        // for the right branch.
        joinClause.setConditionExpression(joinClause.getConditionExpression().accept(this, arg));
        return null;
    }

    @Override
    public Expression visit(NestClause nestClause, Expression arg) throws AsterixException {
        // NOTE: the two branches of a NEST cannot be correlated, instead of
        // checking the correlation here, we defer the check to the query
        // optimizer.
        nestClause.setRightExpression(nestClause.getRightExpression().accept(this, arg));

        // Registers the data item variable.
        VariableExpr rightVar = nestClause.getRightVariable();
        scopeChecker.getCurrentScope().addNewVarSymbolToScope(rightVar.getVar());

        if (nestClause.hasPositionalVariable()) {
            // Registers the positional variable.
            VariableExpr posVar = nestClause.getPositionalVariable();
            scopeChecker.getCurrentScope().addNewVarSymbolToScope(posVar.getVar());
        }

        // The condition expression can refer to the just registered variables
        // for the right branch.
        nestClause.setConditionExpression(nestClause.getConditionExpression().accept(this, arg));
        return null;
    }

    @Override
    public Expression visit(UnnestClause unnestClause, Expression arg) throws AsterixException {
        unnestClause.setRightExpression(unnestClause.getRightExpression().accept(this, arg));

        // register the data item variable
        VariableExpr rightVar = unnestClause.getRightVariable();
        scopeChecker.getCurrentScope().addNewVarSymbolToScope(rightVar.getVar());

        if (unnestClause.hasPositionalVariable()) {
            // register the positional variable
            VariableExpr posVar = unnestClause.getPositionalVariable();
            scopeChecker.getCurrentScope().addNewVarSymbolToScope(posVar.getVar());
        }
        return null;
    }

    @Override
    public Expression visit(Projection projection, Expression arg) throws AsterixException {
        projection.setExpression(projection.getExpression().accept(this, arg));
        return null;
    }

    @Override
    public Expression visit(SelectBlock selectBlock, Expression arg) throws AsterixException {
        // Traverses the select block in the order of "from", "let"s, "where",
        // "group by", "let"s, "having" and "select".
        if (selectBlock.hasFromClause()) {
            selectBlock.getFromClause().accept(this, arg);
        }
        if (selectBlock.hasLetClauses()) {
            List<LetClause> letList = selectBlock.getLetList();
            for (LetClause letClause : letList) {
                letClause.accept(this, arg);
            }
        }
        if (selectBlock.hasWhereClause()) {
            selectBlock.getWhereClause().accept(this, arg);
        }
        if (selectBlock.hasGroupbyClause()) {
            selectBlock.getGroupbyClause().accept(this, arg);
            if (selectBlock.hasLetClausesAfterGroupby()) {
                List<LetClause> letListAfterGby = selectBlock.getLetListAfterGroupby();
                for (LetClause letClauseAfterGby : letListAfterGby) {
                    letClauseAfterGby.accept(this, arg);
                }
            }
            if (selectBlock.hasHavingClause()) {
                // Rewrites the having clause.
                selectBlock.getHavingClause().accept(this, arg);
            }
        }
        selectBlock.getSelectClause().accept(this, arg);
        return null;
    }

    @Override
    public Expression visit(SelectClause selectClause, Expression arg) throws AsterixException {
        if (selectClause.selectElement()) {
            selectClause.getSelectElement().accept(this, arg);
        }
        if (selectClause.selectRegular()) {
            selectClause.getSelectRegular().accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(SelectElement selectElement, Expression arg) throws AsterixException {
        selectElement.setExpression(selectElement.getExpression().accept(this, arg));
        return null;
    }

    @Override
    public Expression visit(SelectRegular selectRegular, Expression arg) throws AsterixException {
        for (Projection projection : selectRegular.getProjections()) {
            projection.accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(SelectSetOperation selectSetOperation, Expression arg) throws AsterixException {
        selectSetOperation.getLeftInput().accept(this, arg);
        for (SetOperationRight right : selectSetOperation.getRightInputs()) {
            scopeChecker.createNewScope();
            right.getSetOperationRightInput().accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(HavingClause havingClause, Expression arg) throws AsterixException {
        havingClause.setFilterExpression(havingClause.getFilterExpression().accept(this, arg));
        return null;
    }

    @Override
    public Expression visit(Query q, Expression arg) throws AsterixException {
        q.setBody(q.getBody().accept(this, arg));
        q.setVarCounter(scopeChecker.getVarCounter());
        context.setVarCounter(scopeChecker.getVarCounter());
        return null;
    }

    @Override
    public Expression visit(FunctionDecl fd, Expression arg) throws AsterixException {
        scopeChecker.createNewScope();
        fd.setFuncBody(fd.getFuncBody().accept(this, arg));
        scopeChecker.removeCurrentScope();
        return null;
    }

    @Override
    public Expression visit(WhereClause whereClause, Expression arg) throws AsterixException {
        whereClause.setWhereExpr(whereClause.getWhereExpr().accept(this, arg));
        return null;
    }

    @Override
    public Expression visit(OrderbyClause oc, Expression arg) throws AsterixException {
        List<Expression> newOrderbyList = new ArrayList<Expression>();
        for (Expression orderExpr : oc.getOrderbyList()) {
            newOrderbyList.add(orderExpr.accept(this, arg));
        }
        oc.setOrderbyList(newOrderbyList);
        return null;
    }

    @Override
    public Expression visit(GroupbyClause gc, Expression arg) throws AsterixException {
        Scope newScope = scopeChecker.extendCurrentScopeNoPush(true);
        // Puts all group-by variables into the symbol set of the new scope.
        for (GbyVariableExpressionPair gbyVarExpr : gc.getGbyPairList()) {
            gbyVarExpr.setExpr(gbyVarExpr.getExpr().accept(this, arg));
            VariableExpr gbyVar = gbyVarExpr.getVar();
            if (gbyVar != null) {
                newScope.addNewVarSymbolToScope(gbyVarExpr.getVar().getVar());
            }
        }
        for (VariableExpr withVar : gc.getWithVarList()) {
            newScope.addNewVarSymbolToScope(withVar.getVar());
        }
        scopeChecker.replaceCurrentScope(newScope);
        return null;
    }

    @Override
    public Expression visit(LimitClause limitClause, Expression arg) throws AsterixException {
        scopeChecker.pushForbiddenScope(scopeChecker.getCurrentScope());
        limitClause.setLimitExpr(limitClause.getLimitExpr().accept(this, arg));
        scopeChecker.popForbiddenScope();
        return null;
    }

    @Override
    public Expression visit(LetClause letClause, Expression arg) throws AsterixException {
        scopeChecker.extendCurrentScope();
        letClause.setBindingExpr(letClause.getBindingExpr().accept(this, arg));
        scopeChecker.getCurrentScope().addNewVarSymbolToScope(letClause.getVarExpr().getVar());
        return null;
    }

    @Override
    public Expression visit(SelectExpression selectExpression, Expression arg) throws AsterixException {
        Scope scopeBeforeSelectExpression = scopeChecker.getCurrentScope();
        scopeChecker.createNewScope();

        // visit let list
        if (selectExpression.hasLetClauses()) {
            for (LetClause letClause : selectExpression.getLetList()) {
                letClause.accept(this, arg);
            }
        }

        // visit the main select.
        selectExpression.getSelectSetOperation().accept(this, selectExpression);

        // visit order by
        if (selectExpression.hasOrderby()) {
            selectExpression.getOrderbyClause().accept(this, arg);
        }

        // visit limit
        if (selectExpression.hasLimit()) {
            selectExpression.getLimitClause().accept(this, arg);
        }

        // Exit scopes that were entered within this select expression
        while (scopeChecker.getCurrentScope() != scopeBeforeSelectExpression) {
            scopeChecker.removeCurrentScope();
        }
        return selectExpression;
    }

    @Override
    public Expression visit(LiteralExpr l, Expression arg) throws AsterixException {
        return l;
    }

    @Override
    public Expression visit(ListConstructor lc, Expression arg) throws AsterixException {
        List<Expression> newExprList = new ArrayList<Expression>();
        for (Expression expr : lc.getExprList()) {
            newExprList.add(expr.accept(this, arg));
        }
        lc.setExprList(newExprList);
        return lc;
    }

    @Override
    public Expression visit(RecordConstructor rc, Expression arg) throws AsterixException {
        for (FieldBinding binding : rc.getFbList()) {
            binding.setLeftExpr(binding.getLeftExpr().accept(this, arg));
            binding.setRightExpr(binding.getRightExpr().accept(this, arg));
        }
        return rc;
    }

    @Override
    public Expression visit(OperatorExpr operatorExpr, Expression arg) throws AsterixException {
        List<Expression> newExprList = new ArrayList<Expression>();
        for (Expression expr : operatorExpr.getExprList()) {
            newExprList.add(expr.accept(this, arg));
        }
        operatorExpr.setExprList(newExprList);
        return operatorExpr;
    }

    @Override
    public Expression visit(IfExpr ifExpr, Expression arg) throws AsterixException {
        ifExpr.setCondExpr(ifExpr.getCondExpr().accept(this, arg));
        ifExpr.setThenExpr(ifExpr.getThenExpr().accept(this, arg));
        ifExpr.setElseExpr(ifExpr.getElseExpr().accept(this, arg));
        return ifExpr;
    }

    @Override
    public Expression visit(QuantifiedExpression qe, Expression arg) throws AsterixException {
        scopeChecker.createNewScope();
        for (QuantifiedPair pair : qe.getQuantifiedList()) {
            scopeChecker.getCurrentScope().addNewVarSymbolToScope(pair.getVarExpr().getVar());
            pair.setExpr(pair.getExpr().accept(this, arg));
        }
        qe.setSatisfiesExpr(qe.getSatisfiesExpr().accept(this, arg));
        scopeChecker.removeCurrentScope();
        return qe;
    }

    @Override
    public Expression visit(CallExpr callExpr, Expression arg) throws AsterixException {
        List<Expression> newExprList = new ArrayList<Expression>();
        for (Expression expr : callExpr.getExprList()) {
            newExprList.add(expr.accept(this, arg));
        }
        callExpr.setExprList(newExprList);
        return callExpr;
    }

    @Override
    public Expression visit(VariableExpr varExpr, Expression arg) throws AsterixException {
        String varName = varExpr.getVar().getValue();
        if (scopeChecker.isInForbiddenScopes(varName)) {
            throw new AsterixException(
                    "Inside limit clauses, it is disallowed to reference a variable having the same name as any variable bound in the same scope as the limit clause.");
        }
        if (rewriteNeeded(varExpr)) {
            return datasetRewrite(varExpr);
        } else {
            return varExpr;
        }
    }

    @Override
    public Expression visit(UnaryExpr u, Expression arg) throws AsterixException {
        u.setExpr(u.getExpr().accept(this, arg));
        return u;
    }

    @Override
    public Expression visit(FieldAccessor fa, Expression arg) throws AsterixException {
        fa.setExpr(fa.getExpr().accept(this, arg));
        return fa;
    }

    @Override
    public Expression visit(IndexAccessor ia, Expression arg) throws AsterixException {
        ia.setExpr(ia.getExpr().accept(this, arg));
        if (ia.getIndexExpr() != null) {
            ia.setIndexExpr(ia.getIndexExpr());
        }
        return ia;
    }

    // Whether a rewrite is needed for a variable reference expression.
    private boolean rewriteNeeded(VariableExpr varExpr) throws AsterixException {
        String varName = varExpr.getVar().getValue();
        Identifier ident = scopeChecker.lookupSymbol(varName);
        if (ident != null) {
            // Exists such an identifier
            varExpr.setIsNewVar(false);
            varExpr.setVar((VarIdentifier) ident);
            return false;
        } else {
            // Meets a undefined variable
            return true;
        }
    }

    // Rewrites for global variable (e.g., dataset) references.
    private Expression datasetRewrite(VariableExpr expr) throws AsterixException {
        if (!overwrite) {
            return expr;
        }
        String funcName = "dataset";
        String dataverse = MetadataConstants.METADATA_DATAVERSE_NAME;
        FunctionSignature signature = new FunctionSignature(dataverse, funcName, 1);
        List<Expression> argList = new ArrayList<Expression>();
        //Ignore the parser-generated prefix "$" for a dataset.
        String dataset = SqlppVariableUtil.toUserDefinedVariableName(expr.getVar()).getValue();
        argList.add(new LiteralExpr(new StringLiteral(dataset)));
        return new CallExpr(signature, argList);
    }
}
