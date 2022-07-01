package org.cosette;

import kala.collection.Seq;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;

import java.math.BigDecimal;

public record RexRacketVisitor(Env env) {
    public SExpr visit(RexNode rex) {
        return switch (rex) {
            case RexInputRef inputRef -> SExpr.app("v-var", SExpr.integer(env.base() + inputRef.getIndex()));
            case RexLiteral literal -> {
                var val = switch (literal.getValue()) {
                    case null -> SExpr.quoted("sqlnull");
                    case Float v -> SExpr.real(v);
                    case Double v -> SExpr.real(v);
                    case BigDecimal v -> {
                        try {
                            yield SExpr.integer(v.longValueExact());
                        } catch (ArithmeticException e) {
                            yield SExpr.real(v.doubleValue());
                        }
                    }
                    case Integer v -> SExpr.integer(v);
                    case Long v -> SExpr.integer(v);
                    case Boolean v -> SExpr.bool(v);
                    case String v -> SExpr.string(v);
                    default -> throw new UnsupportedOperationException("Unsupported literal: " + literal);
                };
                yield SExpr.app("v-op", val, SExpr.app("list"));
            }
            case RexSubQuery subQuery -> {
                var name = switch (subQuery.getOperator().getKind()) {
                    case EXISTS -> "exists";
                    case UNIQUE -> "unique";
                    case IN -> "in";
                    case SqlKind kind -> throw new UnsupportedOperationException("Unsupported subquery operation: " + kind);
                };
                var operands = Seq.from(subQuery.getOperands()).map(this::visit);
                var rel = new RelRacketShuttle(env.advanced(0)).visit(subQuery.rel);
                yield SExpr.app("v-hop", SExpr.quoted(name), SExpr.app("list", operands), rel);
            }
            case RexCall call -> {
                var operands = Seq.from(call.getOperands()).map(this::visit);
                yield SExpr.app("v-op", SExpr.quoted(call.op.getName()), SExpr.app("list", operands));
            }
            case RexFieldAccess access -> {
                var id = SExpr.integer(env.resolve(((RexCorrelVariable) access.getReferenceExpr()).id) + access.getField().getIndex());
                yield SExpr.app("v-var", id);
            }
            default -> throw new UnsupportedOperationException("Unsupported value: " + rex.getKind());
        };
    }
}
