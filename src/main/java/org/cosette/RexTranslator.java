package org.cosette;

import io.github.cvc5.Solver;
import io.github.cvc5.Sort;
import io.github.cvc5.Term;
import kala.collection.immutable.ImmutableMap;
import kala.control.Result;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Objects;

public record RexTranslator(
        Solver solver,
        ImmutableMap<String, Term> declaredFunctions,
        ImmutableMap<RelType.VarType, MatchEnv.ProductType> typeDerivation
) {
    public RexTranslator encode(MatchEnv.SynthesisConstraint constraint) {
        return this;
    }

    public Result<Term[], String> translate(RexNode node, ImmutableMap<Integer, Term[]> fields) {
        return switch (node) {
            case RexInputRef inputRef -> Result.ok(fields.get(inputRef.getIndex()));
            case RexLiteral literal -> Result.ok(new Term[]{switch (literal.getTypeName()) {
                case BOOLEAN ->
                        solver.mkBoolean(Boolean.parseBoolean(Objects.requireNonNull(literal.getValue()).toString()));
                case TINYINT, INTEGER, SMALLINT, BIGINT ->
                        solver.mkInteger(Long.parseLong(Objects.requireNonNull(literal.getValue()).toString()));
                case DECIMAL, FLOAT, REAL, DOUBLE -> {
                    try {
                        yield solver.mkReal(Objects.requireNonNull(literal.getValue()).toString());
                    } catch (Exception ignored) {
                        yield solver.mkConst(solver.getRealSort());
                    }
                }
                case CHAR, VARCHAR -> {
                    var content = literal.toString();
                    yield solver.mkString(content.substring(1, content.length() - 1));
                }
                default -> solver.mkConst(getSort(literal.getTypeName()), literal.toString());
            }});
//            case RexCall call -> {
//                var function = call.getOperator();
//                if (function instanceof RuleBuilder.CosetteFunction) {
//
//                } else {
//
//                }
//            }
//            case RexCall call -> object(Map.of(
//                    "operator", new TextNode(call.getOperator().toString()),
//                    "operand", array(Seq.from(call.getOperands()).map(this::visit)),
//                    "type", new TextNode(call.getType().toString())
//            ));
            default -> Result.err(String.format("Have not implemented translation for %s", node));
        };
    }

    public Sort getSort(SqlTypeName sqlTypeName) {
        return switch (sqlTypeName) {
            case BOOLEAN -> solver.getBooleanSort();
            case TINYINT, INTEGER, SMALLINT, BIGINT -> solver.getIntegerSort();
            case DECIMAL, FLOAT, REAL, DOUBLE -> solver.getRealSort();
            case CHAR, VARCHAR -> solver.getStringSort();
            default -> solver.mkUninterpretedSort(sqlTypeName.getName());
        };
    }
}
