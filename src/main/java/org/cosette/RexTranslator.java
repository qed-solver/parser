package org.cosette;

import io.github.cvc5.Kind;
import io.github.cvc5.Solver;
import io.github.cvc5.Sort;
import io.github.cvc5.Term;
import kala.collection.Seq;
import kala.collection.immutable.ImmutableMap;
import kala.collection.immutable.ImmutableSeq;
import kala.collection.mutable.MutableMap;
import kala.control.Result;
import kala.tuple.Tuple;
import kala.tuple.Tuple3;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Objects;

public record RexTranslator(
        Solver solver,
        MutableMap<String, Tuple3<Term, ImmutableSeq<SqlTypeName>, SqlTypeName>> declaredFunctions,
        ImmutableMap<RelType.VarType, MatchEnv.ProductType> typeDerivation
) {
    public static Result<RexTranslator, String> createTranslator(ImmutableMap<RelType.VarType, MatchEnv.ProductType> typeDerivation) {
        try {
            var solver = new Solver();
            solver.setOption("produce-models", "true");
            solver.setOption("sygus", "true");
            solver.setLogic("ALL");
            return Result.ok(new RexTranslator(solver, MutableMap.create(), typeDerivation));
        } catch (Exception e) {
            return Result.err(String.format("Encountered error during CVC5 initialization: %s", e));
        }
    }

    public Result<Void, String> encode(MatchEnv.SynthesisConstraint constraint) {
        // TODO: Encode columns as variables and then call translate(), after which assert constraints.
        return Result.ok(null);
    }

    public Result<ImmutableSeq<Term>, String> translate(RexNode node, ImmutableMap<Integer, ImmutableSeq<Term>> fields) {
        return switch (node) {
            case RexInputRef inputRef -> Result.ok(fields.get(inputRef.getIndex()));
            case RexLiteral literal -> Result.ok(ImmutableSeq.of(switch (literal.getTypeName()) {
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
            }));
            case RexCall call -> makeCall(call, fields);
            default -> Result.err(String.format("Have not implemented translation for %s", node));
        };
    }

    public Result<ImmutableSeq<Term>, String> makeCall(RexCall call, ImmutableMap<Integer, ImmutableSeq<Term>> fields) {
        var func = call.getOperator();
        var retType = call.getType();
        var rawArgs = Seq.from(call.getOperands());
        var argType = rawArgs.map(RexNode::getType).map(RelDataType::getSqlTypeName);
        var args = rawArgs.map(arg -> translate(arg, fields))
                .foldLeft(Result.<ImmutableSeq<Term>, String>ok(ImmutableSeq.empty()), (res, smtArg) ->
                        res.flatMap(ats -> smtArg.map(ats::appendedAll)));
        if (args.isErr()) {
            return args;
        }
        var smtArgs = args.get().toArray(new Term[]{});
        if (func instanceof RuleBuilder.CosetteFunction cosFun) {
            if (retType instanceof RelType rlt) {
                Result<ImmutableSeq<SqlTypeName>, String> actualRetType = switch (rlt) {
                    case RelType.VarType vt -> Result.ok(typeDerivation.get(vt).elements().map(RelDataType::getSqlTypeName));
                    case RelType.BaseType bt when bt.getSqlTypeName().equals(SqlTypeName.BOOLEAN) -> Result.ok(ImmutableSeq.of());
                    default -> Result.err(String.format("Unsupported function in pattern: %s", func.getName()));
                };
                return actualRetType.flatMap(actualRetTypes -> actualRetTypes.mapIndexed((i, rt) -> declareFunction(cosFun.getName() + "-V" + i, argType, rt))
                        .foldLeft(Result.<ImmutableSeq<Term>, String>ok(ImmutableSeq.empty()), (res, fc) ->
                                res.flatMap(fcs -> fc.map(fcs::appended)))
                        .map(fcs -> fcs.map(fc -> solver.mkTerm(Kind.APPLY_UF, Seq.of(fc).appendedAll(smtArgs).toArray(new Term[]{})))));
            }
        } else {
            var funcKind = func.getKind();
            var retSort = getSort(retType.getSqlTypeName());
            var smtKind = Kind.UNDEFINED_KIND;
            if (retSort.isBoolean()) {
                smtKind = switch (funcKind) {
                    case EQUALS -> Kind.EQUAL;
                    case AND -> Kind.AND;
                    case OR -> Kind.OR;
                    case NOT -> Kind.NOT;
                    case GREATER_THAN -> Kind.GT;
                    case GREATER_THAN_OR_EQUAL -> Kind.GEQ;
                    case LESS_THAN -> Kind.LT;
                    case LESS_THAN_OR_EQUAL -> Kind.LEQ;
                    case ST_CONTAINS -> Kind.STRING_CONTAINS;
                    default -> Kind.UNDEFINED_KIND;
                };
            } else if (retSort.isInteger() || retSort.isReal()) {
                smtKind = switch (funcKind) {
                    case MINUS_PREFIX -> Kind.NEG;
                    case PLUS -> Kind.ADD;
                    case MINUS -> Kind.SUB;
                    case TIMES -> Kind.MULT;
                    case DIVIDE -> Kind.DIVISION;
                    case MOD -> retSort.isInteger() ? Kind.INTS_MODULUS : Kind.UNDEFINED_KIND;
                    case OTHER_FUNCTION -> switch (func.getName()) {
                        case "ABS" -> Kind.ABS;
                        case "CHAR_LENGTH", "CHARACTER_LENGTH" -> Kind.STRING_LENGTH;
                        default -> Kind.UNDEFINED_KIND;
                    };
                    default -> Kind.UNDEFINED_KIND;
                };
            } else if (retSort.isString()) {
                smtKind = switch (func.getName()) {
                    case "||" -> Kind.STRING_CONCAT;
                    case "SUBSTRING" -> Kind.STRING_SUBSTR;
                    default -> Kind.UNDEFINED_KIND;
                };
            }
            if (smtKind.equals(Kind.UNDEFINED_KIND)) {
                return switch (funcKind) {
                    case NOT_EQUALS -> Result.ok(ImmutableSeq.of(solver.mkTerm(Kind.NOT, solver.mkTerm(Kind.EQUAL, smtArgs))));
                    default -> declareFunction(func.getName() + "-U", argType, retType.getSqlTypeName())
                            .map(f -> ImmutableSeq.of(solver.mkTerm(Kind.APPLY_UF, Seq.of(f).appendedAll(smtArgs).toArray(new Term[]{}))));
                };
            } else {
                return Result.ok(ImmutableSeq.of(solver.mkTerm(smtKind, smtArgs)));
            }
        }
        return Result.err("Not Implemented");
    }

    public Result<Term, String> declareFunction(String name, Seq<SqlTypeName> inputTypes, SqlTypeName outputType) {
        var entry = declaredFunctions.getOption(name);
        if (entry.isEmpty()) {
            var synthFunc = solver.synthFun(name, inputTypes.map(ty -> solver.mkVar(getSort(ty))).toArray(new Term[]{}), getSort(outputType));
            declaredFunctions.put(name, Tuple.of(synthFunc, inputTypes.toImmutableSeq(), outputType));
            return Result.ok(synthFunc);
        } else {
            var declared = entry.get();
            return declared.component2().size() == inputTypes.size()
                    && declared.component2()
                    .mapIndexed((i, t) -> Tuple.of(t, inputTypes.get(i)))
                    .allMatch(p -> p.component1().equals(p.component2()))
                    && declared.component3().equals(outputType)
                    ? Result.ok(declared.component1())
                    : Result.err(String.format("Type signature mismatch for function %s", name));
        }
    }

    public Sort getSort(SqlTypeName sqlTypeName) {
        return switch (sqlTypeName) {
            case BOOLEAN -> solver.getBooleanSort();
            case TINYINT, INTEGER, SMALLINT, BIGINT -> solver.getIntegerSort();
            case DECIMAL, FLOAT, REAL, DOUBLE -> solver.getRealSort();
            case CHAR, VARCHAR -> solver.getStringSort();
            // WARNING: Uninterpreted sorts created in this way are always distinct,
            // EVEN IF THEY HAVE THE SAME NAME!
            default -> solver.mkUninterpretedSort(sqlTypeName.getName());
        };
    }
}
