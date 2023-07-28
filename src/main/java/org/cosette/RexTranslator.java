package org.cosette;

import io.github.cvc5.Kind;
import io.github.cvc5.Solver;
import io.github.cvc5.Sort;
import io.github.cvc5.Term;
import kala.collection.immutable.ImmutableMap;
import kala.collection.immutable.ImmutableSeq;
import kala.collection.mutable.MutableMap;
import kala.control.Result;
import kala.tuple.Tuple;
import kala.tuple.Tuple2;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Objects;

public record RexTranslator(Solver solver, Declarations declaredFunctions,
                            ImmutableMap<RelType.VarType, MatchEnv.ProductType> typeDerivation) {
    public static Result<RexTranslator, String> createTranslator(
            ImmutableMap<RelType.VarType, MatchEnv.ProductType> typeDerivation) {
        try {
            var solver = new Solver();
            solver.setOption("sygus", "true");
            solver.setLogic("ALL");
            return Result.ok(new RexTranslator(solver, new Declarations(MutableMap.create()), typeDerivation));
        } catch (Exception e) {
            return Result.err(String.format("Encountered error during CVC5 initialization: %s", e));
        }
    }

    public RexTranslator addConstraints(ImmutableSeq<Term> constraints) {
        constraints.forEach(solver::addSygusConstraint);
        return this;
    }

    public Result<RexTranslator, String> encode(MatchEnv.SynthesisConstraint constraint) {
        // TODO: Encode columns as variables and then call translate(), after which assert constraints.
        var synthVars = constraint.reference().sourceTypes().elements().mapIndexed(
                (i, t) -> solver.declareSygusVar(t.getSqlTypeName() + "-V" + i, getSort(t.getSqlTypeName())));
        var patternFields = constraint.reference().correspondence().map(cs -> cs.map(synthVars::get));
        var pattern = translate(constraint.pattern(), patternFields);
        if (pattern.isErr()) {
            return Result.err(pattern.getErr());
        }
        var targetFields = synthVars.map(ImmutableSeq::of);
        var target = constraint.target().map(t -> translate(t, targetFields))
                .foldLeft(Result.<ImmutableSeq<Term>, String>ok(ImmutableSeq.empty()),
                        (res, t) -> res.flatMap(fs -> t.map(fs::appendedAll)));
        if (target.isErr()) {
            return Result.err(target.getErr());
        }
        pattern.get().zip(target.get())
                .forEach(t -> solver.addSygusConstraint(solver.mkTerm(Kind.EQUAL, t.component1(), t.component2())));
        return Result.ok(this);
    }

    public Result<ImmutableSeq<Term>, String> translate(RexNode node, ImmutableSeq<ImmutableSeq<Term>> fields) {
        return switch (node) {
            case RexInputRef inputRef -> Result.ok(fields.get(inputRef.getIndex()));
            case RexLiteral literal -> Result.ok(ImmutableSeq.of(switch (literal.getType().getSqlTypeName()) {
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

    public Result<ImmutableSeq<Term>, String> makeCall(RexCall call, ImmutableSeq<ImmutableSeq<Term>> fields) {
        var func = call.getOperator();
        var args = ImmutableSeq.from(call.getOperands()).map(arg -> translate(arg, fields))
                .foldLeft(Result.<ImmutableSeq<Term>, String>ok(ImmutableSeq.empty()),
                        (res, smtArg) -> res.flatMap(ats -> smtArg.map(ats::appendedAll)));
        if (args.isErr()) {
            return args;
        }
        var smtArgs = args.get();
        var smtSorts = smtArgs.map(Term::getSort);
        var arrArgs = smtArgs.toArray(new Term[]{});
        if (func instanceof RuleBuilder.CosetteFunction cosFun) {
            Result<ImmutableSeq<SqlTypeName>, String> actualRetType = switch (cosFun.getReturnType()) {
                case RelType.VarType vt ->
                        Result.ok(typeDerivation.get(vt).elements().map(RelDataType::getSqlTypeName));
                case RelType.BaseType bt when bt.getSqlTypeName().equals(SqlTypeName.BOOLEAN) ->
                        Result.ok(ImmutableSeq.of(SqlTypeName.BOOLEAN));
                default -> Result.err(String.format("Unsupported function in pattern: %s", func.getName()));
            };
            return actualRetType.flatMap(rts -> declareFunction(cosFun.getName(), smtSorts, rts.map(this::getSort)).map(
                    fns -> fns.map(f -> solver.mkTerm(Kind.APPLY_UF,
                            ImmutableSeq.of(f).appendedAll(smtArgs).toArray(new Term[]{})))));
        } else {
            var funcKind = func.getKind();
            var retSort = getSort(call.getType().getSqlTypeName());
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
                    case NOT_EQUALS ->
                            Result.ok(ImmutableSeq.of(solver.mkTerm(Kind.NOT, solver.mkTerm(Kind.EQUAL, arrArgs))));
                    default -> declareFunction(func.getName(), smtSorts,
                            ImmutableSeq.of(getSort(call.getType().getSqlTypeName()))).map(f -> ImmutableSeq.of(
                            solver.mkTerm(Kind.APPLY_UF, f.appendedAll(smtArgs).toArray(new Term[]{}))));
                };
            } else {
                return Result.ok(ImmutableSeq.of(solver.mkTerm(smtKind, arrArgs)));
            }
        }
    }

    public Result<ImmutableSeq<Term>, String> declareFunction(String name, ImmutableSeq<Sort> inputSorts,
                                                              ImmutableSeq<Sort> outputSorts) {
        var entry = declaredFunctions.store.getOption(name);
        if (entry.isEmpty()) {
            var inputs = inputSorts.mapIndexed((i, s) -> solver.mkVar(s, "A" + i)).toArray(new Term[]{});
            var functions = outputSorts.mapIndexed((i, s) -> Tuple.of(solver.synthFun(name + "-C" + i, inputs, s), s));
            declaredFunctions.store.put(name, Tuple.of(functions, inputSorts));
            return Result.ok(functions.map(Tuple2::component1));
        } else {
            var declared = entry.get();
            var declaredInputs = declared.component2();
            var declaredOutputs = declared.component1();
            return declaredInputs.size() == inputSorts.size() &&
                    declaredInputs.zip(inputSorts).allMatch(t -> t.component1().equals(t.component2())) &&
                    declaredOutputs.size() == outputSorts.size() && declaredOutputs.zip(outputSorts).stream()
                    .allMatch(t -> t.component1().component2().equals(t.component2())) ?
                    Result.ok(declaredOutputs.map(Tuple2::component1)) :
                    Result.err(String.format("Type signature mismatch for function: %s", name));
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

    public record Declarations(MutableMap<String, Tuple2<ImmutableSeq<Tuple2<Term, Sort>>, ImmutableSeq<Sort>>> store) {
    }
}
