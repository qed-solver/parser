package org.qed;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import kala.collection.Map;
import kala.collection.Seq;

import java.io.File;
import java.io.IOException;

public interface RRule {
    RelRN before();

    RelRN after();

    default String explain() {
        return getClass().getName()
                + "\n"
                + before().semantics().explain()
                + "=>"
                + "\n"
                + after().semantics().explain();
    }

    default String name() {
        return getClass().getSimpleName();
    }

    default String info() {
        return "";
    }

    default ObjectNode toJson() {
        return JSONSerializer.serialize(Seq.of(before().semantics(), after().semantics()));
    }

    default void dump(String path) throws IOException {
        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(new File(path), toJson());
    }

    interface RRuleFamily {
        Seq<RRule> family();
    }

    record RRuleGenerator(RRule rule,
                          Seq<MetaAssignment> assignments) implements RRuleFamily {
        @Override
        public Seq<RRule> family() {
            return assignments.map(assignment -> new RRule() {

                @Override
                public RelRN before() {
                    return assignment.replaceMetaRelRN(rule.before());
                }

                @Override
                public RelRN after() {
                    return assignment.replaceMetaRelRN(rule.after());
                }

                @Override
                public String name() {
                    return rule.name();
                }

                @Override
                public String info() {
                    return assignment.info();
                }
            });
        }

        public record MetaAssignment(
                Map<RelRN.Join.JoinType.MetaJoinType, RelRN.Join.JoinType.ConcreteJoinType> joinTypeAssignment) {
            public RelRN.Join.JoinType replaceMetaJoinType(RelRN.Join.JoinType joinType) {
                return switch (joinType) {
                    case RelRN.Join.JoinType.MetaJoinType metaJoinType -> joinTypeAssignment.get(metaJoinType);
                    default -> joinType;
                };
            }

            public RexRN replaceMetaRexRN(RexRN rexRN) {
                return switch (rexRN) {
                    case RexRN.Field field -> replaceMetaRelRN(field.source()).field(field.ordinal());
                    default -> customReplaceMetaRexRN(rexRN);
                };
            }

            public RelRN replaceMetaRelRN(RelRN relRN) {
                return switch (relRN) {
                    case RelRN.Filter filter ->
                            replaceMetaRelRN(filter.source()).filter(replaceMetaRexRN(filter.cond()));
                    case RelRN.Join join ->
                            replaceMetaRelRN(join.left()).join(replaceMetaJoinType(join.ty()),
                                    replaceMetaRexRN(join.cond()), replaceMetaRelRN(join.right()));
                    default -> customReplaceMetaRelRN(relRN);
                };
            }

            public RexRN customReplaceMetaRexRN(RexRN rexRN) {
                return rexRN;
            }

            public RelRN customReplaceMetaRelRN(RelRN relRN) {
                return relRN;
            }

            public String info() {
                return joinTypeAssignment.joinToString("&", (m, c) -> "{" + m.name() + "}=" + c.semantics());
            }

        }
    }
}

