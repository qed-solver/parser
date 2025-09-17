package org.qed.Backends.MySQL;

import org.qed.*;
import java.util.HashMap;
import java.util.Map;

public class MySQLGenerator implements CodeGenerator<MySQLGenerator.MySQLEnv> {
    
    public static class MySQLEnv {
        StringBuilder pattern = new StringBuilder();
        StringBuilder replacement = new StringBuilder();
        Map<String, Integer> predicatePositions = new HashMap<>();
        Map<String, String> tableAliases = new HashMap<>();
        int aliasCounter = 0;
        int predicateCounter = 0;
        String ruleName;
        
        public MySQLEnv(String ruleName) {
            this.ruleName = ruleName;
        }
        
        String nextAlias() {
            return "t" + (++aliasCounter);
        }
        
        int registerPredicate(String name) {
            if (!predicatePositions.containsKey(name)) {
                predicatePositions.put(name, predicateCounter++);
            }
            return predicatePositions.get(name);
        }
    }
    
    @Override
    public MySQLEnv preMatch(String rulename) {
        return new MySQLEnv(rulename);
    }
    
    @Override
    public MySQLEnv onMatchScan(MySQLEnv env, RelRN.Scan scan) {
        // Store table name - using scan.name() as identifier
        env.tableAliases.put(scan.name(), "test_table");
        return env;
    }
    
    @Override
    public MySQLEnv onMatchFilter(MySQLEnv env, RelRN.Filter filter) {
        // Build pattern for filter using source() method (not input())
        if (filter.source() instanceof RelRN.Filter innerFilter) {
            // Nested filter - create nested SELECT pattern
            env.pattern.append("SELECT * FROM ( ");
            onMatchFilter(env, innerFilter);
            env.pattern.append(" ) AS ").append(env.nextAlias());
            env.pattern.append(" WHERE ");
            onMatch(env, filter.cond());
        } else if (filter.source() instanceof RelRN.Scan scan) {
            // Direct filter on scan
            env.pattern.append("SELECT * FROM ");
            env.pattern.append("test_table"); // Use generic table name for pattern
            env.pattern.append(" WHERE ");
            onMatch(env, filter.cond());
        } else {
            // Handle other input types
            env.pattern.append("SELECT * FROM ( ");
            onMatch(env, filter.source());
            env.pattern.append(" ) AS ").append(env.nextAlias());
            env.pattern.append(" WHERE ");
            onMatch(env, filter.cond());
        }
        return env;
    }
    
    @Override
    public MySQLEnv onMatchPred(MySQLEnv env, RexRN.Pred pred) {
        // Predicates become ? placeholders
        env.pattern.append("?");
        env.registerPredicate(pred.operator().getName());
        return env;
    }
    
    @Override
    public MySQLEnv onMatchAnd(MySQLEnv env, RexRN.And and) {
        boolean first = true;
        for (var source : and.sources()) {
            if (!first) {
                env.pattern.append(" AND ");
            }
            onMatch(env, source);
            first = false;
        }
        return env;
    }
    
    @Override
    public MySQLEnv onMatchOr(MySQLEnv env, RexRN.Or or) {
        env.pattern.append("(");
        boolean first = true;
        for (var source : or.sources()) {
            if (!first) {
                env.pattern.append(" OR ");
            }
            onMatch(env, source);
            first = false;
        }
        env.pattern.append(")");
        return env;
    }
    
    @Override
    public MySQLEnv transformScan(MySQLEnv env, RelRN.Scan scan) {
        String tableName = env.tableAliases.get(scan.name());
        if (tableName == null) {
            tableName = "test_table"; // Default table name for pattern
        }
        env.replacement.append(tableName);
        return env;
    }
    
    @Override
    public MySQLEnv transformFilter(MySQLEnv env, RelRN.Filter filter) {
        env.replacement.append("SELECT * FROM ");
        transform(env, filter.source());
        env.replacement.append(" WHERE ");
        transform(env, filter.cond());
        return env;
    }
    
    @Override
    public MySQLEnv transformPred(MySQLEnv env, RexRN.Pred pred) {
        // In replacement, maintain the same placeholder position
        env.replacement.append("?");
        return env;
    }
    
    @Override
    public MySQLEnv transformAnd(MySQLEnv env, RexRN.And and) {
        boolean first = true;
        for (var source : and.sources()) {
            if (!first) {
                env.replacement.append(" AND ");
            }
            transform(env, source);
            first = false;
        }
        return env;
    }
    
    @Override
    public MySQLEnv transformOr(MySQLEnv env, RexRN.Or or) {
        env.replacement.append("(");
        boolean first = true;
        for (var source : or.sources()) {
            if (!first) {
                env.replacement.append(" OR ");
            }
            transform(env, source);
            first = false;
        }
        env.replacement.append(")");
        return env;
    }
    
    @Override
    public String translate(String name, MySQLEnv onMatch, MySQLEnv transform) {
        StringBuilder sql = new StringBuilder();
        sql.append("SET GLOBAL rewriter_enabled = ON;\n\n");
        sql.append("INSERT INTO query_rewrite.rewrite_rules (pattern, replacement, enabled, message)\n");
        sql.append("VALUES(\n");
        sql.append("    '").append(onMatch.pattern.toString()).append("',\n");
        sql.append("    '").append(transform.replacement.toString()).append("',\n");
        sql.append("    'YES',\n");
        sql.append("    '").append(name).append("'\n");
        sql.append(");\n\n");
        sql.append("CALL query_rewrite.flush_rewrite_rules();\n\n");
        return sql.toString();
    }
    
    @Override
    public MySQLEnv onMatchProject(MySQLEnv env, RelRN.Project project) {
        env.pattern.append("SELECT ");
        // For now, just use * for simplicity in patterns
        env.pattern.append("*");
        env.pattern.append(" FROM ");
        onMatch(env, project.source());
        return env;
    }
    
    @Override
    public MySQLEnv transformProject(MySQLEnv env, RelRN.Project project) {
        env.replacement.append("SELECT ");
        // For now, just use * for simplicity
        env.replacement.append("*");
        env.replacement.append(" FROM ");
        transform(env, project.source());
        return env;
    }
    
    @Override
    public MySQLEnv onMatchUnion(MySQLEnv env, RelRN.Union union) {
        boolean first = true;
        for (var source : union.sources()) {
            if (!first) {
                env.pattern.append(" UNION ");
                if (union.all()) {
                    env.pattern.append("ALL ");
                }
            }
            env.pattern.append("(");
            onMatch(env, source);
            env.pattern.append(")");
            first = false;
        }
        return env;
    }
    
    @Override
    public MySQLEnv transformUnion(MySQLEnv env, RelRN.Union union) {
        boolean first = true;
        for (var source : union.sources()) {
            if (!first) {
                env.replacement.append(" UNION ");
                if (union.all()) {
                    env.replacement.append("ALL ");
                }
            }
            env.replacement.append("(");
            transform(env, source);
            env.replacement.append(")");
            first = false;
        }
        return env;
    }
    
    @Override
    public MySQLEnv onMatchIntersect(MySQLEnv env, RelRN.Intersect intersect) {
        boolean first = true;
        for (var source : intersect.sources()) {
            if (!first) {
                env.pattern.append(" INTERSECT ");
                if (intersect.all()) {
                    env.pattern.append("ALL ");
                }
            }
            env.pattern.append("(");
            onMatch(env, source);
            env.pattern.append(")");
            first = false;
        }
        return env;
    }
    
    @Override
    public MySQLEnv transformIntersect(MySQLEnv env, RelRN.Intersect intersect) {
        boolean first = true;
        for (var source : intersect.sources()) {
            if (!first) {
                env.replacement.append(" INTERSECT ");
                if (intersect.all()) {
                    env.replacement.append("ALL ");
                }
            }
            env.replacement.append("(");
            transform(env, source);
            env.replacement.append(")");
            first = false;
        }
        return env;
    }
    
    @Override
    public MySQLEnv onMatchMinus(MySQLEnv env, RelRN.Minus minus) {
        boolean first = true;
        for (var source : minus.sources()) {
            if (!first) {
                env.pattern.append(" EXCEPT ");
                if (minus.all()) {
                    env.pattern.append("ALL ");
                }
            }
            env.pattern.append("(");
            onMatch(env, source);
            env.pattern.append(")");
            first = false;
        }
        return env;
    }
    
    @Override
    public MySQLEnv transformMinus(MySQLEnv env, RelRN.Minus minus) {
        boolean first = true;
        for (var source : minus.sources()) {
            if (!first) {
                env.replacement.append(" EXCEPT ");
                if (minus.all()) {
                    env.replacement.append("ALL ");
                }
            }
            env.replacement.append("(");
            transform(env, source);
            env.replacement.append(")");
            first = false;
        }
        return env;
    }
    
    @Override
    public MySQLEnv onMatchJoin(MySQLEnv env, RelRN.Join join) {
        // Note: MySQL Query Rewriter has limited support for complex JOIN patterns
        // This is a simplified implementation
        env.pattern.append("SELECT * FROM ");
        onMatch(env, join.left());
        env.pattern.append(" JOIN ");
        onMatch(env, join.right());
        env.pattern.append(" ON ");
        onMatch(env, join.cond());
        return env;
    }
    
    @Override
    public MySQLEnv transformJoin(MySQLEnv env, RelRN.Join join) {
        env.replacement.append("SELECT * FROM ");
        transform(env, join.left());
        String joinType = switch (join.ty().semantics()) {
            case INNER -> " INNER JOIN ";
            case LEFT -> " LEFT JOIN ";
            case RIGHT -> " RIGHT JOIN ";
            case FULL -> " FULL OUTER JOIN ";
            default -> " JOIN ";
        };
        env.replacement.append(joinType);
        transform(env, join.right());
        env.replacement.append(" ON ");
        transform(env, join.cond());
        return env;
    }
    
    @Override
    public MySQLEnv onMatchAggregate(MySQLEnv env, RelRN.Aggregate aggregate) {
        // Simplified aggregate pattern
        env.pattern.append("SELECT * FROM ");
        onMatch(env, aggregate.source());
        env.pattern.append(" GROUP BY ?");
        return env;
    }
    
    @Override
    public MySQLEnv transformAggregate(MySQLEnv env, RelRN.Aggregate aggregate) {
        env.replacement.append("SELECT * FROM ");
        transform(env, aggregate.source());
        env.replacement.append(" GROUP BY ?");
        return env;
    }
}