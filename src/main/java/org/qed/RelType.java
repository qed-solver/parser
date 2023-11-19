package org.qed;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

public sealed interface RelType extends RelDataType {
    final class VarType extends RelDataTypeImpl implements RelType {
        private final String name;
        private final boolean nullable;

        public VarType(String typeName, boolean nullability) {
            name = typeName;
            nullable = nullability;
            computeDigest();
        }

        /*
         * Notice: All virtual types will be translated to integer for prover
         **/
        @Override
        protected void generateTypeString(StringBuilder sb, boolean withDetail) {
            sb.append(name);
            if (withDetail) {
                sb.append(": ").append(nullable ? "nullable" : "");
            }
        }

        @Override
        public boolean isNullable() {
            return nullable;
        }
    }

    final class BaseType extends BasicSqlType implements RelType {
        public BaseType(SqlTypeName typeName, boolean nullable) {
            super(RelDataTypeSystem.DEFAULT, typeName, nullable);
        }
    }

    static RelType fromString(String name, boolean nullable) {
        for (var tn : SqlTypeName.values()) {
            if (tn.getName().equals(name)) {
                return new BaseType(tn, nullable);
            }
        }
        return new VarType(name, nullable);
    }
}
