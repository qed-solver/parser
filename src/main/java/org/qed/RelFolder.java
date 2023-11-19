package org.qed;

import kala.collection.Seq;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;

import java.util.function.Function;

public interface RelFolder extends Function<RelNode, RelNode> {
    RelNode post(RelNode rel);

    default RelNode apply(RelNode rel) {
        class RexFolder extends RexShuttle {
            @Override
            public RexNode visitSubQuery(RexSubQuery subQuery) {
                return super.visitSubQuery(subQuery.clone(RelFolder.this.apply(subQuery.rel)));
            }
        }
        var newRel = rel.accept(new RexFolder());
        var inputs = Seq.from(newRel.getInputs()).map(this).asJava();
        return post(newRel.copy(newRel.getTraitSet(), inputs));
    }

}
