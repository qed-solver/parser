package org.cosette;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.Litmus;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class RelVariable implements RelNode {

    private static final AtomicInteger ID = new AtomicInteger(0);

    private final int id;
    private final RelOptCluster cluster;
    private final RelTraitSet traits;

    public RelVariable(RelOptCluster cluster) {
        this.cluster = cluster;
        this.traits = cluster.traitSet();
        id = ID.getAndIncrement();
    }

    @Override
    public @Nullable Convention getConvention() {
        return null;
    }

    @Override
    public @Nullable String getCorrelVariable() {
        return null;
    }

    @Override
    public RelNode getInput(int i) {
        return null;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public RelTraitSet getTraitSet() {
        return traits;
    }

    @Override
    public RelDataType getRowType() {
        return new RelRecordType(new ArrayList<>());
    }

    @Override
    public String getDescription() {
        return null;
    }

    @Override
    public RelDataType getExpectedInputRowType(int ordinalInParent) {
        return null;
    }

    @Override
    public List<RelNode> getInputs() {
        return List.of();
    }

    @Override
    public RelOptCluster getCluster() {
        return cluster;
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return 0;
    }

    @Override
    public Set<CorrelationId> getVariablesSet() {
        return Set.of();
    }

    @Override
    public void collectVariablesUsed(Set<CorrelationId> variableSet) {

    }

    @Override
    public void collectVariablesSet(Set<CorrelationId> variableSet) {

    }

    @Override
    public void childrenAccept(RelVisitor visitor) {

    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return null;
    }

    @Override
    public <M extends Metadata> M metadata(Class<M> metadataClass, RelMetadataQuery mq) {
        return null;
    }

    @Override
    public void explain(RelWriter pw) {
        pw.item("id", id).done(this);
    }

    @Override
    public RelNode onRegister(RelOptPlanner planner) {
        return null;
    }

    @Override
    public RelDigest getRelDigest() {
        return null;
    }

    @Override
    public void recomputeDigest() {

    }

    @Override
    public boolean deepEquals(@Nullable Object obj) {
        return false;
    }

    @Override
    public int deepHashCode() {
        return id;
    }

    @Override
    public void replaceInput(int ordinalInParent, RelNode p) {

    }

    @Override
    public @Nullable RelOptTable getTable() {
        return null;
    }

    @Override
    public String getRelTypeName() {
        return "LogicalVariable";
    }

    @Override
    public boolean isValid(Litmus litmus, @Nullable Context context) {
        return false;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return null;
    }

    @Override
    public void register(RelOptPlanner planner) {

    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        if (shuttle instanceof RelJSONShuttle) {
            return ((RelJSONShuttle) shuttle).visit(this);
        }
        return null;
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        return null;
    }
}
