package norm

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func (c *CustomFuncs) HasConstantGroupingCols(
	input memo.RelExpr, groupingPrivate *memo.GroupingPrivate,
) bool {
	_, _, _, ok := c.extractConstantGroupingColsAndBuildDummy(input, groupingPrivate)
	return ok
}

func (c *CustomFuncs) extractConstantGroupingColsAndBuildDummy(
	input memo.RelExpr, groupingPrivate *memo.GroupingPrivate,
) (constantCols opt.ColSet, constantValues memo.ScalarListExpr, dummyCols opt.ColList, ok bool) {
	project, ok := input.(*memo.ProjectExpr)
	if !ok {
		return opt.ColSet{}, nil, nil, false
	}

	groupingCols := groupingPrivate.GroupingCols
	constantCols = opt.ColSet{}
	constantValues = make(memo.ScalarListExpr, 0)

	for i := range project.Projections {
		item := &project.Projections[i]
		if groupingCols.Contains(item.Col) && opt.IsConstValueOp(item.Element) {
			constantCols.Add(item.Col)
			constantValues = append(constantValues, item.Element)
		}
	}

	if constantCols.Empty() {
		return opt.ColSet{}, nil, nil, false
	}

	md := c.mem.Metadata()
	dummyCols = make(opt.ColList, len(constantValues))
	for i := range constantValues {
		dummyCols[i] = md.AddColumn("", constantValues[i].DataType())
	}

	return constantCols, constantValues, dummyCols, true
}

func (c *CustomFuncs) GetConstantGroupingCols(
	input memo.RelExpr, groupingPrivate *memo.GroupingPrivate,
) opt.ColSet {
	constantCols, _, _, _ := c.extractConstantGroupingColsAndBuildDummy(input, groupingPrivate)
	return constantCols
}

func (c *CustomFuncs) ExtractMatchingConstantsFromUnion(
	leftProjections memo.ProjectionsExpr,
	rightProjections memo.ProjectionsExpr,
	leftCols opt.ColList,
	rightCols opt.ColList,
	outCols opt.ColList,
) (constantPositions []int, constantValues memo.ScalarListExpr, ok bool) {
	if len(leftCols) != len(rightCols) || len(leftCols) != len(outCols) {
		return nil, nil, false
	}

	leftColToProj := make(map[opt.ColumnID]int)
	for i := range leftProjections {
		leftColToProj[leftProjections[i].Col] = i
	}
	rightColToProj := make(map[opt.ColumnID]int)
	for i := range rightProjections {
		rightColToProj[rightProjections[i].Col] = i
	}

	constantPositions = make([]int, 0)
	constantValues = make(memo.ScalarListExpr, 0)

	for outIdx := range outCols {
		leftCol := leftCols[outIdx]
		rightCol := rightCols[outIdx]

		leftProjIdx, leftHasProj := leftColToProj[leftCol]
		rightProjIdx, rightHasProj := rightColToProj[rightCol]

		if !leftHasProj || !rightHasProj {
			continue
		}

		leftItem := &leftProjections[leftProjIdx]
		rightItem := &rightProjections[rightProjIdx]

		if opt.IsConstValueOp(leftItem.Element) && opt.IsConstValueOp(rightItem.Element) {
			if c.IsConstValueEqual(leftItem.Element, rightItem.Element) {
				constantPositions = append(constantPositions, outIdx)
				constantValues = append(constantValues, leftItem.Element)
			}
		}
	}

	if len(constantPositions) == 0 {
		return nil, nil, false
	}

	return constantPositions, constantValues, true
}

func (c *CustomFuncs) MakeColSetFromPositions(
	positions []int,
	colList opt.ColList,
) opt.ColSet {
	result := opt.ColSet{}
	for _, pos := range positions {
		if pos < len(colList) {
			result.Add(colList[pos])
		}
	}
	return result
}

func (c *CustomFuncs) ComputeNeededColsForUnionPullUp(
	constantPositions []int,
	outCols opt.ColList,
) opt.ColSet {
	constantCols := c.MakeColSetFromPositions(constantPositions, outCols)
	return outCols.ToSet().Difference(constantCols)
}

func (c *CustomFuncs) AddConstantsToProjections(
	constantPositions []int,
	constantValues memo.ScalarListExpr,
	outCols opt.ColList,
) memo.ProjectionsExpr {
	if len(constantPositions) != len(constantValues) {
		panic(errors.AssertionFailedf("constantPositions and constantValues must have same length"))
	}

	projections := make(memo.ProjectionsExpr, 0, len(constantPositions))
	for i, pos := range constantPositions {
		if pos >= len(outCols) {
			panic(errors.AssertionFailedf("position %d out of range for outCols", pos))
		}
		outCol := outCols[pos]
		projections = append(projections, c.f.ConstructProjectionsItem(constantValues[i], outCol))
	}

	return projections
}

func (c *CustomFuncs) HasMatchingConstantsFromUnion(
	leftProjections memo.ProjectionsExpr,
	rightProjections memo.ProjectionsExpr,
	leftCols opt.ColList,
	rightCols opt.ColList,
	outCols opt.ColList,
) bool {
	_, _, ok := c.ExtractMatchingConstantsFromUnion(leftProjections, rightProjections, leftCols, rightCols, outCols)
	return ok
}

func (c *CustomFuncs) UnionPullUpConstantsReplace(
	left memo.RelExpr,
	right memo.RelExpr,
	leftProjections memo.ProjectionsExpr,
	rightProjections memo.ProjectionsExpr,
	private *memo.SetPrivate,
	leftInput memo.RelExpr,
	rightInput memo.RelExpr,
	leftPassthrough opt.ColSet,
	rightPassthrough opt.ColSet,
) memo.RelExpr {
	constantPositions, constantValues, ok := c.ExtractMatchingConstantsFromUnion(
		leftProjections, rightProjections,
		private.LeftCols, private.RightCols, private.OutCols,
	)
	if !ok {
		panic(errors.AssertionFailedf("HasMatchingConstantsFromUnion should have returned true"))
	}
	neededCols := c.ComputeNeededColsForUnionPullUp(constantPositions, private.OutCols)
	leftProject := left.(*memo.ProjectExpr)
	rightProject := right.(*memo.ProjectExpr)

	neededLeftCols := c.NeededColMapLeft(neededCols, private)
	neededRightCols := c.NeededColMapRight(neededCols, private)
	prunedLeft := c.PruneCols(leftProject.Input, neededLeftCols)
	prunedRight := c.PruneCols(rightProject.Input, neededRightCols)

	adjustedPrivate := c.PruneSetPrivate(neededCols, private)

	union := c.f.ConstructUnionAll(prunedLeft, prunedRight, adjustedPrivate)

	unionOutputCols := adjustedPrivate.OutCols
	mergedProjections := make(memo.ProjectionsExpr, 0, len(private.OutCols))
	
	constantProjMap := make(map[opt.ColumnID]memo.ProjectionsItem)
	for i, pos := range constantPositions {
		if pos < len(private.OutCols) {
			outCol := private.OutCols[pos]
			constantProjMap[outCol] = c.f.ConstructProjectionsItem(constantValues[i], outCol)
		}
	}
	
	unionColIdx := 0
	for _, outCol := range private.OutCols {
		if constantProj, isConst := constantProjMap[outCol]; isConst {
			mergedProjections = append(mergedProjections, constantProj)
		} else if unionColIdx < len(unionOutputCols) {
			unionCol := unionOutputCols[unionColIdx]
			mergedProjections = append(mergedProjections, c.f.ConstructProjectionsItem(
				c.f.ConstructVariable(unionCol),
				outCol,
			))
			unionColIdx++
		}
	}

	return c.f.ConstructProject(union, mergedProjections, opt.ColSet{})
}

func (c *CustomFuncs) ColListToSet(colList opt.ColList) opt.ColSet {
	return colList.ToSet()
}

func (c *CustomFuncs) CanCommuteJoin(left, right memo.RelExpr) bool {
	return c.OutputCols(left).Len() <= c.OutputCols(right).Len()
}

func (c *CustomFuncs) SwapJoinOutputColumns(
	leftCols opt.ColSet,
	rightCols opt.ColSet,
) memo.ProjectionsExpr {
	projections := make(memo.ProjectionsExpr, 0, leftCols.Len()+rightCols.Len())
	md := c.mem.Metadata()

	for col, ok := rightCols.Next(0); ok; col, ok = rightCols.Next(col + 1) {
		colMeta := md.ColumnMeta(col)
		newCol := md.AddColumn(colMeta.Alias, colMeta.Type)
		projections = append(projections, c.f.ConstructProjectionsItem(
			c.f.ConstructVariable(col),
			newCol,
		))
	}

	for col, ok := leftCols.Next(0); ok; col, ok = leftCols.Next(col + 1) {
		colMeta := md.ColumnMeta(col)
		newCol := md.AddColumn(colMeta.Alias, colMeta.Type)
		projections = append(projections, c.f.ConstructProjectionsItem(
			c.f.ConstructVariable(col),
			newCol,
		))
	}
	
	return projections
}

func (c *CustomFuncs) HasBoundConditions(
	filters memo.FiltersExpr,
	leftCols opt.ColSet,
	rightCols opt.ColSet,
) bool {
	for i := range filters {
		if c.IsBoundBy(&filters[i], leftCols) || c.IsBoundBy(&filters[i], rightCols) {
			return true
		}
	}
	return false
}

func (c *CustomFuncs) IsFilterTrue(filters memo.FiltersExpr) bool {
	if len(filters) == 0 {
		return true
	}
	for i := range filters {
		condition := filters[i].Condition
		if condition.Op() != opt.TrueOp {
			return false
		}
	}
	return true
}

func (c *CustomFuncs) CanExtractJoinFilter(
	left memo.RelExpr,
	right memo.RelExpr,
	on memo.FiltersExpr,
) bool {
	if c.IsFilterTrue(on) || c.IsFilterEmpty(on) {
		return false
	}
	hasNonTrueCondition := false
	for i := range on {
		if on[i].Condition.Op() != opt.TrueOp {
			hasNonTrueCondition = true
			break
		}
	}
	if !hasNonTrueCondition {
		return false
	}
	return true
}

func (c *CustomFuncs) CanExtractProjectFromAggregate(
	aggregations memo.AggregationsExpr,
) bool {
	for i := range aggregations {
		agg := aggregations[i].Agg
		if agg.ChildCount() > 0 {
			arg := agg.Child(0)
			if scalarArg, ok := arg.(opt.ScalarExpr); ok {
				if _, ok := scalarArg.(*memo.VariableExpr); !ok {
					return true
				}
			}
		}
	}
	return false
}

func (c *CustomFuncs) ExtractProjectFromAggregate(
	input memo.RelExpr,
	aggregations memo.AggregationsExpr,
) memo.RelExpr {
	inputCols := c.OutputCols(input)

	var pb projectBuilder
	pb.init(c, inputCols)

	for i := range aggregations {
		agg := aggregations[i].Agg
		if agg.ChildCount() > 0 {
			arg := agg.Child(0)
			if scalarArg, ok := arg.(opt.ScalarExpr); ok {
				pb.add(scalarArg)
			}
		}
	}
	
	return pb.buildProject(input)
}

func (c *CustomFuncs) RemapAggregationsAfterExtractProject(
	aggregations memo.AggregationsExpr,
	input memo.RelExpr,
) memo.AggregationsExpr {
	inputCols := c.OutputCols(input)
	var pb projectBuilder
	pb.init(c, inputCols)

	exprToVar := make(map[opt.ScalarExpr]opt.ScalarExpr)
	for i := range aggregations {
		agg := aggregations[i].Agg
		if agg.ChildCount() > 0 {
			arg := agg.Child(0)
			if scalarArg, ok := arg.(opt.ScalarExpr); ok {
				if _, exists := exprToVar[scalarArg]; !exists {
					varExpr := pb.add(scalarArg)
					exprToVar[scalarArg] = varExpr
				}
			}
		}
	}

	newAggs := make(memo.AggregationsExpr, len(aggregations))
	for i := range aggregations {
		agg := aggregations[i].Agg
		var newArg opt.ScalarExpr
		
		if agg.ChildCount() > 0 {
			arg := agg.Child(0)
			if scalarArg, ok := arg.(opt.ScalarExpr); ok {
				if varExpr, exists := exprToVar[scalarArg]; exists {
					newArg = varExpr
				} else {
					newArg = scalarArg
				}
			} else {
				newArg = nil
			}
		}

		var newAgg opt.ScalarExpr
		if newArg != nil && agg.ChildCount() > 0 {
			var replace ReplaceFunc
			replace = func(e opt.Expr) opt.Expr {
				if e == agg.Child(0) {
					return newArg
				}
				return c.f.Replace(e, replace)
			}
			newAgg = replace(agg).(opt.ScalarExpr)
		} else {
			newAgg = agg
		}
		
		newAggs[i] = c.f.ConstructAggregationsItem(newAgg, aggregations[i].Col)
	}
	
	return newAggs
}

func (c *CustomFuncs) IsSemiJoin(input memo.RelExpr) bool {
	switch input.Op() {
	case opt.SemiJoinOp, opt.SemiJoinApplyOp:
		return true
	default:
		return false
	}
}
