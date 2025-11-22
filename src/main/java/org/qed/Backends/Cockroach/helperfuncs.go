package norm

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func (c *CustomFuncs) HasConstantGroupingCols(
	input memo.RelExpr, groupingPrivate *memo.GroupingPrivate,
) bool {
	_, _, _, ok := c.ExtractConstantGroupingColsAndBuildDummy(input, groupingPrivate)
	return ok
}

func (c *CustomFuncs) ExtractConstantGroupingColsAndBuildDummy(
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
	constantCols, _, _, _ := c.ExtractConstantGroupingColsAndBuildDummy(input, groupingPrivate)
	return constantCols
}

func (c *CustomFuncs) GetConstantValues(
	input memo.RelExpr, groupingPrivate *memo.GroupingPrivate,
) memo.ScalarListExpr {
	_, constantValues, _, _ := c.ExtractConstantGroupingColsAndBuildDummy(input, groupingPrivate)
	return constantValues
}

func (c *CustomFuncs) GetDummyCols(
	input memo.RelExpr, groupingPrivate *memo.GroupingPrivate,
) opt.ColList {
	_, _, dummyCols, _ := c.ExtractConstantGroupingColsAndBuildDummy(input, groupingPrivate)
	return dummyCols
}

func (c *CustomFuncs) ConstructDummyValuesTable(
	constantValues memo.ScalarListExpr, dummyCols opt.ColList,
) memo.RelExpr {
	if len(constantValues) == 0 || len(dummyCols) == 0 {
		panic(errors.AssertionFailedf("ConstructDummyValuesTable called with empty constantValues or dummyCols"))
	}
	tupleTypes := make([]*types.T, len(constantValues))
	for i := range constantValues {
		tupleTypes[i] = constantValues[i].DataType()
	}
	tupleTyp := types.MakeTuple(tupleTypes)
	tuple := c.f.ConstructTuple(constantValues, tupleTyp)
	rows := memo.ScalarListExpr{tuple}
	return c.f.ConstructValues(rows, &memo.ValuesPrivate{
		Cols: dummyCols,
		ID:   c.mem.Metadata().NextUniqueID(),
	})
}

func (c *CustomFuncs) RemapProjectionsForDummyJoin(
	projections memo.ProjectionsExpr,
	constantCols opt.ColSet,
	dummyCols opt.ColList,
) memo.ProjectionsExpr {
	constantColList := constantCols.ToList()
	constantToDummy := make(map[opt.ColumnID]opt.ColumnID)
	for i := range constantColList {
		if i < len(dummyCols) {
			constantToDummy[constantColList[i]] = dummyCols[i]
		}
	}

	newProjections := make(memo.ProjectionsExpr, 0, len(projections))
	for i := range projections {
		item := &projections[i]
		if dummyCol, ok := constantToDummy[item.Col]; ok {
			newProjections = append(newProjections, c.f.ConstructProjectionsItem(
				c.f.ConstructVariable(dummyCol),
				item.Col,
			))
		} else {
			newProjections = append(newProjections, *item)
		}
	}
	return newProjections
}

func (c *CustomFuncs) RemapGroupingColsForDummyJoin(
	groupingPrivate *memo.GroupingPrivate,
	constantCols opt.ColSet,
	dummyCols opt.ColList,
) *memo.GroupingPrivate {
	constantColList := constantCols.ToList()
	constantToDummy := make(map[opt.ColumnID]opt.ColumnID)
	for i := range constantColList {
		if i < len(dummyCols) {
			constantToDummy[constantColList[i]] = dummyCols[i]
		}
	}

	newGroupingCols := opt.ColSet{}
	for col, ok := groupingPrivate.GroupingCols.Next(0); ok; col, ok = groupingPrivate.GroupingCols.Next(col + 1) {
		if dummyCol, isConst := constantToDummy[col]; isConst {
			newGroupingCols.Add(dummyCol)
		} else {
			newGroupingCols.Add(col)
		}
	}

	newPrivate := *groupingPrivate
	newPrivate.GroupingCols = newGroupingCols
	return &newPrivate
}

func (c *CustomFuncs) ConstructAggregateProjectConstantToDummyJoin(
	input memo.RelExpr,
	aggregations memo.AggregationsExpr,
	groupingPrivate *memo.GroupingPrivate,
) memo.RelExpr {
	project := input.(*memo.ProjectExpr)

	constantCols, constantValues, dummyCols, ok := c.ExtractConstantGroupingColsAndBuildDummy(project, groupingPrivate)
	if !ok {
		panic(errors.AssertionFailedf("should have matched"))
	}

	values := c.ConstructDummyValuesTable(constantValues, dummyCols)

	joinPrivate := &memo.JoinPrivate{}

	filters := memo.FiltersExpr{{Condition: c.f.ConstructTrue()}}
	join := c.f.ConstructInnerJoin(project.Input, values, filters, joinPrivate)

	newProjections := c.RemapProjectionsForDummyJoin(project.Projections, constantCols, dummyCols)
	newProject := c.f.ConstructProject(join, newProjections, project.Passthrough)

	newGroupingPrivate := c.RemapGroupingColsForDummyJoin(groupingPrivate, constantCols, dummyCols)
	return c.f.ConstructGroupBy(newProject, aggregations, newGroupingPrivate)
}

func (c *CustomFuncs) CanMergeProjectIntoAggregate(
	input memo.RelExpr,
	groupingPrivate *memo.GroupingPrivate,
) bool {
	project, ok := input.(*memo.ProjectExpr)
	if !ok {
		return false
	}

	if !groupingPrivate.GroupingCols.SubsetOf(project.Passthrough) {
		return false
	}

	if !groupingPrivate.Ordering.ColSet().SubsetOf(project.Passthrough) {
		return false
	}

	if len(project.Projections) == 0 {
		return false
	}

	return true
}

func (c *CustomFuncs) MergeProjectIntoAggregate(
	input memo.RelExpr,
	aggregations memo.AggregationsExpr,
) memo.AggregationsExpr {
	project := input.(*memo.ProjectExpr)

	colToExpr := make(map[opt.ColumnID]opt.ScalarExpr)
	for i := range project.Projections {
		item := &project.Projections[i]
		colToExpr[item.Col] = item.Element
	}

	newAggs := make(memo.AggregationsExpr, len(aggregations))
	for i := range aggregations {
		agg := aggregations[i].Agg

		var replace ReplaceFunc
		replace = func(e opt.Expr) opt.Expr {
			if v, ok := e.(*memo.VariableExpr); ok {
				if expr, found := colToExpr[v.Col]; found {
					return expr
				}
			}
			return c.f.Replace(e, replace)
		}

		newAgg := replace(agg).(opt.ScalarExpr)
		newAggs[i] = c.f.ConstructAggregationsItem(newAgg, aggregations[i].Col)
	}

	return newAggs
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
	passthrough := opt.ColSet{}

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
			if unionCol == outCol {
				passthrough.Add(outCol)
			} else {
				mergedProjections = append(mergedProjections, c.f.ConstructProjectionsItem(
					c.f.ConstructVariable(unionCol),
					outCol,
				))
			}
			unionColIdx++
		}
	}

	return c.f.ConstructProject(union, mergedProjections, passthrough)
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
	if c.HasOuterCols(left) || c.HasOuterCols(right) {
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

	leftCols := left.Relational().OutputCols
	rightCols := right.Relational().OutputCols
	hasBoundCondition := false
	allBoundByOneSide := true
	for i := range on {
		if on[i].Condition.Op() != opt.TrueOp {
			boundByLeft := c.IsBoundBy(&on[i], leftCols)
			boundByRight := c.IsBoundBy(&on[i], rightCols)
			if !boundByLeft && !boundByRight {
				return false
			}
			if boundByLeft && boundByRight {
				allBoundByOneSide = false
			}
			hasBoundCondition = true
		}
	}

	if !hasBoundCondition {
		return false
	}

	if !allBoundByOneSide {
		return false
	}

	return true
}

func (c *CustomFuncs) ConstructJoinExtractFilterResult(
	left memo.RelExpr,
	right memo.RelExpr,
	on memo.FiltersExpr,
	private *memo.JoinPrivate,
) memo.RelExpr {
	var disabledRules intsets.Fast
	disabledRules.Add(int(opt.FilterIntoJoin))
	disabledRules.Add(int(opt.MergeSelectInnerJoin))

	var result memo.RelExpr
	c.f.DisableOptimizationRulesTemporarily(disabledRules, func() {
		result = c.f.ConstructSelect(
			c.f.ConstructInnerJoin(left, right, memo.EmptyFiltersExpr, private),
			on,
		)
	})
	return result
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
					if !opt.IsConstValueOp(scalarArg) {
						return true
					}
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

func (c *CustomFuncs) ConstructAggregateExtractProject(
	input memo.RelExpr,
	aggregations memo.AggregationsExpr,
	groupingPrivate *memo.GroupingPrivate,
) memo.RelExpr {
	inputCols := c.OutputCols(input)

	var pb projectBuilder
	pb.init(c, inputCols)

	newAggs := make(memo.AggregationsExpr, len(aggregations))
	for i := range aggregations {
		agg := aggregations[i].Agg
		var newArg opt.ScalarExpr

		if agg.ChildCount() > 0 {
			arg := agg.Child(0)
			if scalarArg, ok := arg.(opt.ScalarExpr); ok {
				newArg = pb.add(scalarArg)
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

	newProject := pb.buildProject(input)

	var result memo.RelExpr
	var disabledRules intsets.Fast
	disabledRules.Add(int(opt.AggregateProjectMerge))
	c.f.DisableOptimizationRulesTemporarily(disabledRules, func() {
		result = c.f.ConstructGroupBy(newProject, newAggs, groupingPrivate)
	})
	return result
}

func (c *CustomFuncs) IsRedundantSemiJoin(
	left memo.RelExpr,
	right memo.RelExpr,
	filters memo.FiltersExpr,
) bool {
	currentLeft := left
	for {
		if proj, ok := currentLeft.(*memo.ProjectExpr); ok {
			currentLeft = proj.Input
			continue
		}
		if sel, ok := currentLeft.(*memo.SelectExpr); ok {
			currentLeft = sel.Input
			continue
		}
		break
	}

	semi, ok := currentLeft.(*memo.SemiJoinExpr)
	if !ok {
		return false
	}

	peel := func(e memo.RelExpr) memo.RelExpr {
		current := e
		for {
			if proj, ok := current.(*memo.ProjectExpr); ok {
				current = proj.Input
				continue
			}
			if sel, ok := current.(*memo.SelectExpr); ok {
				current = sel.Input
				continue
			}
			return current
		}
	}

	baseRight := peel(right)
	baseSemiRight := peel(semi.Right)

	if baseRight == baseSemiRight {
		return true
	}

	scanRight, okRight := baseRight.(*memo.ScanExpr)
	scanSemiRight, okSemiRight := baseSemiRight.(*memo.ScanExpr)

	if okRight && okSemiRight {
		return scanRight.Table == scanSemiRight.Table
	}

	return false
}

func (c *CustomFuncs) IsVariable(scalar opt.ScalarExpr) bool {
	return scalar.Op() == opt.VariableOp
}

func (c *CustomFuncs) AggArg(agg opt.ScalarExpr) opt.ScalarExpr {
	if agg.ChildCount() > 0 {
		if arg, ok := agg.Child(0).(opt.ScalarExpr); ok {
			return arg
		}
	}
	return nil
}

func (c *CustomFuncs) ReplaceAggArg(agg, newArg opt.ScalarExpr) opt.ScalarExpr {
	var replace ReplaceFunc
	replace = func(e opt.Expr) opt.Expr {
		if e == agg.Child(0) {
			return newArg
		}
		return c.f.Replace(e, replace)
	}
	return replace(agg).(opt.ScalarExpr)
}

func (c *CustomFuncs) IsSemiJoin(input memo.RelExpr) bool {
	switch input.Op() {
	case opt.SemiJoinOp, opt.SemiJoinApplyOp:
		return true
	default:
		return false
	}
}

func (c *CustomFuncs) UnbindFiltersFromProjections(
	projections memo.ProjectionsExpr, filters memo.FiltersExpr,
) memo.FiltersExpr {
	var colMap opt.ColMap
	for i := range projections {
		from := projections[i].Col
		to := projections[i].Element.(*memo.VariableExpr).Col
		colMap.Set(int(from), int(to))
	}
	newFilters := make(memo.FiltersExpr, len(filters))
	for i := range filters {
		newCondition := c.f.RemapCols(filters[i].Condition, colMap)
		newFilters[i] = c.f.ConstructFiltersItem(newCondition)
	}
	return newFilters
}

func (c *CustomFuncs) BindFiltersToProjections(
	projections memo.ProjectionsExpr, passthrough opt.ColSet, filters memo.FiltersExpr,
) memo.FiltersExpr {
	var colMap opt.ColMap
	for col, ok := passthrough.Next(0); ok; col, ok = passthrough.Next(col + 1) {
		colMap.Set(int(col), int(col))
	}
	for i := range projections {
		from := projections[i].Element.(*memo.VariableExpr).Col
		to := projections[i].Col
		colMap.Set(int(from), int(to))
	}
	newFilters := make(memo.FiltersExpr, len(filters))
	for i := range filters {
		newCondition := c.f.RemapCols(filters[i].Condition, colMap)
		newFilters[i] = c.f.ConstructFiltersItem(newCondition)
	}
	return newFilters
}


func deriveGroupByRejectNullCols(
	mem *memo.Memo, in memo.RelExpr, disabledRules intsets.Fast,
) opt.ColSet {
	input := in.Child(0).(memo.RelExpr)
	aggs := *in.Child(1).(*memo.AggregationsExpr)

	var rejectNullCols opt.ColSet
	var savedInColID opt.ColumnID
	for i := range aggs {
		agg := memo.ExtractAggFunc(aggs[i].Agg)
		aggOp := agg.Op()

		if aggOp == opt.ConstAggOp {
			continue
		}

		if !opt.AggregateIgnoresNulls(aggOp) || !opt.AggregateIsNullOnEmpty(aggOp) {
			return opt.ColSet{}
		}

		var inColID opt.ColumnID
		if v, ok := agg.Child(0).(*memo.VariableExpr); ok {
			inColID = v.Col
		} else {
			return opt.ColSet{}
		}

		if savedInColID != 0 && savedInColID != inColID {
			return opt.ColSet{}
		}
		savedInColID = inColID

		if !DeriveRejectNullCols(mem, input, disabledRules).Contains(inColID) {
			return opt.ColSet{}
		}

		rejectNullCols.Add(aggs[i].Col)
	}
	return rejectNullCols
}

func (c *CustomFuncs) AllFiltersCanMapOnSetOp(filters memo.FiltersExpr) bool {
	for i := range filters {
		if !c.CanMapOnSetOp(&filters[i]) {
			return false
		}
	}
	return true
}

func (c *CustomFuncs) MapSetOpFiltersLeft(
	filters memo.FiltersExpr, set *memo.SetPrivate,
) memo.FiltersExpr {
	newFilters := make(memo.FiltersExpr, len(filters))
	for i := range filters {
		newCondition := c.MapSetOpFilterLeft(&filters[i], set)
		newFilters[i] = c.f.ConstructFiltersItem(newCondition)
	}
	return newFilters
}

func (c *CustomFuncs) MapSetOpFiltersRight(
	filters memo.FiltersExpr, set *memo.SetPrivate,
) memo.FiltersExpr {
	newFilters := make(memo.FiltersExpr, len(filters))
	for i := range filters {
		newCondition := c.MapSetOpFilterRight(&filters[i], set)
		newFilters[i] = c.f.ConstructFiltersItem(newCondition)
	}
	return newFilters
}

func (c *CustomFuncs) MakeUnionPrivateForExcept(pInner, pOuter *memo.SetPrivate) *memo.SetPrivate {
    if len(pInner.RightCols) != len(pOuter.RightCols) {
        panic(errors.AssertionFailedf("invalid SetPrivate shapes for Except-minus merge: inner.RightCols and outer.RightCols must have same length"))
    }
    leftCols := make(opt.ColList, len(pInner.RightCols))
    copy(leftCols, pInner.RightCols)

    rightCols := make(opt.ColList, len(pOuter.RightCols))
    copy(rightCols, pOuter.RightCols)

    outCols := make(opt.ColList, len(pInner.RightCols))
    copy(outCols, pInner.RightCols)

    return &memo.SetPrivate{
        LeftCols:  leftCols,
        RightCols: rightCols,
        OutCols:   outCols,
    }
}

func (c *CustomFuncs) ConstructMinusMergeResult(
	leftLeft memo.RelExpr,
	leftRight memo.RelExpr,
	right memo.RelExpr,
	innerPrivate *memo.SetPrivate,
	outerPrivate *memo.SetPrivate,
) memo.RelExpr {
	md := c.mem.Metadata()
	
	colType := md.ColumnMeta(innerPrivate.RightCols[0]).Type
	
	outCols := make(opt.ColList, len(innerPrivate.RightCols))
	for i := range outCols {
		outCols[i] = md.AddColumn("", colType)
	}
	
	unionPrivate := &memo.SetPrivate{
		LeftCols:  innerPrivate.RightCols,
		RightCols: outerPrivate.RightCols,
		OutCols:   outCols,
	}
	
	union := c.f.ConstructUnion(leftRight, right, unionPrivate)

	unionOutputCols := c.OutputCols(union).ToList()

	if len(unionOutputCols) != len(outerPrivate.RightCols) {
		panic(errors.AssertionFailedf("Union output column count mismatch: got %d, expected %d",
			len(unionOutputCols), len(outerPrivate.RightCols)))
	}

	needsProject := false
	for i := range unionOutputCols {
		if unionOutputCols[i] != outerPrivate.RightCols[i] {
			needsProject = true
			break
		}
	}

	var unionForExcept memo.RelExpr
	if needsProject {
		projections := make(memo.ProjectionsExpr, len(unionOutputCols))
		for i := range unionOutputCols {
			projections[i] = c.f.ConstructProjectionsItem(
				c.f.ConstructVariable(unionOutputCols[i]),
				outerPrivate.RightCols[i],
			)
		}
		unionForExcept = c.f.ConstructProject(union, projections, opt.ColSet{})
	} else {
		unionForExcept = union
	}

	exceptPrivate := c.MakeSetPrivate(
		innerPrivate.LeftCols,
		outerPrivate.RightCols,
		outerPrivate.OutCols,
	)

	return c.f.ConstructExcept(leftLeft, unionForExcept, exceptPrivate)
}

func (c *CustomFuncs) MakeSetPrivate(
	leftCols, rightCols, outCols opt.ColList,
) *memo.SetPrivate {
	if len(leftCols) != len(rightCols) || len(leftCols) != len(outCols) {
		panic(errors.AssertionFailedf(
			"invalid SetPrivate: leftCols, rightCols, and outCols must have same length",
		))
	}

	leftColsCopy := make(opt.ColList, len(leftCols))
	copy(leftColsCopy, leftCols)

	rightColsCopy := make(opt.ColList, len(rightCols))
	copy(rightColsCopy, rightCols)

	outColsCopy := make(opt.ColList, len(outCols))
	copy(outColsCopy, outCols)

	return &memo.SetPrivate{
		LeftCols:  leftColsCopy,
		RightCols: rightColsCopy,
		OutCols:   outColsCopy,
	}
}

