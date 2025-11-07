package norm

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/errors"
)

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

func (c *CustomFuncs) ConstructDummyValuesTable(
	constantValues memo.ScalarListExpr, dummyCols opt.ColList,
) memo.RelExpr {
	tuple := c.f.ConstructTuple(constantValues, nil)
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

func (c *CustomFuncs) ColListToSet(colList opt.ColList) opt.ColSet {
	return colList.ToSet()
}
