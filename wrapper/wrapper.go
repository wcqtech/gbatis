package wrapper

import (
	"gitee.com/wcqtech/gbatis/sqlkeyword"
	"gorm.io/gorm/schema"
)

type (
	QueryWrapper[T schema.Tabler] struct {
		primaryKey    string
		whereSegment  SqlFilter
		selectSegment SqlSelector
	}
	UpdateWrapper[T schema.Tabler] struct {
		primaryKey   string
		whereSegment SqlFilter
		setSegment   SqlUpdater
	}
)

func NewQueryWrapper[T schema.Tabler]() *QueryWrapper[T] {
	wp := new(QueryWrapper[T])
	initPrimaryKey(new(T), &wp.primaryKey)
	return wp
}

func NewUpdateWrapper[T schema.Tabler]() *UpdateWrapper[T] {
	wp := new(UpdateWrapper[T])
	initPrimaryKey(new(T), &wp.primaryKey)
	return wp
}

func (qwp *QueryWrapper[T]) Eq(cond bool, column string, val any) *QueryWrapper[T] {
	if cond {
		qwp.whereSegment.addFilter(column, sqlkeyword.Eq, val)
	}
	return qwp
}

func (qwp *QueryWrapper[T]) Ne(cond bool, column string, val any) *QueryWrapper[T] {
	if cond {
		qwp.whereSegment.addFilter(column, sqlkeyword.Ne, val)
	}
	return qwp
}

func (qwp *QueryWrapper[T]) Gt(cond bool, column string, val any) *QueryWrapper[T] {
	if cond {
		qwp.whereSegment.addFilter(column, sqlkeyword.Gt, val)
	}
	return qwp
}

func (qwp *QueryWrapper[T]) Ge(cond bool, column string, val any) *QueryWrapper[T] {
	if cond {
		qwp.whereSegment.addFilter(column, sqlkeyword.Ge, val)
	}
	return qwp
}

func (qwp *QueryWrapper[T]) Lt(cond bool, column string, val any) *QueryWrapper[T] {
	if cond {
		qwp.whereSegment.addFilter(column, sqlkeyword.Lt, val)
	}
	return qwp
}

func (qwp *QueryWrapper[T]) Le(cond bool, column string, val any) *QueryWrapper[T] {
	if cond {
		qwp.whereSegment.addFilter(column, sqlkeyword.Le, val)
	}
	return qwp
}

func (qwp *QueryWrapper[T]) IsNull(cond bool, column string) *QueryWrapper[T] {
	if cond {
		qwp.whereSegment.addFilter(column, sqlkeyword.IsNull, nil)
	}
	return qwp
}

func (qwp *QueryWrapper[T]) IsNotNull(cond bool, column string) *QueryWrapper[T] {
	if cond {
		qwp.whereSegment.addFilter(column, sqlkeyword.IsNotNull, nil)
	}
	return qwp
}

func (qwp *QueryWrapper[T]) Between(cond bool, column string, val1 any, val2 any) *QueryWrapper[T] {
	vals := [2]any{val1, val2}
	if cond {
		qwp.whereSegment.addFilter(column, sqlkeyword.Between, vals)
	}
	return qwp
}

func (qwp *QueryWrapper[T]) In(cond bool, column string, vals any) *QueryWrapper[T] {
	if cond {
		qwp.whereSegment.addFilter(column, sqlkeyword.In, vals)
	}
	return qwp
}

func (qwp *QueryWrapper[T]) Like(cond bool, column string, val any) *QueryWrapper[T] {
	if cond {
		qwp.whereSegment.addFilter(column, sqlkeyword.Like, val)
	}
	return qwp
}

func (qwp *QueryWrapper[T]) LikeLeft(cond bool, column string, val any) *QueryWrapper[T] {
	if cond {
		qwp.whereSegment.addFilter(column, sqlkeyword.LikeLeft, val)
	}
	return qwp
}

func (qwp *QueryWrapper[T]) LikeRight(cond bool, column string, val any) *QueryWrapper[T] {
	if cond {
		qwp.whereSegment.addFilter(column, sqlkeyword.LikeRight, val)
	}
	return qwp
}

func (qwp *QueryWrapper[T]) Or(cond bool, fn func(wp *QueryWrapper[T])) *QueryWrapper[T] {
	if cond {
		qwp.whereSegment.addFilter("", sqlkeyword.Or, nil)
		if fn != nil {
			qwp.whereSegment.addFilter("", sqlkeyword.LeftBracket, nil)
			fn(qwp)
			qwp.whereSegment.addFilter("", sqlkeyword.RightBracket, nil)
		}
	}
	return qwp
}

func (qwp *QueryWrapper[T]) And(cond bool, fn func(wp *QueryWrapper[T])) *QueryWrapper[T] {
	if cond {
		qwp.whereSegment.addFilter("", sqlkeyword.And, nil)
		if fn != nil {
			qwp.whereSegment.addFilter("", sqlkeyword.LeftBracket, nil)
			fn(qwp)
			qwp.whereSegment.addFilter("", sqlkeyword.RightBracket, nil)
		}
	}
	return qwp
}

func (qwp *QueryWrapper[T]) OrderByAsc(cond bool, column string) *QueryWrapper[T] {
	if cond {
		qwp.selectSegment.addOrder(column, sqlkeyword.Asc)
	}
	return qwp
}

func (qwp *QueryWrapper[T]) OrderByDesc(cond bool, column string) *QueryWrapper[T] {
	if cond {
		qwp.selectSegment.addOrder(column, sqlkeyword.Desc)
	}
	return qwp
}

func (qwp *QueryWrapper[T]) GroupBy(cond bool, column string) *QueryWrapper[T] {
	if cond {
		qwp.selectSegment.addGorup(column)
	}
	return qwp
}

func (qwp *QueryWrapper[T]) Having(cond bool, sqlHaving string, vals ...any) *QueryWrapper[T] {
	if cond {
		qwp.selectSegment.addHaving(sqlHaving, vals)
	}
	return qwp
}

func (qwp *QueryWrapper[T]) Select(columns ...string) *QueryWrapper[T] {
	qwp.selectSegment.addSelect(columns)
	return qwp
}

func (qwp *QueryWrapper[T]) Offset(offset int) *QueryWrapper[T] {
	qwp.selectSegment.addOffset(offset)
	return qwp
}

func (qwp *QueryWrapper[T]) Limit(limit int) *QueryWrapper[T] {
	qwp.selectSegment.addLimit(limit)
	return qwp
}

func (qwp *QueryWrapper[T]) GetWhere() (string, []any) {
	return qwp.whereSegment.GetWhere()
}

func (qwp *QueryWrapper[T]) GetOrderBy() string {
	return qwp.selectSegment.GetOrderBy()
}

func (qwp *QueryWrapper[T]) GetSelect() string {
	return qwp.selectSegment.GetSelect()
}

func (qwp *QueryWrapper[T]) GetGroupBy() string {
	return qwp.selectSegment.GetGroupBy()
}

func (qwp *QueryWrapper[T]) GetHaving() (string, []any) {
	return qwp.selectSegment.GetHaving()
}

func (qwp *QueryWrapper[T]) GetOffset() int {
	return qwp.selectSegment.GetOffset()
}

func (qwp *QueryWrapper[T]) GetLimit() int {
	return qwp.selectSegment.GetLimit()
}

func (uwp *UpdateWrapper[T]) Eq(cond bool, column string, val any) *UpdateWrapper[T] {
	if cond {
		uwp.whereSegment.addFilter(column, sqlkeyword.Eq, val)
	}
	return uwp
}

func (uwp *UpdateWrapper[T]) Ne(cond bool, column string, val any) *UpdateWrapper[T] {
	if cond {
		uwp.whereSegment.addFilter(column, sqlkeyword.Ne, val)
	}
	return uwp
}

func (uwp *UpdateWrapper[T]) Gt(cond bool, column string, val any) *UpdateWrapper[T] {
	if cond {
		uwp.whereSegment.addFilter(column, sqlkeyword.Gt, val)
	}
	return uwp
}

func (uwp *UpdateWrapper[T]) Ge(cond bool, column string, val any) *UpdateWrapper[T] {
	if cond {
		uwp.whereSegment.addFilter(column, sqlkeyword.Ge, val)
	}
	return uwp
}

func (uwp *UpdateWrapper[T]) Lt(cond bool, column string, val any) *UpdateWrapper[T] {
	if cond {
		uwp.whereSegment.addFilter(column, sqlkeyword.Lt, val)
	}
	return uwp
}

func (uwp *UpdateWrapper[T]) Le(cond bool, column string, val any) *UpdateWrapper[T] {
	if cond {
		uwp.whereSegment.addFilter(column, sqlkeyword.Le, val)
	}
	return uwp
}

func (uwp *UpdateWrapper[T]) IsNull(cond bool, column string) *UpdateWrapper[T] {
	if cond {
		uwp.whereSegment.addFilter(column, sqlkeyword.IsNull, nil)
	}
	return uwp
}

func (uwp *UpdateWrapper[T]) IsNotNull(cond bool, column string) *UpdateWrapper[T] {
	if cond {
		uwp.whereSegment.addFilter(column, sqlkeyword.IsNotNull, nil)
	}
	return uwp
}

func (uwp *UpdateWrapper[T]) Between(cond bool, column string, val1 any, val2 any) *UpdateWrapper[T] {
	vals := [2]any{val1, val2}
	if cond {
		uwp.whereSegment.addFilter(column, sqlkeyword.Between, vals)
	}
	return uwp
}

func (uwp *UpdateWrapper[T]) In(cond bool, column string, vals any) *UpdateWrapper[T] {
	if cond {
		uwp.whereSegment.addFilter(column, sqlkeyword.In, vals)
	}
	return uwp
}

func (uwp *UpdateWrapper[T]) Like(cond bool, column string, val any) *UpdateWrapper[T] {
	if cond {
		uwp.whereSegment.addFilter(column, sqlkeyword.Like, val)
	}
	return uwp
}

func (uwp *UpdateWrapper[T]) LikeLeft(cond bool, column string, val any) *UpdateWrapper[T] {
	if cond {
		uwp.whereSegment.addFilter(column, sqlkeyword.LikeLeft, val)
	}
	return uwp
}

func (uwp *UpdateWrapper[T]) LikeRight(cond bool, column string, val any) *UpdateWrapper[T] {
	if cond {
		uwp.whereSegment.addFilter(column, sqlkeyword.LikeRight, val)
	}
	return uwp
}

func (uwp *UpdateWrapper[T]) Or(cond bool, fn func(wp *UpdateWrapper[T])) *UpdateWrapper[T] {
	if cond {
		uwp.whereSegment.addFilter("", sqlkeyword.Or, nil)
		if fn != nil {
			uwp.whereSegment.addFilter("", sqlkeyword.LeftBracket, nil)
			fn(uwp)
			uwp.whereSegment.addFilter("", sqlkeyword.RightBracket, nil)
		}
	}
	return uwp
}

func (uwp *UpdateWrapper[T]) And(cond bool, fn func(wp *UpdateWrapper[T])) *UpdateWrapper[T] {
	if cond {
		uwp.whereSegment.addFilter("", sqlkeyword.And, nil)
		if fn != nil {
			uwp.whereSegment.addFilter("", sqlkeyword.LeftBracket, nil)
			fn(uwp)
			uwp.whereSegment.addFilter("", sqlkeyword.RightBracket, nil)
		}
	}
	return uwp
}

func (uwp *UpdateWrapper[T]) Set(cond bool, column string, val any) *UpdateWrapper[T] {
	if cond {
		uwp.setSegment.addSetter(column, val)
	}
	return uwp
}

func (uwp *UpdateWrapper[T]) GetWhere() (string, []any) {
	return uwp.whereSegment.GetWhere()
}

func (uwp *UpdateWrapper[T]) GetSet() map[string]any {
	return uwp.setSegment.GetSet()
}
