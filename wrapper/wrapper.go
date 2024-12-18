package wrapper

import (
	"gitee.com/wcqtech/gbatis/sqlconst"
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

func (this *QueryWrapper[T]) Eq(cond bool, column string, val any) *QueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter(column, sqlconst.Eq, val)
	}
	return this
}

func (this *QueryWrapper[T]) Ne(cond bool, column string, val any) *QueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter(column, sqlconst.Ne, val)
	}
	return this
}

func (this *QueryWrapper[T]) Gt(cond bool, column string, val any) *QueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter(column, sqlconst.Gt, val)
	}
	return this
}

func (this *QueryWrapper[T]) Ge(cond bool, column string, val any) *QueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter(column, sqlconst.Ge, val)
	}
	return this
}

func (this *QueryWrapper[T]) Lt(cond bool, column string, val any) *QueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter(column, sqlconst.Lt, val)
	}
	return this
}

func (this *QueryWrapper[T]) Le(cond bool, column string, val any) *QueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter(column, sqlconst.Le, val)
	}
	return this
}

func (this *QueryWrapper[T]) IsNull(cond bool, column string) *QueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter(column, sqlconst.IsNull, nil)
	}
	return this
}

func (this *QueryWrapper[T]) IsNotNull(cond bool, column string) *QueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter(column, sqlconst.IsNotNull, nil)
	}
	return this
}

func (this *QueryWrapper[T]) Between(cond bool, column string, val1 any, val2 any) *QueryWrapper[T] {
	vals := [2]any{val1, val2}
	if cond {
		this.whereSegment.addFilter(column, sqlconst.Between, vals)
	}
	return this
}

func (this *QueryWrapper[T]) In(cond bool, column string, vals any) *QueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter(column, sqlconst.In, vals)
	}
	return this
}

func (this *QueryWrapper[T]) Like(cond bool, column string, val any) *QueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter(column, sqlconst.Like, val)
	}
	return this
}

func (this *QueryWrapper[T]) LikeLeft(cond bool, column string, val any) *QueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter(column, sqlconst.LikeLeft, val)
	}
	return this
}

func (this *QueryWrapper[T]) LikeRight(cond bool, column string, val any) *QueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter(column, sqlconst.LikeRight, val)
	}
	return this
}

func (this *QueryWrapper[T]) Or(cond bool, fn func(wp *QueryWrapper[T])) *QueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter("", sqlconst.Or, nil)
		if fn != nil {
			this.whereSegment.addFilter("", sqlconst.LeftBracket, nil)
			fn(this)
			this.whereSegment.addFilter("", sqlconst.RightBracket, nil)
		}
	}
	return this
}

func (this *QueryWrapper[T]) And(cond bool, fn func(wp *QueryWrapper[T])) *QueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter("", sqlconst.And, nil)
		if fn != nil {
			this.whereSegment.addFilter("", sqlconst.LeftBracket, nil)
			fn(this)
			this.whereSegment.addFilter("", sqlconst.RightBracket, nil)
		}
	}
	return this
}

func (this *QueryWrapper[T]) OrderByAsc(cond bool, column string) *QueryWrapper[T] {
	if cond {
		this.selectSegment.addOrder(column, sqlconst.Asc)
	}
	return this
}

func (this *QueryWrapper[T]) OrderByDesc(cond bool, column string) *QueryWrapper[T] {
	if cond {
		this.selectSegment.addOrder(column, sqlconst.Desc)
	}
	return this
}

func (this *QueryWrapper[T]) GroupBy(cond bool, column string) *QueryWrapper[T] {
	if cond {
		this.selectSegment.addGorup(column)
	}
	return this
}

func (this *QueryWrapper[T]) Having(cond bool, sqlHaving string, vals ...any) *QueryWrapper[T] {
	if cond {
		this.selectSegment.addHaving(sqlHaving, vals)
	}
	return this
}

func (this *QueryWrapper[T]) Select(columns ...string) *QueryWrapper[T] {
	this.selectSegment.addSelect(columns)
	return this
}

func (this *QueryWrapper[T]) Offset(offset int) *QueryWrapper[T] {
	this.selectSegment.addOffset(offset)
	return this
}

func (this *QueryWrapper[T]) Limit(limit int) *QueryWrapper[T] {
	this.selectSegment.addLimit(limit)
	return this
}

func (this *QueryWrapper[T]) GetWhere() (string, []any) {
	return this.whereSegment.GetWhere()
}

func (this *QueryWrapper[T]) GetOrderBy() string {
	return this.selectSegment.GetOrderBy()
}

func (this *QueryWrapper[T]) GetSelect() string {
	return this.selectSegment.GetSelect()
}

func (this *QueryWrapper[T]) GetGroupBy() string {
	return this.selectSegment.GetGroupBy()
}

func (this *QueryWrapper[T]) GetHaving() (string, []any) {
	return this.selectSegment.GetHaving()
}

func (this *QueryWrapper[T]) GetOffset() int {
	return this.selectSegment.GetOffset()
}

func (this *QueryWrapper[T]) GetLimit() int {
	return this.selectSegment.GetLimit()
}

func (this *UpdateWrapper[T]) Eq(cond bool, column string, val any) *UpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter(column, sqlconst.Eq, val)
	}
	return this
}

func (this *UpdateWrapper[T]) Ne(cond bool, column string, val any) *UpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter(column, sqlconst.Ne, val)
	}
	return this
}

func (this *UpdateWrapper[T]) Gt(cond bool, column string, val any) *UpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter(column, sqlconst.Gt, val)
	}
	return this
}

func (this *UpdateWrapper[T]) Ge(cond bool, column string, val any) *UpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter(column, sqlconst.Ge, val)
	}
	return this
}

func (this *UpdateWrapper[T]) Lt(cond bool, column string, val any) *UpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter(column, sqlconst.Lt, val)
	}
	return this
}

func (this *UpdateWrapper[T]) Le(cond bool, column string, val any) *UpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter(column, sqlconst.Le, val)
	}
	return this
}

func (this *UpdateWrapper[T]) IsNull(cond bool, column string) *UpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter(column, sqlconst.IsNull, nil)
	}
	return this
}

func (this *UpdateWrapper[T]) IsNotNull(cond bool, column string) *UpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter(column, sqlconst.IsNotNull, nil)
	}
	return this
}

func (this *UpdateWrapper[T]) Between(cond bool, column string, val1 any, val2 any) *UpdateWrapper[T] {
	vals := [2]any{val1, val2}
	if cond {
		this.whereSegment.addFilter(column, sqlconst.Between, vals)
	}
	return this
}

func (this *UpdateWrapper[T]) In(cond bool, column string, vals any) *UpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter(column, sqlconst.In, vals)
	}
	return this
}

func (this *UpdateWrapper[T]) Like(cond bool, column string, val any) *UpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter(column, sqlconst.Like, val)
	}
	return this
}

func (this *UpdateWrapper[T]) LikeLeft(cond bool, column string, val any) *UpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter(column, sqlconst.LikeLeft, val)
	}
	return this
}

func (this *UpdateWrapper[T]) LikeRight(cond bool, column string, val any) *UpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter(column, sqlconst.LikeRight, val)
	}
	return this
}

func (this *UpdateWrapper[T]) Or(cond bool, fn func(wp *UpdateWrapper[T])) *UpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter("", sqlconst.Or, nil)
		if fn != nil {
			this.whereSegment.addFilter("", sqlconst.LeftBracket, nil)
			fn(this)
			this.whereSegment.addFilter("", sqlconst.RightBracket, nil)
		}
	}
	return this
}

func (this *UpdateWrapper[T]) And(cond bool, fn func(wp *UpdateWrapper[T])) *UpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter("", sqlconst.And, nil)
		if fn != nil {
			this.whereSegment.addFilter("", sqlconst.LeftBracket, nil)
			fn(this)
			this.whereSegment.addFilter("", sqlconst.RightBracket, nil)
		}
	}
	return this
}

func (this *UpdateWrapper[T]) Set(cond bool, column string, val any) *UpdateWrapper[T] {
	if cond {
		this.setSegment.addSetter(column, val)
	}
	return this
}

func (this *UpdateWrapper[T]) GetWhere() (string, []any) {
	return this.whereSegment.GetWhere()
}

func (this *UpdateWrapper[T]) GetSet() map[string]any {
	return this.setSegment.GetSet()
}
