package wrapper

import (
	"gitee.com/wcqtech/gbatis/gbatis"
	"gitee.com/wcqtech/gbatis/sqlkeyword"
	"gorm.io/gorm/schema"
	"reflect"
)

type (
	LambdaQueryWrapper[T schema.Tabler] struct {
		Entity        *T
		field2Column  map[uintptr]string
		primaryKey    string
		whereSegment  SqlFilter
		selectSegment SqlSelector
	}
	LambdaUpdateWrapper[T schema.Tabler] struct {
		Entity       *T
		field2Column map[uintptr]string
		primaryKey   string
		whereSegment SqlFilter
		setSegment   SqlUpdater
	}
)

func NewLambdaQueryWrapper[T schema.Tabler]() *LambdaQueryWrapper[T] {
	wp := new(LambdaQueryWrapper[T])
	wp.Entity = new(T)
	wp.field2Column = make(map[uintptr]string)
	initField2Column(wp.Entity, &wp.field2Column)
	initPrimaryKey(wp.Entity, &wp.primaryKey)
	return wp
}

func NewLambdaUpdateWrapper[T schema.Tabler]() *LambdaUpdateWrapper[T] {
	wp := new(LambdaUpdateWrapper[T])
	wp.Entity = new(T)
	wp.field2Column = make(map[uintptr]string)
	initField2Column(wp.Entity, &wp.field2Column)
	initPrimaryKey(wp.Entity, &wp.primaryKey)
	return wp
}

func initField2Column[T schema.Tabler](entity *T, f2c *map[uintptr]string) {
	valueOf := reflect.ValueOf(entity).Elem()
	typeOf := reflect.TypeOf(entity).Elem()
	for i := 0; i < valueOf.NumField(); i++ {
		field := typeOf.Field(i)
		if field.Anonymous {
			initNestedField2Column(valueOf, field, f2c)
		} else {
			pointer := valueOf.Field(i).Addr().Pointer()
			columnName := parseColumnName(field)
			(*f2c)[pointer] = columnName
		}
	}
}

func initNestedField2Column(valueOf reflect.Value, field reflect.StructField, f2c *map[uintptr]string) {
	modelType := field.Type
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	for j := 0; j < modelType.NumField(); j++ {
		subField := modelType.Field(j)
		if subField.Anonymous {
			initNestedField2Column(valueOf, subField, f2c)
		} else {
			pointer := valueOf.FieldByName(modelType.Field(j).Name).Addr().Pointer()
			name := parseColumnName(modelType.Field(j))
			(*f2c)[pointer] = name
		}
	}
}

func parseColumnName(field reflect.StructField) string {
	gormTag := schema.ParseTagSetting(field.Tag.Get("gorm"), ";")
	name, ok := gormTag["COLUMN"]
	if ok {
		return name
	}
	return gbatis.GetConf().NamingStrategy.ColumnName("", field.Name)
}

func (qwp *LambdaQueryWrapper[T]) getColumn(field any) string {
	return getColumn(qwp.field2Column, field)
}

func (qwp *LambdaQueryWrapper[T]) getColumns(fields []any) []string {
	return getColumns(qwp.field2Column, fields)
}

func (uwp *LambdaUpdateWrapper[T]) getColumn(field any) string {
	return getColumn(uwp.field2Column, field)
}

func (uwp *LambdaUpdateWrapper[T]) getColumns(fields []any) []string {
	return getColumns(uwp.field2Column, fields)
}

func getColumn(mp map[uintptr]string, field any) string {
	pointer := reflect.ValueOf(field).Elem().Addr().Pointer()
	column, ok := mp[pointer]
	if !ok {
		panic("UNKNOW COLUMN")
	}
	return column
}

func getColumns(mp map[uintptr]string, fields []any) []string {
	var columns []string
	for _, field := range fields {
		columns = append(columns, getColumn(mp, field))
	}
	return columns
}

func (qwp *LambdaQueryWrapper[T]) Eq(cond bool, field any, val any) *LambdaQueryWrapper[T] {
	if cond {
		qwp.whereSegment.addFilter(qwp.getColumn(field), sqlkeyword.Eq, val)
	}
	return qwp
}

func (qwp *LambdaQueryWrapper[T]) Ne(cond bool, field any, val any) *LambdaQueryWrapper[T] {
	if cond {
		qwp.whereSegment.addFilter(qwp.getColumn(field), sqlkeyword.Ne, val)
	}
	return qwp
}

func (qwp *LambdaQueryWrapper[T]) Gt(cond bool, field any, val any) *LambdaQueryWrapper[T] {
	if cond {
		qwp.whereSegment.addFilter(qwp.getColumn(field), sqlkeyword.Gt, val)
	}
	return qwp
}

func (qwp *LambdaQueryWrapper[T]) Ge(cond bool, field any, val any) *LambdaQueryWrapper[T] {
	if cond {
		qwp.whereSegment.addFilter(qwp.getColumn(field), sqlkeyword.Ge, val)
	}
	return qwp
}

func (qwp *LambdaQueryWrapper[T]) Lt(cond bool, field any, val any) *LambdaQueryWrapper[T] {
	if cond {
		qwp.whereSegment.addFilter(qwp.getColumn(field), sqlkeyword.Lt, val)
	}
	return qwp
}

func (qwp *LambdaQueryWrapper[T]) Le(cond bool, field any, val any) *LambdaQueryWrapper[T] {
	if cond {
		qwp.whereSegment.addFilter(qwp.getColumn(field), sqlkeyword.Le, val)
	}
	return qwp
}

func (qwp *LambdaQueryWrapper[T]) IsNull(cond bool, field any) *LambdaQueryWrapper[T] {
	if cond {
		qwp.whereSegment.addFilter(qwp.getColumn(field), sqlkeyword.IsNull, nil)
	}
	return qwp
}

func (qwp *LambdaQueryWrapper[T]) IsNotNull(cond bool, field any) *LambdaQueryWrapper[T] {
	if cond {
		qwp.whereSegment.addFilter(qwp.getColumn(field), sqlkeyword.IsNotNull, nil)
	}
	return qwp
}

func (qwp *LambdaQueryWrapper[T]) Between(cond bool, field any, val1 any, val2 any) *LambdaQueryWrapper[T] {
	vals := [2]any{val1, val2}
	if cond {
		qwp.whereSegment.addFilter(qwp.getColumn(field), sqlkeyword.Between, vals)
	}
	return qwp
}

func (qwp *LambdaQueryWrapper[T]) In(cond bool, field any, vals any) *LambdaQueryWrapper[T] {
	if cond {
		qwp.whereSegment.addFilter(qwp.getColumn(field), sqlkeyword.In, vals)
	}
	return qwp
}

func (qwp *LambdaQueryWrapper[T]) Like(cond bool, field any, val any) *LambdaQueryWrapper[T] {
	if cond {
		qwp.whereSegment.addFilter(qwp.getColumn(field), sqlkeyword.Like, val)
	}
	return qwp
}

func (qwp *LambdaQueryWrapper[T]) LikeLeft(cond bool, field any, val any) *LambdaQueryWrapper[T] {
	if cond {
		qwp.whereSegment.addFilter(qwp.getColumn(field), sqlkeyword.LikeLeft, val)
	}
	return qwp
}

func (qwp *LambdaQueryWrapper[T]) LikeRight(cond bool, field any, val any) *LambdaQueryWrapper[T] {
	if cond {
		qwp.whereSegment.addFilter(qwp.getColumn(field), sqlkeyword.LikeRight, val)
	}
	return qwp
}

func (qwp *LambdaQueryWrapper[T]) Or(cond bool, fn func(wp *LambdaQueryWrapper[T])) *LambdaQueryWrapper[T] {
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

func (qwp *LambdaQueryWrapper[T]) And(cond bool, fn func(wp *LambdaQueryWrapper[T])) *LambdaQueryWrapper[T] {
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

func (qwp *LambdaQueryWrapper[T]) OrderByAsc(cond bool, field any) *LambdaQueryWrapper[T] {
	if cond {
		qwp.selectSegment.addOrder(qwp.getColumn(field), sqlkeyword.Asc)
	}
	return qwp
}

func (qwp *LambdaQueryWrapper[T]) OrderByDesc(cond bool, field any) *LambdaQueryWrapper[T] {
	if cond {
		qwp.selectSegment.addOrder(qwp.getColumn(field), sqlkeyword.Desc)
	}
	return qwp
}

func (qwp *LambdaQueryWrapper[T]) GroupBy(cond bool, field any) *LambdaQueryWrapper[T] {
	if cond {
		qwp.selectSegment.addGorup(qwp.getColumn(field))
	}
	return qwp
}

func (qwp *LambdaQueryWrapper[T]) Having(cond bool, sqlHaving string, vals ...any) *LambdaQueryWrapper[T] {
	if cond {
		qwp.selectSegment.addHaving(sqlHaving, vals)
	}
	return qwp
}

func (qwp *LambdaQueryWrapper[T]) Select(fields ...any) *LambdaQueryWrapper[T] {
	qwp.selectSegment.addSelect(qwp.getColumns(fields))
	return qwp
}

func (qwp *LambdaQueryWrapper[T]) Offset(offset int) *LambdaQueryWrapper[T] {
	qwp.selectSegment.addOffset(offset)
	return qwp
}

func (qwp *LambdaQueryWrapper[T]) Limit(limit int) *LambdaQueryWrapper[T] {
	qwp.selectSegment.addLimit(limit)
	return qwp
}

func (qwp *LambdaQueryWrapper[T]) GetWhere() (string, []any) {
	return qwp.whereSegment.GetWhere()
}

func (qwp *LambdaQueryWrapper[T]) GetOrderBy() string {
	return qwp.selectSegment.GetOrderBy()
}

func (qwp *LambdaQueryWrapper[T]) GetSelect() string {
	return qwp.selectSegment.GetSelect()
}

func (qwp *LambdaQueryWrapper[T]) GetGroupBy() string {
	return qwp.selectSegment.GetGroupBy()
}

func (qwp *LambdaQueryWrapper[T]) GetHaving() (string, []any) {
	return qwp.selectSegment.GetHaving()
}

func (qwp LambdaQueryWrapper[T]) GetOffset() int {
	return qwp.selectSegment.GetOffset()
}

func (qwp LambdaQueryWrapper[T]) GetLimit() int {
	return qwp.selectSegment.GetLimit()
}

func (uwp *LambdaUpdateWrapper[T]) Eq(cond bool, field any, val any) *LambdaUpdateWrapper[T] {
	if cond {
		uwp.whereSegment.addFilter(uwp.getColumn(field), sqlkeyword.Eq, val)
	}
	return uwp
}

func (uwp *LambdaUpdateWrapper[T]) Ne(cond bool, field any, val any) *LambdaUpdateWrapper[T] {
	if cond {
		uwp.whereSegment.addFilter(uwp.getColumn(field), sqlkeyword.Ne, val)
	}
	return uwp
}

func (uwp *LambdaUpdateWrapper[T]) Gt(cond bool, field any, val any) *LambdaUpdateWrapper[T] {
	if cond {
		uwp.whereSegment.addFilter(uwp.getColumn(field), sqlkeyword.Gt, val)
	}
	return uwp
}

func (uwp *LambdaUpdateWrapper[T]) Ge(cond bool, field any, val any) *LambdaUpdateWrapper[T] {
	if cond {
		uwp.whereSegment.addFilter(uwp.getColumn(field), sqlkeyword.Ge, val)
	}
	return uwp
}

func (uwp *LambdaUpdateWrapper[T]) Lt(cond bool, field any, val any) *LambdaUpdateWrapper[T] {
	if cond {
		uwp.whereSegment.addFilter(uwp.getColumn(field), sqlkeyword.Lt, val)
	}
	return uwp
}

func (uwp *LambdaUpdateWrapper[T]) Le(cond bool, field any, val any) *LambdaUpdateWrapper[T] {
	if cond {
		uwp.whereSegment.addFilter(uwp.getColumn(field), sqlkeyword.Le, val)
	}
	return uwp
}

func (uwp *LambdaUpdateWrapper[T]) IsNull(cond bool, field any) *LambdaUpdateWrapper[T] {
	if cond {
		uwp.whereSegment.addFilter(uwp.getColumn(field), sqlkeyword.IsNull, nil)
	}
	return uwp
}

func (uwp *LambdaUpdateWrapper[T]) IsNotNull(cond bool, field any) *LambdaUpdateWrapper[T] {
	if cond {
		uwp.whereSegment.addFilter(uwp.getColumn(field), sqlkeyword.IsNotNull, nil)
	}
	return uwp
}

func (uwp *LambdaUpdateWrapper[T]) Between(cond bool, field any, val1 any, val2 any) *LambdaUpdateWrapper[T] {
	vals := [2]any{val1, val2}
	if cond {
		uwp.whereSegment.addFilter(uwp.getColumn(field), sqlkeyword.Between, vals)
	}
	return uwp
}

func (uwp *LambdaUpdateWrapper[T]) In(cond bool, field any, vals any) *LambdaUpdateWrapper[T] {
	if cond {
		uwp.whereSegment.addFilter(uwp.getColumn(field), sqlkeyword.In, vals)
	}
	return uwp
}

func (uwp *LambdaUpdateWrapper[T]) Like(cond bool, field any, val any) *LambdaUpdateWrapper[T] {
	if cond {
		uwp.whereSegment.addFilter(uwp.getColumn(field), sqlkeyword.Like, val)
	}
	return uwp
}

func (uwp *LambdaUpdateWrapper[T]) LikeLeft(cond bool, field any, val any) *LambdaUpdateWrapper[T] {
	if cond {
		uwp.whereSegment.addFilter(uwp.getColumn(field), sqlkeyword.LikeLeft, val)
	}
	return uwp
}

func (uwp *LambdaUpdateWrapper[T]) LikeRight(cond bool, field any, val any) *LambdaUpdateWrapper[T] {
	if cond {
		uwp.whereSegment.addFilter(uwp.getColumn(field), sqlkeyword.LikeRight, val)
	}
	return uwp
}

func (uwp *LambdaUpdateWrapper[T]) Or(cond bool, fn func(wp *LambdaUpdateWrapper[T])) *LambdaUpdateWrapper[T] {
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

func (uwp *LambdaUpdateWrapper[T]) And(cond bool, fn func(wp *LambdaUpdateWrapper[T])) *LambdaUpdateWrapper[T] {
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

func (uwp *LambdaUpdateWrapper[T]) Set(cond bool, field any, val any) *LambdaUpdateWrapper[T] {
	if cond {
		uwp.setSegment.addSetter(uwp.getColumn(field), val)
	}
	return uwp
}

func (uwp *LambdaUpdateWrapper[T]) GetWhere() (string, []any) {
	return uwp.whereSegment.GetWhere()
}

func (uwp *LambdaUpdateWrapper[T]) GetSet() map[string]any {
	return uwp.setSegment.GetSet()
}
