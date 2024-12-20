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
			//嵌套 兼容gorm.Model
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
	//return strings.ToLower(field.Name)
}

func (uwp *LambdaQueryWrapper[T]) getColumn(field any) string {
	return getColumn(uwp.field2Column, field)
}

func (uwp *LambdaQueryWrapper[T]) getColumns(fields []any) []string {
	return getColumns(uwp.field2Column, fields)
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

func (this *LambdaQueryWrapper[T]) Eq(cond bool, field any, val any) *LambdaQueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter(this.getColumn(field), sqlkeyword.Eq, val)
	}
	return this
}

func (this *LambdaQueryWrapper[T]) Ne(cond bool, field any, val any) *LambdaQueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter(this.getColumn(field), sqlkeyword.Ne, val)
	}
	return this
}

func (this *LambdaQueryWrapper[T]) Gt(cond bool, field any, val any) *LambdaQueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter(this.getColumn(field), sqlkeyword.Gt, val)
	}
	return this
}

func (this *LambdaQueryWrapper[T]) Ge(cond bool, field any, val any) *LambdaQueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter(this.getColumn(field), sqlkeyword.Ge, val)
	}
	return this
}

func (this *LambdaQueryWrapper[T]) Lt(cond bool, field any, val any) *LambdaQueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter(this.getColumn(field), sqlkeyword.Lt, val)
	}
	return this
}

func (this *LambdaQueryWrapper[T]) Le(cond bool, field any, val any) *LambdaQueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter(this.getColumn(field), sqlkeyword.Le, val)
	}
	return this
}

func (this *LambdaQueryWrapper[T]) IsNull(cond bool, field any) *LambdaQueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter(this.getColumn(field), sqlkeyword.IsNull, nil)
	}
	return this
}

func (this *LambdaQueryWrapper[T]) IsNotNull(cond bool, field any) *LambdaQueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter(this.getColumn(field), sqlkeyword.IsNotNull, nil)
	}
	return this
}

func (this *LambdaQueryWrapper[T]) Between(cond bool, field any, val1 any, val2 any) *LambdaQueryWrapper[T] {
	vals := [2]any{val1, val2}
	if cond {
		this.whereSegment.addFilter(this.getColumn(field), sqlkeyword.Between, vals)
	}
	return this
}

func (this *LambdaQueryWrapper[T]) In(cond bool, field any, vals any) *LambdaQueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter(this.getColumn(field), sqlkeyword.In, vals)
	}
	return this
}

func (this *LambdaQueryWrapper[T]) Like(cond bool, field any, val any) *LambdaQueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter(this.getColumn(field), sqlkeyword.Like, val)
	}
	return this
}

func (this *LambdaQueryWrapper[T]) LikeLeft(cond bool, field any, val any) *LambdaQueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter(this.getColumn(field), sqlkeyword.LikeLeft, val)
	}
	return this
}

func (this *LambdaQueryWrapper[T]) LikeRight(cond bool, field any, val any) *LambdaQueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter(this.getColumn(field), sqlkeyword.LikeRight, val)
	}
	return this
}

func (this *LambdaQueryWrapper[T]) Or(cond bool, fn func(wp *LambdaQueryWrapper[T])) *LambdaQueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter("", sqlkeyword.Or, nil)
		if fn != nil {
			this.whereSegment.addFilter("", sqlkeyword.LeftBracket, nil)
			fn(this)
			this.whereSegment.addFilter("", sqlkeyword.RightBracket, nil)
		}
	}
	return this
}

func (this *LambdaQueryWrapper[T]) And(cond bool, fn func(wp *LambdaQueryWrapper[T])) *LambdaQueryWrapper[T] {
	if cond {
		this.whereSegment.addFilter("", sqlkeyword.And, nil)
		if fn != nil {
			this.whereSegment.addFilter("", sqlkeyword.LeftBracket, nil)
			fn(this)
			this.whereSegment.addFilter("", sqlkeyword.RightBracket, nil)
		}
	}
	return this
}

func (this *LambdaQueryWrapper[T]) OrderByAsc(cond bool, field any) *LambdaQueryWrapper[T] {
	if cond {
		this.selectSegment.addOrder(this.getColumn(field), sqlkeyword.Asc)
	}
	return this
}

func (this *LambdaQueryWrapper[T]) OrderByDesc(cond bool, field any) *LambdaQueryWrapper[T] {
	if cond {
		this.selectSegment.addOrder(this.getColumn(field), sqlkeyword.Desc)
	}
	return this
}

func (this *LambdaQueryWrapper[T]) GroupBy(cond bool, field any) *LambdaQueryWrapper[T] {
	if cond {
		this.selectSegment.addGorup(this.getColumn(field))
	}
	return this
}

func (this *LambdaQueryWrapper[T]) Having(cond bool, sqlHaving string, vals ...any) *LambdaQueryWrapper[T] {
	if cond {
		this.selectSegment.addHaving(sqlHaving, vals)
	}
	return this
}

func (this *LambdaQueryWrapper[T]) Select(fields ...any) *LambdaQueryWrapper[T] {
	this.selectSegment.addSelect(this.getColumns(fields))
	return this
}

func (this *LambdaQueryWrapper[T]) Offset(offset int) *LambdaQueryWrapper[T] {
	this.selectSegment.addOffset(offset)
	return this
}

func (this *LambdaQueryWrapper[T]) Limit(limit int) *LambdaQueryWrapper[T] {
	this.selectSegment.addLimit(limit)
	return this
}

func (this *LambdaQueryWrapper[T]) GetWhere() (string, []any) {
	return this.whereSegment.GetWhere()
}

func (this *LambdaQueryWrapper[T]) GetOrderBy() string {
	return this.selectSegment.GetOrderBy()
}

func (this *LambdaQueryWrapper[T]) GetSelect() string {
	return this.selectSegment.GetSelect()
}

func (this *LambdaQueryWrapper[T]) GetGroupBy() string {
	return this.selectSegment.GetGroupBy()
}

func (this *LambdaQueryWrapper[T]) GetHaving() (string, []any) {
	return this.selectSegment.GetHaving()
}

func (this LambdaQueryWrapper[T]) GetOffset() int {
	return this.selectSegment.GetOffset()
}

func (this LambdaQueryWrapper[T]) GetLimit() int {
	return this.selectSegment.GetLimit()
}

func (this *LambdaUpdateWrapper[T]) Eq(cond bool, field any, val any) *LambdaUpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter(this.getColumn(field), sqlkeyword.Eq, val)
	}
	return this
}

func (this *LambdaUpdateWrapper[T]) Ne(cond bool, field any, val any) *LambdaUpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter(this.getColumn(field), sqlkeyword.Ne, val)
	}
	return this
}

func (this *LambdaUpdateWrapper[T]) Gt(cond bool, field any, val any) *LambdaUpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter(this.getColumn(field), sqlkeyword.Gt, val)
	}
	return this
}

func (this *LambdaUpdateWrapper[T]) Ge(cond bool, field any, val any) *LambdaUpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter(this.getColumn(field), sqlkeyword.Ge, val)
	}
	return this
}

func (this *LambdaUpdateWrapper[T]) Lt(cond bool, field any, val any) *LambdaUpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter(this.getColumn(field), sqlkeyword.Lt, val)
	}
	return this
}

func (this *LambdaUpdateWrapper[T]) Le(cond bool, field any, val any) *LambdaUpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter(this.getColumn(field), sqlkeyword.Le, val)
	}
	return this
}

func (this *LambdaUpdateWrapper[T]) IsNull(cond bool, field any) *LambdaUpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter(this.getColumn(field), sqlkeyword.IsNull, nil)
	}
	return this
}

func (this *LambdaUpdateWrapper[T]) IsNotNull(cond bool, field any) *LambdaUpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter(this.getColumn(field), sqlkeyword.IsNotNull, nil)
	}
	return this
}

func (this *LambdaUpdateWrapper[T]) Between(cond bool, field any, val1 any, val2 any) *LambdaUpdateWrapper[T] {
	vals := [2]any{val1, val2}
	if cond {
		this.whereSegment.addFilter(this.getColumn(field), sqlkeyword.Between, vals)
	}
	return this
}

func (this *LambdaUpdateWrapper[T]) In(cond bool, field any, vals any) *LambdaUpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter(this.getColumn(field), sqlkeyword.In, vals)
	}
	return this
}

func (this *LambdaUpdateWrapper[T]) Like(cond bool, field any, val any) *LambdaUpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter(this.getColumn(field), sqlkeyword.Like, val)
	}
	return this
}

func (this *LambdaUpdateWrapper[T]) LikeLeft(cond bool, field any, val any) *LambdaUpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter(this.getColumn(field), sqlkeyword.LikeLeft, val)
	}
	return this
}

func (this *LambdaUpdateWrapper[T]) LikeRight(cond bool, field any, val any) *LambdaUpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter(this.getColumn(field), sqlkeyword.LikeRight, val)
	}
	return this
}

func (this *LambdaUpdateWrapper[T]) Or(cond bool, fn func(wp *LambdaUpdateWrapper[T])) *LambdaUpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter("", sqlkeyword.Or, nil)
		if fn != nil {
			this.whereSegment.addFilter("", sqlkeyword.LeftBracket, nil)
			fn(this)
			this.whereSegment.addFilter("", sqlkeyword.RightBracket, nil)
		}
	}
	return this
}

func (this *LambdaUpdateWrapper[T]) And(cond bool, fn func(wp *LambdaUpdateWrapper[T])) *LambdaUpdateWrapper[T] {
	if cond {
		this.whereSegment.addFilter("", sqlkeyword.And, nil)
		if fn != nil {
			this.whereSegment.addFilter("", sqlkeyword.LeftBracket, nil)
			fn(this)
			this.whereSegment.addFilter("", sqlkeyword.RightBracket, nil)
		}
	}
	return this
}

func (this *LambdaUpdateWrapper[T]) Set(cond bool, field any, val any) *LambdaUpdateWrapper[T] {
	if cond {
		this.setSegment.addSetter(this.getColumn(field), val)
	}
	return this
}

func (this *LambdaUpdateWrapper[T]) GetWhere() (string, []any) {
	return this.whereSegment.GetWhere()
}

func (this *LambdaUpdateWrapper[T]) GetSet() map[string]any {
	return this.setSegment.GetSet()
}
