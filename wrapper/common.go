package wrapper

import (
	"fmt"
	"gitee.com/wcqtech/gbatis/gbutil"
	"gitee.com/wcqtech/gbatis/sqlkeyword"
	"gorm.io/gorm/schema"
	"reflect"
	"strings"
)

type (
	AbstractQueryWrapper interface {
		GetWhere() (string, []any)
		GetOrderBy() string
		GetSelect() string
		GetGroupBy() string
		GetHaving() (string, []any)
		GetOffset() int
		GetLimit() int
	}
	AbstractUpdateWrapper interface {
		GetWhere() (string, []any)
		GetSet() map[string]any
	}
	SqlFilter struct {
		whereSegmentBuilder strings.Builder
		whereLastStr        string
		whereArgs           []any
	}
	SqlSelector struct {
		selectSegmentBuilder strings.Builder
		lastSegmentBuilder   strings.Builder
		orderSegmentBuilder  strings.Builder
		groupSegmentBuilder  strings.Builder
		havingSegmentBuilder strings.Builder
		havingArgs           []any
		offset               int
		limit                int
	}
	SqlUpdater struct {
		column2Val map[string]any
	}
)

var (
	entity2PrimaryKeyCache = make(map[string]string)
)

func GetEntityPrimaryKey[T schema.Tabler](entity *T) string {
	name := reflect.TypeOf(entity).Elem().Name()
	pk, ok := entity2PrimaryKeyCache[name]
	if !ok {
		var pk2 string
		initPrimaryKey(entity, &pk2)
		return pk2
	}
	return pk
}

func initPrimaryKey[T schema.Tabler](entity *T, pk *string) {
	t := reflect.TypeOf(entity).Elem()
	cache, ok := entity2PrimaryKeyCache[t.Name()]
	if ok {
		*pk = cache
	}
	valueOf := reflect.ValueOf(entity).Elem()
	typeOf := reflect.TypeOf(entity).Elem()
	for i := 0; i < t.NumField(); i++ {
		field := typeOf.Field(i)
		if field.Anonymous {
			initNestedPrimaryKey(valueOf, field, pk)
		} else {
			if validatePrimaryKey(field) {
				*pk = parseColumnName(field)
				break
			}
		}
	}
	entity2PrimaryKeyCache[t.Name()] = *pk
}

func initNestedPrimaryKey(valueOf reflect.Value, field reflect.StructField, pk *string) {
	modelType := field.Type
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	for j := 0; j < modelType.NumField(); j++ {
		subField := modelType.Field(j)
		if subField.Anonymous {
			initNestedPrimaryKey(valueOf, subField, pk)
		} else {
			if validatePrimaryKey(modelType.Field(j)) {
				*pk = parseColumnName(modelType.Field(j))
				break
			}
		}
	}
}

func validatePrimaryKey(field reflect.StructField) bool {
	gormTag := schema.ParseTagSetting(field.Tag.Get("gorm"), ";")
	_, ok1 := gormTag["PRIMARYKEY"]
	_, ok2 := gormTag["PRIMARY_KEY"]
	if ok1 || ok2 {
		return true
	}
	return false
}

func (this *SqlFilter) addFilter(column string, key string, val any) {
	if this.whereSegmentBuilder.Len() > 0 {
		this.whereSegmentBuilder.WriteString(" ")
	}
	if this.whereLastStr != sqlkeyword.And &&
		this.whereLastStr != sqlkeyword.Or &&
		this.whereLastStr != "" &&
		this.whereLastStr != sqlkeyword.LeftBracket &&
		key != sqlkeyword.And &&
		key != sqlkeyword.Or &&
		key != sqlkeyword.RightBracket {
		this.whereSegmentBuilder.WriteString("AND ")
	}
	switch key {
	case sqlkeyword.IsNull:
		this.whereSegmentBuilder.WriteString(fmt.Sprintf("%s %s", column, key))
		this.whereLastStr = sqlkeyword.IsNull
	case sqlkeyword.IsNotNull:
		this.whereSegmentBuilder.WriteString(fmt.Sprintf("%s %s", column, key))
		this.whereLastStr = sqlkeyword.IsNotNull
	case sqlkeyword.Like:
		this.whereSegmentBuilder.WriteString(fmt.Sprintf("%s LIKE ?", column))
		this.whereArgs = append(this.whereArgs, fmt.Sprintf("%%%v%%", val))
		this.whereLastStr = sqlkeyword.Question
	case sqlkeyword.LikeLeft:
		this.whereSegmentBuilder.WriteString(fmt.Sprintf("%s LIKE ?", column))
		this.whereArgs = append(this.whereArgs, fmt.Sprintf("%%%v", val))
		this.whereLastStr = sqlkeyword.Question
	case sqlkeyword.LikeRight:
		this.whereSegmentBuilder.WriteString(fmt.Sprintf("%s LIKE ?", column))
		this.whereArgs = append(this.whereArgs, fmt.Sprintf("%v%%", val))
		this.whereLastStr = sqlkeyword.Question
	case sqlkeyword.In:
		list := gbutil.ValidateList(val)
		this.whereSegmentBuilder.WriteString(fmt.Sprintf("%s IN ?", column))
		this.whereArgs = append(this.whereArgs, list)
		this.whereLastStr = sqlkeyword.RightBracket
	case sqlkeyword.NotIn:
		list := gbutil.ValidateList(val)
		this.whereSegmentBuilder.WriteString(fmt.Sprintf("%s NOT IN ?", column))
		this.whereArgs = append(this.whereArgs, list)
		this.whereLastStr = sqlkeyword.RightBracket
	case sqlkeyword.Between:
		if arr, ok := val.([2]any); ok {
			this.whereSegmentBuilder.WriteString(fmt.Sprintf("%s BETWEEN ? AND ?", column))
			this.whereArgs = append(this.whereArgs, arr[0], arr[1])
			this.whereLastStr = sqlkeyword.Question
		} else {
			panic("ERROR VALUE TYPE")
		}
	case sqlkeyword.Or:
		this.whereSegmentBuilder.WriteString(sqlkeyword.Or)
		this.whereLastStr = sqlkeyword.Or
	case sqlkeyword.And:
		this.whereSegmentBuilder.WriteString(sqlkeyword.And)
		this.whereLastStr = sqlkeyword.And
	case sqlkeyword.LeftBracket:
		this.whereSegmentBuilder.WriteString(sqlkeyword.LeftBracket)
		this.whereLastStr = sqlkeyword.LeftBracket
	case sqlkeyword.RightBracket:
		this.whereSegmentBuilder.WriteString(sqlkeyword.RightBracket)
		this.whereLastStr = sqlkeyword.RightBracket
	default:
		this.whereSegmentBuilder.WriteString(fmt.Sprintf("%s %s ?", column, key))
		this.whereArgs = append(this.whereArgs, val)
		this.whereLastStr = sqlkeyword.Question
	}
}

func (this *SqlSelector) addOrder(column string, key string) {
	if this.orderSegmentBuilder.Len() > 0 {
		this.orderSegmentBuilder.WriteString(", ")
	}
	this.orderSegmentBuilder.WriteString(fmt.Sprintf("%s %s", column, key))
}

func (this *SqlSelector) addGorup(column string) {
	if this.groupSegmentBuilder.Len() > 0 {
		this.groupSegmentBuilder.WriteString(", ")
	}
	this.groupSegmentBuilder.WriteString(column)
}

func (this *SqlSelector) addHaving(sqlHaving string, vals []any) {
	this.havingSegmentBuilder.WriteString(sqlHaving)
	for _, val := range vals {
		this.havingArgs = append(this.havingArgs, val)
	}
}

func (this *SqlSelector) addSelect(columns []string) {
	for _, column := range columns {
		if this.selectSegmentBuilder.Len() > 0 {
			this.selectSegmentBuilder.WriteString(", ")
		}
		this.selectSegmentBuilder.WriteString(column)
	}
}

func (this *SqlSelector) addOffset(offset int) {
	this.offset = offset
}

func (this *SqlSelector) addLimit(limit int) {
	this.limit = limit
}

func (this *SqlUpdater) addSetter(column string, val any) {
	if len(this.column2Val) == 0 {
		this.column2Val = make(map[string]any)
	}
	this.column2Val[column] = val
}

func (this SqlFilter) GetWhere() (string, []any) {
	return this.whereSegmentBuilder.String(), this.whereArgs
}

func (this SqlSelector) GetOrderBy() string {
	return this.orderSegmentBuilder.String()
}

func (this SqlSelector) GetGroupBy() string {
	return this.groupSegmentBuilder.String()
}

func (this SqlSelector) GetHaving() (string, []any) {
	return this.havingSegmentBuilder.String(), this.havingArgs
}

func (this SqlSelector) GetSelect() string {
	return this.selectSegmentBuilder.String()
}

func (this SqlSelector) GetOffset() int {
	return this.offset
}

func (this SqlSelector) GetLimit() int {
	return this.limit
}

func (this SqlUpdater) GetSet() map[string]any {
	return this.column2Val
}
