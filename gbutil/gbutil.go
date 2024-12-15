package gbutil

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

func ValidateList(val any) []any {
	dataType := reflect.TypeOf(val)
	if dataType.Kind() != reflect.Array && dataType.Kind() != reflect.Slice {
		panic("ERROR VALUE TYPE")
	}
	sliceValue := reflect.ValueOf(val)
	slice := make([]any, sliceValue.Len())
	for i := 0; i < sliceValue.Len(); i++ {
		slice[i] = sliceValue.Index(i).Interface()
	}
	return slice
}

func ConvertParamObj2Map(obj any) (map[string]any, error) {
	paramsMap := make(map[string]interface{})

	jsonData, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(jsonData, &paramsMap)
	if err != nil {
		return nil, err
	}

	return paramsMap, nil
}

func ParseTmplPlaceholder(tmpl string, paramsMap map[string]any) (string, []any, error) {
	reg := regexp.MustCompile(`#{[^}]*}`)
	matches := reg.FindAllStringSubmatch(tmpl, -1)
	params := make([]any, len(matches))
	for i, match := range matches {
		fullKey := match[0][2 : len(match[0])-1]
		subKeys := strings.Split(fullKey, ".")
		var param any
		param = paramsMap
		for _, subKey := range subKeys {
			val, exist := param.(map[string]any)[subKey]
			if !exist {
				return "", nil, errors.New(fmt.Sprintf("param: %s not found", fullKey))
			}
			param = val
		}
		params[i] = param
	}
	return reg.ReplaceAllString(tmpl, "?"), params, nil
}
