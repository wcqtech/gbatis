package ext_mapper

import (
	"encoding/xml"
	"errors"
	"fmt"
	"gitee.com/wcqtech/gbatis/gbutil"
	"gorm.io/gorm"
)

type (
	ExtMapper struct {
		tx     *gorm.DB
		mapper *XmlMapper
	}
	XmlMapper struct {
		XmlName    xml.Name `xml:"mapper"`
		Namespace  string   `xml:"namespace,attr"`
		SelectSQLs []XmlSQL `xml:"select"`
		InsertSQLs []XmlSQL `xml:"insert"`
		UpdateSQLs []XmlSQL `xml:"update"`
		DeleteSQLs []XmlSQL `xml:"delete"`
		Id2Sql     map[string]*XmlSQL
	}
	XmlSQL struct {
		XmlName xml.Name `xml:"sql"`
		ID      string   `xml:"id,attr"`
		Content string   `xml:",chardata"`
	}
)

func NewExtMapper(tx *gorm.DB, xmlMapperData []byte) (*ExtMapper, error) {
	em := new(ExtMapper)
	mapper, err := GetXmlMapper(xmlMapperData)
	if err != nil {
		return nil, err
	}
	em.mapper = mapper
	em.tx = tx
	return em, nil
}

func GetXmlMapper(xmlData []byte) (*XmlMapper, error) {
	var xmlMapper XmlMapper
	if err := xml.Unmarshal(xmlData, &xmlMapper); err != nil {
		return nil, err
	}

	//建立id~sql映射
	xmlMapper.Id2Sql = make(map[string]*XmlSQL)
	var sqls []XmlSQL
	sqls = append(sqls, xmlMapper.SelectSQLs...)
	sqls = append(sqls, xmlMapper.InsertSQLs...)
	sqls = append(sqls, xmlMapper.UpdateSQLs...)
	sqls = append(sqls, xmlMapper.DeleteSQLs...)

	for _, sql := range sqls {
		exist := xmlMapper.Id2Sql[sql.ID]
		if exist != nil {
			return nil, errors.New(fmt.Sprintf("Duplicate sql id '%s' in extension mapper '%s'", sql.ID, xmlMapper.Namespace))
		}
		xmlMapper.Id2Sql[sql.ID] = &sql
	}

	return &xmlMapper, nil
}

func (this *ExtMapper) Select(sqlId string, paramObj any, rx any) error {
	preparedStmt, params, err := this.GetAndParseSql(sqlId, paramObj)
	if err != nil {
		return err
	}
	return this.tx.Exec(preparedStmt, params...).Scan(rx).Error
}

func (this *ExtMapper) Insert(sqlId string, paramObj any) error {
	preparedStmt, params, err := this.GetAndParseSql(sqlId, paramObj)
	if err != nil {
		return err
	}
	return this.tx.Exec(preparedStmt, params...).Error
}

func (this *ExtMapper) Update(sqlId string, paramObj any) error {
	preparedStmt, params, err := this.GetAndParseSql(sqlId, paramObj)
	if err != nil {
		return err
	}
	return this.tx.Exec(preparedStmt, params...).Error
}

func (this *ExtMapper) Delete(sqlId string, paramObj any) error {
	preparedStmt, params, err := this.GetAndParseSql(sqlId, paramObj)
	if err != nil {
		return err
	}
	return this.tx.Raw(preparedStmt, params...).Error
}

func (this *ExtMapper) GetAndParseSql(sqlId string, paramObj any) (string, []any, error) {
	sql, err := this.GetSql(sqlId)
	if err != nil {
		return "", nil, err
	}
	preparedStmt, params, err := parseSql(sql, paramObj)
	if err != nil {
		return "", nil, err
	}
	return preparedStmt, params, nil
}

func parseSql(sql *XmlSQL, paramObj any) (string, []any, error) {
	dynamicTmpl, err := pongo2.FromString(sql.Content)
	if err != nil {
		return "", nil, err
	}

	paramsMap, err := gbutil.ConvertParamObj2Map(paramObj)
	if err != nil {
		return "", nil, err
	}

	tmpl, err := dynamicTmpl.Execute(paramsMap)
	if err != nil {
		return "", nil, err
	}

	preparedStmt, queryParams, err := gbutil.ParseTmplPlaceholder(tmpl, paramsMap)
	if err != nil {
		return "", nil, err
	}

	return preparedStmt, queryParams, nil
}

func (this *ExtMapper) GetSql(sqlId string) (*XmlSQL, error) {
	sql, exist := this.mapper.Id2Sql[sqlId]
	if !exist {
		return nil, errors.New(fmt.Sprintf("Sql '%s' not found", sqlId))
	}
	return sql, nil
}
