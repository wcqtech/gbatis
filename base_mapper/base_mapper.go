package base_mapper

import (
	"encoding/json"
	"fmt"
	"gitee.com/wcqtech/gbatis/gbutil"
	"gitee.com/wcqtech/gbatis/wrapper"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

type BaseMapper[T schema.Tabler] struct {
	tx        *gorm.DB
	batchSize int
}

func NewBaseMapper[T schema.Tabler](tx *gorm.DB) *BaseMapper[T] {
	bm := new(BaseMapper[T])
	bm.tx = tx
	bm.batchSize = 1000
	return bm
}

func (bm *BaseMapper[T]) SetBatchSize(size int) *BaseMapper[T] {
	bm.batchSize = size
	return bm
}

func (bm *BaseMapper[T]) SetTx(tx *gorm.DB) *BaseMapper[T] {
	bm.tx = tx
	return bm
}

func (bm *BaseMapper[T]) Insert(entity *T) error {
	return bm.tx.Model(new(T)).Create(entity).Error
}

func (bm *BaseMapper[T]) InsertBatch(entities []*T) error {
	return bm.tx.Model(new(T)).CreateInBatches(entities, bm.batchSize).Error
}

func (bm *BaseMapper[T]) DeleteById(id any) error {
	var ett *T
	pk := wrapper.GetEntityPrimaryKey(ett)
	return bm.tx.Model(ett).Where(fmt.Sprintf("%s = ?", pk), id).Delete(ett).Error
}

func (bm *BaseMapper[T]) DeleteBatchIds(ids any) error {
	var ett *T
	pk := wrapper.GetEntityPrimaryKey(ett)
	return bm.tx.Model(ett).Where(fmt.Sprintf("%s IN ?", pk), gbutil.ValidateList(ids)).Delete(ett).Error
}

func (bm *BaseMapper[T]) UpdateById(entity *T) error {
	ett := new(T)
	pk := wrapper.GetEntityPrimaryKey(ett)
	jsonData, err := json.Marshal(entity)
	if err != nil {
		return err
	}
	mp := make(map[string]any)
	if err := json.Unmarshal(jsonData, &mp); err != nil {
		return err
	}
	return bm.tx.Model(ett).Where(fmt.Sprintf("%s = ?", pk), mp[pk]).Updates(entity).Error
}

func (bm *BaseMapper[T]) Update(wp wrapper.AbstractUpdateWrapper) error {
	ett := new(T)
	return buildUpdateDb(wp, bm.tx.Model(ett)).Error
}

func (bm *BaseMapper[T]) SelectById(id any) (*T, error) {
	ett := new(T)
	pk := wrapper.GetEntityPrimaryKey(ett)
	err := bm.tx.Model(ett).Where(fmt.Sprintf("%s = ?", pk), id).First(ett).Error
	return ett, err
}

func (bm *BaseMapper[T]) SelectBatchIds(ids any) ([]*T, error) {
	ett := new(T)
	var etts []*T
	pk := wrapper.GetEntityPrimaryKey(ett)
	err := bm.tx.Model(ett).Where(fmt.Sprintf("%s IN ?", pk), gbutil.ValidateList(ids)).Find(&etts).Error
	return etts, err
}

func (bm *BaseMapper[T]) SelectOne(wp wrapper.AbstractQueryWrapper) (*T, error) {
	ett := new(T)
	err := buildQueryDb(wp, bm.tx.Model(ett)).First(ett).Error
	return ett, err
}

func (bm *BaseMapper[T]) SelectCount(wp wrapper.AbstractQueryWrapper) (int64, error) {
	ett := new(T)
	var count int64
	err := buildQueryDb(wp, bm.tx.Model(ett)).Count(&count).Error
	return count, err
}

func (bm *BaseMapper[T]) SelectList(wp wrapper.AbstractQueryWrapper) ([]*T, error) {
	var ett *T
	var etts []*T
	err := buildQueryDb(wp, bm.tx.Model(ett)).Find(&etts).Error
	return etts, err
}

func buildQueryDb(wp wrapper.AbstractQueryWrapper, db *gorm.DB) *gorm.DB {
	if wp.GetSelect() != "" {
		db.Select(wp.GetSelect())
	}
	wherePreparedStmt, whereArgs := wp.GetWhere()
	if wherePreparedStmt != "" {
		db.Where(wherePreparedStmt, whereArgs...)
	}
	if wp.GetGroupBy() != "" {
		db.Group(wp.GetGroupBy())
	}
	havingPreparedStmt, havingArgs := wp.GetHaving()
	if havingPreparedStmt != "" {
		db.Having(havingPreparedStmt, havingArgs...)
	}
	if wp.GetOrderBy() != "" {
		db.Order(wp.GetOrderBy())
	}
	if wp.GetOffset() != 0 {
		db.Offset(wp.GetOffset())
	}
	if wp.GetLimit() != 0 {
		db.Limit(wp.GetLimit())
	}

	return db
}

func buildUpdateDb(wp wrapper.AbstractUpdateWrapper, db *gorm.DB) *gorm.DB {
	wherePreparedStmt, whereArgs := wp.GetWhere()
	if wherePreparedStmt != "" {
		db.Where(wherePreparedStmt, whereArgs...)
	}
	if len(wp.GetSet()) > 0 {
		db.Updates(wp.GetSet())
	}
	return db
}
