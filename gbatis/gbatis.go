package gbatis

import "gorm.io/gorm"

var (
	conf = &gorm.Config{}
)

func Conf(config *gorm.Config) {
	conf = config
}

func GetConf() *gorm.Config {
	return conf
}
