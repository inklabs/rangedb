package postgresstore

import (
	"fmt"
)

type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
}

func (c Config) DataSourceName() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		c.Host, c.Port, c.User, c.Password, c.DBName)
}
