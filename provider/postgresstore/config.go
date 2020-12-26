package postgresstore

import (
	"fmt"
)

// Config holds the state for a PostgreSQL DB config.
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
}

// DataSourceName returns the DSN for a PostgreSQL DB.
func (c Config) DataSourceName() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		c.Host, c.Port, c.User, c.Password, c.DBName)
}
