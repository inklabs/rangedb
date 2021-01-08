package postgresstore

import (
	"fmt"
	"os"
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

func NewConfigFromEnvironment() (*Config, error) {
	pgHost := os.Getenv("PG_HOST")
	pgUser := os.Getenv("PG_USER")
	pgPassword := os.Getenv("PG_PASSWORD")
	pgDBName := os.Getenv("PG_DBNAME")

	if pgHost == "" || pgUser == "" || pgDBName == "" {
		return nil, fmt.Errorf("postgreSQL DB has not been configured via environment variables")
	}

	return &Config{
		Host:     pgHost,
		Port:     5432,
		User:     pgUser,
		Password: pgPassword,
		DBName:   pgDBName,
	}, nil
}
