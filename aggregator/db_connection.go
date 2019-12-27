package aggregator

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql" // MySQL driver
)

// MySQLConfig is represents the MySQL configuration
type MySQLConfig struct {
	DSN string `yaml:"dsn"`
}

// NewDbConnection opens a new connection using the configured DSN
func NewDbConnection(config MySQLConfig) (*sql.DB, error) {
	db, err := sql.Open("mysql", config.DSN)
	if err != nil {
		return nil, fmt.Errorf("database connection error: %s", err)
	}

	return db, nil
}
