package aggregator

import (
	"database/sql"
)

// WriterConfig represents the config of the Writer
type WriterConfig struct {
}

// Writer inserts or updates an Aggregation
type Writer struct {
	config WriterConfig
	db     *sql.DB
}

// NewWriter creates a new Writer
func NewWriter(config WriterConfig, db *sql.DB) *Writer {
	return &Writer{
		config: config,
		db:     db,
	}
}
