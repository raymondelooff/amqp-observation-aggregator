package aggregator

import (
	"database/sql"
	"fmt"

	"go.uber.org/zap"
)

// WriterConfig represents the config of the Writer
type WriterConfig struct {
}

// Writer inserts or updates observation updates
type Writer struct {
	config WriterConfig
	db     *sql.DB
	stmt   *sql.Stmt
	logger *zap.SugaredLogger
}

func (w *Writer) prepareStmt() (*sql.Stmt, error) {
	if w.stmt != nil {
		return w.stmt, nil
	}

	var err error

	sql := "INSERT INTO `weather_condition` (`type`, `value`, `modified_at`)" +
		"VALUES (?, ?, NOW()) " +
		"ON DUPLICATE KEY UPDATE " +
		"`type` = VALUES(type), " +
		"`value` = VALUES(value), " +
		"`modified_at` = VALUES(modified_at)"

	w.stmt, err = w.db.Prepare(sql)
	if err != nil {
		return nil, fmt.Errorf("Writer: %s", err)
	}

	return w.stmt, nil
}

// Write inserts or updates a single observation
func (w *Writer) Write(observationUpdate *ObservationUpdate) error {
	stmt, err := w.prepareStmt()
	if err != nil {
		return err
	}

	stmt.Close()

	return nil
}

// NewWriter creates a new Writer
func NewWriter(config WriterConfig, db *sql.DB, logger *zap.SugaredLogger) *Writer {
	return &Writer{
		config: config,
		db:     db,
		logger: logger,
	}
}
