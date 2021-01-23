package postgreskeystore

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/lib/pq"

	"github.com/inklabs/rangedb/pkg/crypto"
	"github.com/inklabs/rangedb/provider/postgresstore"
)

const (
	PgUniqueViolationCode             = pq.ErrorCode("23505")
	PgDuplicateSubjectIDViolation     = "vault_pkey"
	PgDuplicateEncryptionKeyViolation = "vault_encryptionkey_key"
)

type postgresKeyStore struct {
	config *postgresstore.Config
	db     *sql.DB
}

func New(config *postgresstore.Config) (*postgresKeyStore, error) {
	p := &postgresKeyStore{
		config: config,
	}

	err := p.connectToDB()
	if err != nil {
		return nil, err
	}
	err = p.initDB()
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (p *postgresKeyStore) Get(subjectID string) (string, error) {
	row := p.db.QueryRow("SELECT EncryptionKey, DeletedAtTimestamp FROM vault WHERE SubjectID = $1",
		subjectID)

	var encryptionKey string
	var deletedAtTimestamp *uint64
	err := row.Scan(&encryptionKey, &deletedAtTimestamp)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", crypto.ErrKeyNotFound
		}

		return "", err
	}

	if deletedAtTimestamp != nil {
		return "", crypto.ErrKeyWasDeleted
	}

	return encryptionKey, nil
}

func (p *postgresKeyStore) Set(subjectID, encryptionKey string) error {
	if encryptionKey == "" {
		return crypto.ErrInvalidKey
	}

	_, err := p.db.Exec("INSERT INTO vault (SubjectID, EncryptionKey) VALUES ($1, $2)",
		subjectID, encryptionKey)
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			if err.Code == PgUniqueViolationCode {
				switch err.Constraint {
				case PgDuplicateSubjectIDViolation:
					return crypto.ErrKeyExistsForSubjectID

				case PgDuplicateEncryptionKeyViolation:
					return crypto.ErrKeyAlreadyUsed
				}
			}
		}
		return err
	}

	return nil
}

func (p *postgresKeyStore) Delete(subjectID string) error {
	_, err := p.db.Exec("UPDATE vault SET DeletedAtTimestamp = $1 WHERE SubjectID = $2",
		time.Now().Unix(),
		subjectID)
	if err != nil {
		return err
	}

	return nil
}

func (p *postgresKeyStore) connectToDB() error {
	db, err := sql.Open("postgres", p.config.DataSourceName())
	if err != nil {
		return fmt.Errorf("unable to open DB connection: %v", err)
	}

	err = db.Ping()
	if err != nil {
		return fmt.Errorf("unable to connect to DB: %v", err)
	}

	p.db = db

	return nil
}

func (p *postgresKeyStore) initDB() error {
	sqlStatements := []string{
		`CREATE TABLE IF NOT EXISTS vault (
			SubjectID TEXT PRIMARY KEY,
			EncryptionKey TEXT UNIQUE,
			DeletedAtTimestamp BIGINT
		);`,
	}

	for _, statement := range sqlStatements {
		_, err := p.db.Exec(statement)
		if err != nil {
			return err
		}
	}

	return nil
}
