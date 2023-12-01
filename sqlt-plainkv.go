// Package SQLtplainkv is a package implementing PlainKVer using SQlite
package sqltplainkv

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	_ "github.com/glebarez/go-sqlite"
)

// SQLtPlainKV is a key-value database that uses
// SQLite database as its storage backend
type SQLtPlainKV struct {
	DSN           string // Data Source Name
	db            *sql.DB
	tx            *sql.Tx
	currBuckt     string
	defTableName  string
	autoClose     bool
	inTransaction bool
}

const (
	mimeBuckt string = `--mime--`
	tallyKey  string = `_______#tally-%s`
)

var (
	ErrBucketIdTooLong error = errors.New(`bucket id too long`)
	ErrKeyTooLong      error = errors.New(`key too long`)
	ErrValueTooLong    error = errors.New(`value too large`)
)

// NewSQLtPlainKV creates a new SQLtPlainKV object
// This is the recommended method
func NewSQLtPlainKV(dsn string, autoClose bool) *SQLtPlainKV {
	return &SQLtPlainKV{
		DSN:          dsn,
		currBuckt:    `default`,
		autoClose:    autoClose,
		defTableName: `KeyValueTBL`,
	}
}

func (p *SQLtPlainKV) get(bucket, key string) ([]byte, error) {

	var (
		err error
		val []byte
	)

	val = make([]byte, 0)
	if err = p.Open(); err != nil {
		return val, err
	}
	if p.autoClose {
		defer p.Close()
	}
	if bucket == "" {
		bucket = "default"
	}

	sqlstr := `
	SELECT Value FROM ` + p.defTableName + `
	WHERE Bucket=?
		AND KeyID=?;`
	if p.inTransaction {
		err = p.tx.QueryRow(sqlstr, bucket, key).Scan(&val)
	} else {
		err = p.db.QueryRow(sqlstr, bucket, key).Scan(&val)
	}
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return val, err
		}
	}
	return val, nil
}

// Set creates or updates the record by the value
func (p *SQLtPlainKV) set(bucket, key string, value []byte) error {
	var err error

	if err = p.Open(); err != nil {
		return err
	}
	if p.autoClose {
		defer p.Close()
	}
	if len(bucket) > 50 {
		return ErrBucketIdTooLong
	}
	if len(key) > 300 {
		return ErrKeyTooLong
	}
	if len(value) > 16777215 {
		return ErrValueTooLong
	}
	sqlstr := `
	INSERT INTO ` + p.defTableName + ` (Bucket, KeyID, Value) VALUES (?, ?, ?)
	ON CONFLICT(Bucket,KeyID) DO UPDATE SET Value=excluded.Value;`
	if p.inTransaction {
		_, err = p.tx.Exec(sqlstr, bucket, key, value)
	} else {
		_, err = p.db.Exec(sqlstr, bucket, key, value)
	}
	if err != nil {
		return err
	}

	return nil
}

// Get retrieves a record using a key
func (p *SQLtPlainKV) Get(key string) ([]byte, error) {
	return p.get(p.currBuckt, key)
}

// Get retrieves a record using a key
func (p *SQLtPlainKV) GetMime(key string) (string, error) {

	val, err := p.get(mimeBuckt, key)
	if err != nil || len(val) == 0 {
		return "text/html", err
	}

	return string(val), nil
}

// Set creates or updates the record by the value
func (p *SQLtPlainKV) Set(key string, value []byte) error {
	if p.currBuckt == "" {
		p.currBuckt = "default"
	}
	if err := p.set(p.currBuckt, key, value); err != nil {
		return err
	}
	return nil
}

// SetMime sets the mime of the value stored
func (p *SQLtPlainKV) SetMime(key string, mime string) error {
	if err := p.set(mimeBuckt, key, []byte(mime)); err != nil {
		return err
	}
	return nil
}

// SetBucket sets the current bucket.
// If set, all succeeding values will be retrieved and stored by the bucket name
func (p *SQLtPlainKV) SetBucket(bucket string) {
	p.currBuckt = bucket
}

// Del deletes a record with the provided key
func (p *SQLtPlainKV) Del(key string) error {
	var err error
	if err = p.Open(); err != nil {
		return err
	}
	if p.autoClose {
		defer p.Close()
	}
	if p.currBuckt == "" {
		p.currBuckt = "default"
	}
	sqlstr := `DELETE FROM ` + p.defTableName + ` WHERE Bucket = ? AND KeyID = ?;`

	if p.inTransaction {
		if _, err = p.tx.Exec(sqlstr, p.currBuckt, key); err != nil {
			return err
		}
		if _, err = p.tx.Exec(sqlstr, mimeBuckt, key); err != nil {
			return err
		}
		return nil
	}

	if _, err = p.db.Exec(sqlstr, p.currBuckt, key); err != nil {
		return err
	}
	if _, err = p.db.Exec(sqlstr, mimeBuckt, key); err != nil {
		return err
	}
	return nil
}

// ListKeys lists all keys containing the current pattern
func (p *SQLtPlainKV) ListKeys(pattern string) ([]string, error) {
	var (
		err error
		val []string
		k   string
		sqr *sql.Rows
	)

	val = make([]string, 0)
	if err = p.Open(); err != nil {
		return val, err
	}
	if p.autoClose {
		defer p.Close()
	}
	if p.currBuckt == "" {
		p.currBuckt = "default"
	}
	sqlstr := `SELECT KeyID FROM ` + p.defTableName + ` WHERE Bucket=? AND KeyID LIKE ?;`
	if p.inTransaction {
		sqr, err = p.tx.Query(sqlstr, p.currBuckt, pattern+"%")
	} else {
		sqr, err = p.db.Query(sqlstr, p.currBuckt, pattern+"%")
	}
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return val, err
		}
	}
	defer sqr.Close()
	for sqr.Next() {
		if err = sqr.Scan(&k); err != nil {
			return val, err
		}
		val = append(val, k)
	}
	if err = sqr.Err(); err != nil {
		return val, err
	}
	return val, nil
}

// Tally gets the current tally of a key.
// To start with a pre-defined number, set the offset variable
// It automatically creates new key if it does not exist
func (p *SQLtPlainKV) Tally(key string, offset int) (int, error) {
	tk := fmt.Sprintf(tallyKey, key)
	tlly, err := p.get(p.currBuckt, tk)
	if err != nil {
		return -1, err
	}
	if len(tlly) == 0 {
		if err = p.set(p.currBuckt, tk, []byte(strconv.Itoa(offset))); err != nil {
			return -1, err
		}
	}
	tv := string(tlly)
	tvv, _ := strconv.Atoi(tv)
	return tvv, nil
}

// Incr increments the tally
func (p *SQLtPlainKV) TallyIncr(key string) (int, error) {

	tlly, err := p.Tally(key, 0)
	if err != nil {
		return tlly, err
	}
	tk := fmt.Sprintf(tallyKey, key)
	if err = p.set(
		p.currBuckt,
		tk,
		[]byte(strconv.Itoa(tlly+1))); err != nil {
		return tlly, err
	}
	return tlly + 1, nil
}

// Decr decrements the tally
func (p *SQLtPlainKV) TallyDecr(key string) (int, error) {
	tlly, err := p.Tally(key, 0)
	if err != nil {
		return tlly, err
	}
	tk := fmt.Sprintf(tallyKey, key)
	if err = p.set(
		p.currBuckt,
		tk,
		[]byte(strconv.Itoa(tlly-1))); err != nil {
		return tlly, err
	}
	return tlly - 1, nil
}

// Reset resets tally to zero
func (p *SQLtPlainKV) TallyReset(key string) error {
	tk := fmt.Sprintf(tallyKey, key)
	if err := p.set(
		p.currBuckt,
		tk,
		[]byte("0")); err != nil {
		return err
	}
	return nil
}

// Open a connection to a MySQL database database
func (p *SQLtPlainKV) Open() error {
	if p.db != nil {
		return nil
	}
	p.inTransaction = false
	var err error
	p.db, err = sql.Open("sqlite", p.DSN)
	if err != nil {
		return err
	}

	// See "Important settings" section.
	p.db.SetConnMaxLifetime(time.Minute * 3)
	p.db.SetMaxOpenConns(10)
	p.db.SetMaxIdleConns(10)

	// Check if table exists and create it if not
	sql :=
		`CREATE TABLE IF NOT EXISTS ` + p.defTableName + ` (
			Bucket VARCHAR(50),
			KeyID VARCHAR(300),
			Value MEDIUMBLOB,
			PRIMARY KEY (Bucket, KeyID)
		);`
	_, err = p.db.Exec(sql)
	if err != nil {
		return err
	}
	return nil
}

// Begin a transaction
func (p *SQLtPlainKV) Begin() error {
	var err error
	if p.tx, err = p.db.Begin(); err != nil {
		return err
	}
	p.inTransaction = true
	return nil
}

// Commit transaction
func (p *SQLtPlainKV) Commit() error {
	if p.tx == nil {
		return nil // silently commit
	}
	if err := p.tx.Commit(); err != nil {
		return err
	}
	p.inTransaction = false
	return nil
}

// Rollback transaction
func (p *SQLtPlainKV) Rollback() error {
	if p.tx == nil {
		return nil // silently rollback
	}
	if err := p.tx.Rollback(); err != nil {
		return err
	}
	p.inTransaction = false
	return nil
}

// Close closes the database
func (p *SQLtPlainKV) Close() error {
	if p.tx != nil {
		p.tx = nil
	}
	if p.db == nil {
		return nil
	}
	if err := p.db.Close(); err != nil {
		return err
	}
	p.db = nil
	return nil
}

// SetTableName changes the default table name
func (p *SQLtPlainKV) SetTableName(tableName string) {
	p.defTableName = tableName
}
