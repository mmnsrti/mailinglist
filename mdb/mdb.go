package mdb

import (
	"database/sql"
	"github.com/mattn/go-sqlite3"
	"log"
	"time"
)

type EmailEntry struct {
	Id        int64
	Email     string
	Confirmed *time.Time
	OptOut    bool
}

func TryCreate(db *sql.DB) {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS emails (
			id INTEGER PRIMARY KEY ,
			email TEXT NOT NULL UNIQUE,
			confirmed_at INTEGER,
			opt_out INTEGER
		);
	`)
	if err != nil {
		if sqliteErr, ok := err.(sqlite3.Error); ok {
			if sqliteErr.Code != 1 {
				log.Fatalf("SQLite error code %d: %s", sqliteErr.Code, sqliteErr.Error())
			} else {
				log.Fatal(err)
			}
		}
	}
}

func emailEntryFromRows(rows *sql.Rows) (*EmailEntry, error) {
	var id int64
	var email string
	var confirmedAt int64
	var optOut bool
	err := rows.Scan(&id, &email, &confirmedAt, &optOut)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	t := time.Unix(confirmedAt, 0)
	return &EmailEntry{Id: id, Email: email, Confirmed: &t, OptOut: optOut}, nil
}

func CreateEmail(db *sql.DB, email string) error {
	_, err := db.Exec(`INSERT INTO 
	emails(email, confirmed_at, opt_out)
	VALUES(?, 0,false)
	`, email)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func GetEmail(db *sql.DB, email string) (*EmailEntry, error) {
	rows, err := db.Query(`SELECT id, email, confirmed_at, opt_out FROM emails WHERE email = ?`, email)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		return emailEntryFromRows(rows)
	}
	return nil, nil
}
func UpdateEmail(db *sql.DB, entry EmailEntry) error {
	t := entry.Confirmed.Unix()
	_, err := db.Exec(`INSERT INTO emails(email, confirmed_at, opt_out) VALUES(?, ?, ?) ON CONFLICT(email) DO UPDATE SET confirmed_at=?, opt_out=?`,
		entry.Email, t, entry.OptOut, t, entry.OptOut)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func DeleteEmail(db *sql.DB, email string) error {
	_, err := db.Exec(`UPDATE emails SET opt_out=true WHERE email = ?`, email)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

type GetEmailBatchQueryParams struct {
	Page  int `json:"page"`
	Count int `json:"count"`
}

func GetEmailBatch(db *sql.DB, params GetEmailBatchQueryParams) ([]EmailEntry, error) {
	var empty []EmailEntry
	rows, err := db.Query(`
	SELECT id, email, confirmed_at, opt_out
	FROM emails
	WHERE opt_out = false 
	ORDER BY id ASC 
	LIMIT ? OFFSET ?`, params.Count, (params.Page-1)*params.Count)
	if err != nil {
		log.Println(err)
		return empty, err
	}
	defer rows.Close()
	emails := make([]EmailEntry, 0, params.Count)
	for rows.Next() {
		email, err := emailEntryFromRows(rows)
		if err != nil {
			log.Println(err)
			return empty, err
		}
		emails = append(emails, *email)
	}
	return emails, nil
}
