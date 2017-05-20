package core

// transfer2go implementation of Trivial File Catalog (TFC)
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"database/sql"
	"fmt"
	"os/exec"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/vkuznet/transfer2go/utils"

	// loads sqlite3 database layer
	_ "github.com/mattn/go-sqlite3"
)

// Record represent main DB record we work with
type Record map[string]interface{}

// DB is global pointer to sql database object, it is initialized once when server starts
var DB *sql.DB

// DBTYPE holds database type, e.g. sqlite3
var DBTYPE string

// DBSQL represent common record we get from DB SQL statement
var DBSQL Record

// TFC stands for Trivial File Catalog
var TFC Catalog

// CatalogEntry represents an entry in TFC
type CatalogEntry struct {
	Lfn          string `json:"lfn"`          // lfn stands for Logical File Name
	Pfn          string `json:"pfn"`          // pfn stands for Physical File Name
	Dataset      string `json:"dataset"`      // dataset represents collection of blocks
	Block        string `json:"block"`        // block idetify single block within a dataset
	Bytes        int64  `json:"bytes"`        // size of the files in bytes
	Hash         string `json:"hash"`         // hash represents checksum of the pfn
	TransferTime int64  `json:"transferTime"` // transfer time
	Timestamp    int64  `json:"timestamp"`    // time stamp
}

// Catalog represents Trivial File Catalog (TFC) of the model
type Catalog struct {
	Type     string `json:"type"`     // catalog type, e.g. sqlite3, etc.
	Uri      string `json:"uri"`      // catalog uri, e.g. file.db
	Login    string `json:"login"`    // database login
	Password string `json:"password"` // database password
	Owner    string `json:"owner"`    // used by ORACLE DB, defines owner of the database
}

func check(msg string, err error) {
	if err != nil {
		log.WithFields(log.Fields{
			"Message": msg,
			"Err":     err,
		}).Fatal("Stop process")
	}
}

// LoadSQL is a helper function to load DBS SQL statements
func LoadSQL(dbtype, owner string) Record {
	dbsql := make(Record)
	// query statement
	tmplData := make(Record)
	tmplData["Owner"] = owner
	sdir := fmt.Sprintf("%s/sql/%s", utils.STATICDIR, dbtype)
	for _, f := range utils.ListFiles(sdir) {
		k := strings.Split(f, ".")[0]
		dbsql[k] = utils.ParseTmpl(sdir, f, tmplData)
	}
	return dbsql
}

// helper function to get SQL statement from DBSQL dict for a given key
func getSQL(key string) string {
	// use generic query API to fetch the results from DB
	stm, ok := DBSQL[key]
	if !ok {
		msg := fmt.Sprintf("Unable to load %s SQL", key)
		log.Fatal(msg)
	}
	return stm.(string)
}

// helper function to assign placeholder for SQL WHERE clause, it depends on database type
func placeholder(pholder string) string {
	if DBTYPE == "ora" || DBTYPE == "oci8" {
		return fmt.Sprintf(":%s", pholder)
	} else if DBTYPE == "PostgreSQL" {
		return fmt.Sprintf("$%s", pholder)
	} else {
		return "?"
	}
}

// String provides string representation of CatalogEntry
func (c *CatalogEntry) String() string {
	return fmt.Sprintf("<CatalogEntry: dataset=%s block=%s lfn=%s pfn=%s bytes=%d hash=%s transferTime=%d timestamp=%d>", c.Dataset, c.Block, c.Lfn, c.Pfn, c.Bytes, c.Hash, c.TransferTime, c.Timestamp)
}

// Dump method returns TFC dump in CSV format
func (c *Catalog) Dump() []byte {
	if c.Type == "sqlite3" {
		//         cmd := fmt.Sprintf("sqlite3 %s .dump", c.Uri)
		out, err := exec.Command("sqlite3", c.Uri, ".dump").Output()
		if err != nil {
			log.WithFields(log.Fields{
				"Err": err,
			}).Error("c.Dump")
		}
		return out
	}
	log.WithFields(log.Fields{
		"Type": c.Type,
	}).Println("Catalog Dump method is not implemented yet for")

	return nil
}

// Add method adds entry to a catalog
func (c *Catalog) Add(entry CatalogEntry) error {

	// add entry to the catalog
	tx, e := DB.Begin()
	check("Unable to setup transaction", e)

	var stm string
	var did, bid int

	// insert dataset into dataset tables
	stm = getSQL("insert_datasets")
	_, e = DB.Exec(stm, entry.Dataset)
	if e != nil {
		if !strings.Contains(e.Error(), "UNIQUE") {
			check("Unable to insert into datasets table", e)
		}
	}

	// get dataset id
	stm = getSQL("id_datasets")
	rows, err := DB.Query(stm, entry.Dataset)
	check("Unable to perform DB.Query over datasets table", err)
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&did)
		check("Unable to scan rows for datasetid", err)
	}

	// insert block into block table
	stm = getSQL("insert_blocks")
	_, e = DB.Exec(stm, entry.Block)
	if e != nil {
		if !strings.Contains(e.Error(), "UNIQUE") {
			check("Unable to insert into blocks table", e)
		}
	}

	// get block id
	stm = getSQL("id_blocks")
	rows, err = DB.Query(stm, entry.Block)
	check("Unable to DB.Query over blocks table", err)
	for rows.Next() {
		err = rows.Scan(&bid)
		check("Unable to scan rows for datasetid", err)
	}

	// insert entry into files table
	stm = getSQL("insert_files")
	_, e = DB.Exec(stm, entry.Lfn, entry.Pfn, bid, did, entry.Bytes, entry.Hash, entry.TransferTime, entry.Timestamp)
	if e != nil {
		if !strings.Contains(e.Error(), "UNIQUE") {
			check(fmt.Sprintf("Unable to DB.Exec(%s)", stm), e)
		}
	}

	tx.Commit()

	if utils.VERBOSE > 0 {
		log.WithFields(log.Fields{
			"Entry":      entry.String(),
			"Dataset Id": did,
			"Block Id":   bid,
		}).Println("Committed to Catalog")
	}

	return nil
}

// Files returns list of files for specified conditions
func (c *Catalog) Files(dataset, block, lfn string) []string {
	var files []string
	req := TransferRequest{Dataset: dataset, Block: block, File: lfn}
	for _, rec := range c.Records(req) {
		files = append(files, rec.Lfn)
	}
	return files
}

// Records returns catalog records for a given transfer request
func (c *Catalog) Records(req TransferRequest) []CatalogEntry {
	stm := getSQL("files_blocks_datasets")
	var cond []string
	var vals []interface{}
	if req.File != "" {
		cond = append(cond, fmt.Sprintf("F.LFN=%s", placeholder("lfn")))
		vals = append(vals, req.File)
	}
	if req.Block != "" {
		cond = append(cond, fmt.Sprintf("B.BLOCK=%s", placeholder("block")))
		vals = append(vals, req.Block)
	}
	if req.Dataset != "" {
		cond = append(cond, fmt.Sprintf("D.DATASET=%s", placeholder("dataset")))
		vals = append(vals, req.Dataset)
	}
	if len(cond) > 0 {
		stm += fmt.Sprintf(" WHERE %s", strings.Join(cond, " AND "))
	}

	if utils.VERBOSE > 0 {
		log.WithFields(log.Fields{
			"Query": stm,
			"Value": vals,
		}).Println("Records query")
	}

	// fetch data from DB
	rows, err := DB.Query(stm, vals...)
	if err != nil {
		log.WithFields(log.Fields{
			"Query": stm,
			"Error": err,
		}).Error("DB.Query")
		return []CatalogEntry{}
	}
	defer rows.Close()
	var out []CatalogEntry
	for rows.Next() {
		rec := CatalogEntry{}
		err := rows.Scan(&rec.Dataset, &rec.Block, &rec.Lfn, &rec.Pfn, &rec.Bytes, &rec.Hash)
		if err != nil {
			log.WithFields(log.Fields{
				"Err": err,
			}).Error("rows.Scan")
		}
		out = append(out, rec)
	}
	return out
}

// Transfers method returns transfers of the agent in given time interval
func (c *Catalog) Transfers(time0, time1 string) []CatalogEntry {
	stm := getSQL("transfers")
	var vals []interface{}
	stm += fmt.Sprintf(" WHERE TIMESTAMP>=%s AND TIMESTAMP<=%s", time0, time1)
	vals = append(vals, time0)
	vals = append(vals, time1)

	if utils.VERBOSE > 0 {
		log.WithFields(log.Fields{
			"Query": stm,
			"Value": vals,
		}).Println("Records query", stm, vals)
	}

	// fetch data from DB
	rows, err := DB.Query(stm, vals...)
	if err != nil {
		log.WithFields(log.Fields{
			"Query": stm,
			"Err":   err,
		}).Error("DB.Query")
		return []CatalogEntry{}
	}
	defer rows.Close()
	var out []CatalogEntry
	for rows.Next() {
		rec := CatalogEntry{}
		err := rows.Scan(&rec.Bytes, &rec.TransferTime)
		if err != nil {
			log.WithFields(log.Fields{
				"Err": err,
			}).Error("rows.Scan")
		}
		out = append(out, rec)
	}
	return out
}
