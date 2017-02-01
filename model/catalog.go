package model

// transfer2go data model module
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"strings"

	"github.com/vkuznet/transfer2go/utils"

	// loads sqlite3 database layer
	_ "github.com/mattn/go-sqlite3"
)

// global pointer to DB
var DB *sql.DB

func check(msg string, err error) {
	if err != nil {
		log.Fatalf("ERROR %s, %v\n", msg, err)
	}
}

// CatalogEntry represents an entry in TFC
type CatalogEntry struct {
	Lfn     string `json:"lfn"`     // lfn stands for Logical File Name
	Pfn     string `json:"pfn"`     // pfn stands for Physical File Name
	Dataset string `json:"dataset"` // dataset represents collection of blocks
	Block   string `json:"block"`   // block idetify single block within a dataset
	Bytes   int64  `json:"bytes"`   // size of the files in bytes
	Hash    string `json:"hash"`    // hash represents checksum of the pfn
}

// Catalog represents Trivial File Catalog (TFC) of the model
type Catalog struct {
	Type     string `json:"type"`     // catalog type, e.g. filesystem, sqlite3, etc.
	Uri      string `json:"uri"`      // catalog uri, e.g. file.db
	Login    string `json:"login"`    // database login
	Password string `json:"password"` // database password
	Owner    string `json:"owner"`    // used by ORACLE DB, defines owner of the database
}

// Find method look-up entries in a catalog for a given query
func (c *Catalog) Find(stm string, cols []string, vals []interface{}, args ...interface{}) []CatalogEntry {
	var out []CatalogEntry
	rows, err := DB.Query(stm, args...)
	if err != nil {
		msg := fmt.Sprintf("ERROR DB.Query, query='%s' args='%v' error=%v", stm, args, err)
		log.Fatal(msg)
	}
	defer rows.Close()

	// loop over rows
	for rows.Next() {
		err := rows.Scan(vals...)
		if err != nil {
			msg := fmt.Sprintf("ERROR rows.Scan, vals='%v', error=%v", vals, err)
			log.Fatal(msg)
		}
		rec := CatalogEntry{}
		rec.Dataset = vals[0].(string)
		rec.Block = vals[1].(string)
		rec.Lfn = vals[2].(string)
		rec.Pfn = vals[3].(string)
		rec.Bytes = vals[4].(int64)
		rec.Hash = vals[5].(string)
		//         for i, _ := range cols {
		//             rec[cols[i]] = vals[i]
		//         }
		out = append(out, rec)
	}
	if err = rows.Err(); err != nil {
		log.Fatalf("ERROR rows.Err, %v\n", err)
	}
	return out
}

// Add method adds entry to a catalog
func (c *Catalog) Add(entry CatalogEntry) error {

	// add entry to the catalog
	tx, e := DB.Begin()
	check("Unable to setup transaction", e)

	var stm string
	var did, bid int

	// insert dataset into dataset tables
	stm = fmt.Sprintf("INSERT INTO DATASETS(dataset) VALUES(?)")
	_, e = DB.Exec(stm, entry.Dataset)
	if e != nil {
		if !strings.Contains(e.Error(), "UNIQUE") {
			check("Unable to insert into datasets table", e)
		}
	}

	// get dataset id
	stm = fmt.Sprintf("SELECT id FROM DATASETS WHERE dataset=?")
	rows, err := DB.Query(stm, entry.Dataset)
	check("Unable to perform DB.Query over datasets table", err)
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&did)
		check("Unable to scan rows for datasetid", err)
	}

	// insert block into block table
	stm = fmt.Sprintf("INSERT INTO BLOCKS(block) VALUES(?)")
	_, e = DB.Exec(stm, entry.Block)
	if e != nil {
		if !strings.Contains(e.Error(), "UNIQUE") {
			check("Unable to insert into blocks table", e)
		}
	}

	// get block id
	stm = fmt.Sprintf("SELECT id FROM BLOCKS WHERE block=?")
	rows, err = DB.Query(stm, entry.Block)
	check("Unabel to DB.Query over blocks table", err)
	for rows.Next() {
		err = rows.Scan(&bid)
		check("Unable to scan rows for datasetid", err)
	}

	// insert entry into files table
	stm = fmt.Sprintf("INSERT INTO FILES(lfn, pfn, blockid, datasetid, bytes, hash) VALUES(?,?,?,?,?,?)")
	_, err = DB.Exec(stm, entry.Lfn, entry.Pfn, bid, did, entry.Bytes, entry.Hash)
	if e != nil {
		if !strings.Contains(e.Error(), "UNIQUE") {
			check(fmt.Sprintf("Unable to DB.Exec(%s)", stm), err)
		}
	}

	tx.Commit()

	if utils.VERBOSE > 0 {
		log.Println("Committed to Catalog", entry, "datasetid", did, "blockid", bid)
	}

	return nil
}

// Files method of catalog returns list of files known in catalog
func (c *Catalog) Files(pattern string) []string {
	var files []string
	if c.Type == "filesystem" {
		filesInfo, err := ioutil.ReadDir(c.Uri)
		if err != nil {
			log.Println("ERROR: unable to list files in catalog", c.Uri, err)
			return []string{}
		}
		for _, f := range filesInfo {
			if pattern != "" {
				if strings.Contains(f.Name(), pattern) {
					files = append(files, fmt.Sprintf("%s/%s", c.Uri, f.Name()))
				}
			} else {
				files = append(files, fmt.Sprintf("%s/%s", c.Uri, f.Name()))
			}
		}
		return files
	} else if c.Type == "sqlite3" {
		// construct SQL query
		var args []interface{} // argument values passed to SQL statement
		cols := []string{"dataset", "blockid", "lfn", "pfn", "bytes", "hash"}
		stm := fmt.Sprintf("SELECT %s FROM FILES AS F JOIN BLOCKS AS B ON F.BLOCKID=B.ID JOIN DATASETS AS D ON F.DATASETID = D.ID", strings.Join(cols, ","))
		vals := []interface{}{new(sql.NullString), new(sql.NullString), new(sql.NullString), new(sql.NullString), new(sql.NullInt64), new(sql.NullString)}
		for _, entry := range c.Find(stm, cols, vals, args) {
			files = append(files, entry.Lfn)
		}
		return files
	}
	return files
}

// FileInfo provides information about given file name in Catalog
func (c *Catalog) FileInfo(fileEntry string) CatalogEntry {
	if c.Type == "filesystem" {
		fname := fileEntry
		data, err := ioutil.ReadFile(fname)
		if err != nil {
			log.Println("ERROR, unable to read a file", fname, err)
		}
		hash, b := utils.Hash(data)
		entry := CatalogEntry{Lfn: fname, Pfn: fname, Hash: hash, Bytes: b, Dataset: "/a/b/c", Block: "123"}
		return entry
	} else if c.Type == "sqlite3" {
		cols := []string{"pfn", "bytes", "hash"}
		stm := fmt.Sprintf("SELECT %s FROM FILES AS F WHERE LFN = ?", strings.Join(cols, ","))
		vals := []interface{}{new(sql.NullString), new(sql.NullInt64), new(sql.NullString)}
		for _, entry := range c.Find(stm, cols, vals, fileEntry) {
			return entry
		}
	}
	return CatalogEntry{}
}

// TFC stands for Trivial File Catalog
var TFC Catalog
