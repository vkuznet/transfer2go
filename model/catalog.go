package model

// transfer2go data model module
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os/exec"
	"strings"

	"github.com/vkuznet/transfer2go/utils"

	// loads sqlite3 database layer
	_ "github.com/mattn/go-sqlite3"
)

// main DB record we work with
type Record map[string]interface{}

// DB is global pointer to sql database object, it is initialized once when server starts
var DB *sql.DB
var DBTYPE string
var DBSQL Record

func check(msg string, err error) {
	if err != nil {
		log.Fatalf("ERROR %s, %v\n", msg, err)
	}
}

// helper function to load DBS SQL statements
func LoadSQL(owner string) Record {
	dbsql := make(Record)
	// query statement
	tmplData := make(Record)
	tmplData["Owner"] = owner
	sdir := fmt.Sprintf("%s/sql", utils.STATICDIR)
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

// Dump method returns TFC dump in CSV format
func (c *Catalog) Dump() []byte {
	if c.Type == "sqlite3" {
		//         cmd := fmt.Sprintf("sqlite3 %s .dump", c.Uri)
		out, err := exec.Command("sqlite3", c.Uri, ".dump").Output()
		if err != nil {
			log.Println("ERROR c.Dump", err)
		}
		return out
	}
	log.Println("Catalog Dump method is not implemented yet for", c.Type)
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
	check("Unable to DB.Query over blocks table", err)
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
	} else {
		// TODO: make generic database statements via templates for given database type
		//         cols := []string{"dataset", "blockid", "lfn", "pfn", "bytes", "hash"}
		//         stm := fmt.Sprintf("SELECT %s FROM FILES AS F JOIN BLOCKS AS B ON F.BLOCKID=B.ID JOIN DATASETS AS D ON F.DATASETID = D.ID WHERE F.LFN=?", strings.Join(cols, ","))
		stm := getSQL("files_blocks_datasets") + fmt.Sprintf(" WHERE F.LFN=%s", placeholder("lfn"))
		if utils.VERBOSE > 0 {
			log.Println("Files query", stm, pattern)
		}
		vals := []interface{}{new(sql.NullString), new(sql.NullString), new(sql.NullString), new(sql.NullString), new(sql.NullInt64), new(sql.NullString)}

		// fetch data from DB
		rows, err := DB.Query(stm, pattern)
		if err != nil {
			log.Printf("ERROR DB.Query, query='%s' error=%v\n", stm, err)
			return files
		}
		defer rows.Close()
		for rows.Next() {
			rec := CatalogEntry{}
			err := rows.Scan(&rec.Dataset, &rec.Block, &rec.Lfn, &rec.Pfn, &rec.Bytes, &rec.Hash)
			if err != nil {
				msg := fmt.Sprintf("ERROR rows.Scan, vals='%v', error=%v", vals, err)
				log.Fatal(msg)
			}
			files = append(files, rec.Lfn)
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
		// TODO: I need to know how to generate dataset and block names in this case
		entry := CatalogEntry{Lfn: fname, Pfn: fname, Hash: hash, Bytes: b, Dataset: "/a/b/c", Block: "123"}
		return entry
	} else {
		// TODO: make generic SQL statement via templates
		//         cols := []string{"lfn", "pfn", "bytes", "hash"}
		//         stm := fmt.Sprintf("SELECT %s FROM FILES AS F WHERE LFN = ?", strings.Join(cols, ","))
		stm := getSQL("files") + fmt.Sprintf(" WHERE F.LFN=%s", placeholder("lfn"))
		if utils.VERBOSE > 0 {
			log.Println("FileInfo query", stm, fileEntry)
		}

		// fetch data from DB
		rows, err := DB.Query(stm, fileEntry)
		if err != nil {
			log.Printf("ERROR DB.Query, query='%s' error=%v\n", stm, err)
			return CatalogEntry{}
		}
		defer rows.Close()
		for rows.Next() {
			rec := CatalogEntry{}
			err := rows.Scan(&rec.Lfn, &rec.Pfn, &rec.Bytes, &rec.Hash)
			if err != nil {
				log.Println("ERROR rows.Scan", err)
				return CatalogEntry{}
			}
			return rec
		}
	}
	return CatalogEntry{}
}

// TFC stands for Trivial File Catalog
var TFC Catalog
