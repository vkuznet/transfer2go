package core

// transfer2go implementation of Trivial File Catalog (TFC)
// Author: Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"database/sql"
	"errors"
	"fmt"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"time"

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

// TransferData helps to structure the rows of transfers table
type TransferData struct {
	Timestamp  int64   `json:"timestamp"`  // Helps to get data historically
	CpuUsage   float64 `json:"cpu"`        // percentage of cpu used
	MemUsage   float64 `json:"ram"`        // ram used in MB
	Throughput float64 `json:"throughput"` // network throughput during transfer in MB
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
		path := strings.Split(f, "/")
		k := strings.Split(path[len(path)-1], ".")[0]
		dbsql[k] = utils.ParseTmpl(f, tmplData)
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
	_, e = DB.Exec(stm, entry.Block, did)
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
	req := TransferRequest{Dataset: dataset, Block: block, Lfn: lfn}
	for _, rec := range c.Records(req) {
		files = append(files, rec.Lfn)
	}
	return files
}

// PfnFiles returns list of files for specified conditions
func (c *Catalog) PfnFiles(dataset, block, lfn string) []string {
	var files []string
	req := TransferRequest{Dataset: dataset, Block: block, Lfn: lfn}
	for _, rec := range c.Records(req) {
		files = append(files, rec.Pfn)
	}
	return files
}

// helper function to convert interface into string representation
func asString(src interface{}) string {
	switch v := src.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	}
	rv := reflect.ValueOf(src)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(rv.Uint(), 10)
	case reflect.Float64:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 64)
	case reflect.Float32:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 32)
	case reflect.Bool:
		return strconv.FormatBool(rv.Bool())
	}
	return fmt.Sprintf("%v", src)
}

// Snapshot returns a snapshot of the TFC catalog and return it as a map
// which holds table names and list of rows where each row is represented
// as a comma separated values
func (c *Catalog) Snapshot() map[string][]string {
	maps := make(map[string][]string)
	snapshots := []string{"files", "blocks", "datasets", "requests", "transfers"}
	for _, name := range snapshots {
		stm := getSQL(fmt.Sprintf("snapshot_%s", name))
		// fetch data from DB
		rows, err := DB.Query(stm)
		if err != nil {
			log.WithFields(log.Fields{
				"Query": stm,
				"Error": err,
			}).Error("DB.Query")
			return maps
		}
		defer rows.Close()
		cols, err := rows.Columns() // Remember to check err afterwards
		if err != nil {
			log.WithFields(log.Fields{
				"Error": err,
			}).Error("Unable to get column names")
			return maps
		}
		values := make([]interface{}, len(cols))
		args := make([]interface{}, len(values))
		for i := range values {
			args[i] = &values[i]
		}
		var out []string
		for rows.Next() {
			err := rows.Scan(args...)
			if err != nil {
				log.WithFields(log.Fields{
					"Err": err,
				}).Error("rows.Scan")
			}
			var rowValues []string
			for i, _ := range cols {
				rowValues = append(rowValues, asString(values[i]))
			}
			out = append(out, strings.Join(rowValues, ","))
		}
		maps[name] = out
	}
	return maps
}

// Records returns catalog records for a given transfer request
func (c *Catalog) Records(req TransferRequest) []CatalogEntry {
	stm := getSQL("files_blocks_datasets")
	var cond []string
	var vals []interface{}
	if req.Lfn != "" {
		cond = append(cond, fmt.Sprintf("F.LFN=%s", placeholder("lfn")))
		vals = append(vals, req.Lfn)
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

// GetTransfers provide details about transfers in given time interval
func (c *Catalog) GetTransfers(time0, time1 string) ([]TransferData, error) {
	stm := getSQL("get_transfers")
	// fetch data from DB
	rows, err := DB.Query(stm, time0, time1)
	if err != nil {
		log.WithFields(log.Fields{
			"Query": stm,
			"Err":   err,
		}).Error("DB.Query")
		return []TransferData{}, err
	}
	defer rows.Close()
	var out []TransferData
	for rows.Next() {
		rec := TransferData{}
		err := rows.Scan(&rec.Timestamp, &rec.CpuUsage, &rec.MemUsage, &rec.Throughput)
		if err != nil {
			log.WithFields(log.Fields{
				"Err": err,
			}).Error("Exec: rows.Scan")
		}
		out = append(out, rec)
	}
	return out, nil
}

// InsertRequest inserts new request
func (c *Catalog) InsertRequest(r TransferRequest) error {
	stm := getSQL("insert_request")
	_, e := DB.Exec(stm, r.Id, r.Lfn, r.Block, r.Dataset, r.SrcUrl, r.SrcAlias, r.DstUrl, r.DstAlias, r.RegUrl, r.RegAlias, "pending", r.Priority)
	log.WithFields(log.Fields{
		"Request": r,
	}).Info("Catalog: InsertRequest")
	if e != nil {
		if !strings.Contains(e.Error(), "UNIQUE") {
			check("Unable to insert into REQUESTS table", e)
		}
	}
	return e
}

// Exec method update db upto three times
func (c *Catalog) Exec(stm, status, rid string) error {
	count := 0
	_, err := DB.Exec(stm, status, rid)
	for err != nil && count < 3 {
		_, err = DB.Exec(stm, status, rid)
		count += 1
		time.Sleep(time.Second * 1)
	}
	return err
}

// UpdateRequest updates the status of request
func (c *Catalog) UpdateRequest(rid string, status string) error {
	stm := getSQL("update_request")
	err := c.Exec(stm, status, rid)
	return err
}

// RetrieveRequest gets the request details based on request id
func (c *Catalog) RetrieveRequest(r *TransferRequest) error {
	stm := getSQL("request_by_id")
	rows, err := DB.Query(stm, r.Id)
	if err != nil {
		r.Status = err.Error()
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(&r.Lfn, &r.Block, &r.Dataset, &r.SrcUrl, &r.SrcAlias, &r.DstUrl, &r.DstAlias, &r.RegUrl, &r.RegAlias, &r.Priority); err != nil {
			r.Status = err.Error()
			return err
		}
	}
	return nil
}

// GetStatus gets the status of request
func (c *Catalog) GetStatus(rid string) (string, error) {
	var status string
	stm := getSQL("get_status")
	rows, err := DB.Query(stm, rid)

	if err != nil {
		return "", err
	}
	for rows.Next() {
		if err := rows.Scan(&status); err != nil {
			return "", err
		}
	}

	defer rows.Close()
	return status, err
}

// ListRequest gets specific type of transfer requests according to status
func (c *Catalog) ListRequest(query string) ([]TransferRequest, error) {
	var (
		err  error
		rows *sql.Rows
	)
	switch query {
	case "pending":
		stm := getSQL("request_by_status") // Request is waiting for the approval
		rows, err = DB.Query(stm, query)
	case "finished":
		stm := getSQL("request_by_status") // Request is successfully transferred
		rows, err = DB.Query(stm, query)
	case "all":
		stm := getSQL("all_request") // Get all the requests
		rows, err = DB.Query(stm)
	case "deleted":
		stm := getSQL("request_by_status") // Request is deleted without transfer
		rows, err = DB.Query(stm, query)
	case "error":
		stm := getSQL("request_by_status") // Error occurred while transferring data
		rows, err = DB.Query(stm, query)
	case "processing":
		stm := getSQL("request_by_status") // Error occurred while transferring data
		rows, err = DB.Query(stm, query)
	default:
		return nil, errors.New("Requested request type could not find")
	}

	if err != nil {
		return nil, err
	}
	cols, err := rows.Columns()
	defer rows.Close()
	if err != nil {
		return nil, err
	}

	pointers := make([]interface{}, len(cols))
	con := make([]string, len(cols)) // A pointer to Columns of db
	var requests []TransferRequest

	for i := range pointers {
		pointers[i] = &con[i]
	}

	// Sqlite columns => 0:id 1:rid 2:file 3:block 4:dataset 5:srcurl 6:srcalias 7:dsturl 8:dstalias 9:regurl 10:regalias 11:status 12:priority
	for rows.Next() {
		rows.Scan(pointers...)
		priority, err := strconv.Atoi(con[12])
		if err != nil {
			return nil, err
		}
		r := TransferRequest{SrcUrl: con[5], SrcAlias: con[6], DstUrl: con[7], DstAlias: con[8], RegUrl: con[9], RegAlias: con[10], Lfn: con[2], Block: con[3], Dataset: con[4], Id: con[1], Priority: priority, Status: con[11]}
		requests = append(requests, r)
	}
	return requests, err
}

// InsertTransfers inserts new row to TRANSFERS table
func (c *Catalog) InsertTransfers(time int64, cpuUsage float64, memUsage float64, throughput float64) {
	stm := getSQL("insert_transfers")
	DB.Exec(stm, time, cpuUsage, memUsage, throughput)
}
