package core

// transfer2go implementation of Central Catalog
// Author: Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
)

// CC reprents isntance of CentralCatalog
var CC CentralCatalog

// CentralCatalog represent structure of Central Catalog
// it is represneted by list of tables where each table contains list of records
type CentralCatalog struct {
	Path string `json:"path"` // path to central catalog
}

func findLatestSnapshot(path, table string) string {
	var year, month, day, unix int
	files, _ := ioutil.ReadDir(path)
	for _, f := range files {
		v, _ := strconv.Atoi(f.Name())
		if v > year {
			year = v
		}
	}
	files, _ = ioutil.ReadDir(fmt.Sprintf("%s/%d", path, year))
	for _, f := range files {
		v, _ := strconv.Atoi(f.Name())
		if v > month {
			month = v
		}
	}
	files, _ = ioutil.ReadDir(fmt.Sprintf("%s/%d/%d", path, year, month))
	for _, f := range files {
		v, _ := strconv.Atoi(f.Name())
		if v > day {
			day = v
		}
	}
	files, _ = ioutil.ReadDir(fmt.Sprintf("%s/%d/%d/%d", path, year, month, day))
	for _, f := range files {
		v, _ := strconv.Atoi(f.Name())
		if v > unix {
			unix = v
		}
	}
	return fmt.Sprintf("%s/%d/%d/%d/%d/%s", path, year, month, day, unix, table)
}

// Get method gets records from Central Catalog for a given table
func (c *CentralCatalog) Get(table string) ([]byte, error) {
	fname := findLatestSnapshot(c.Path, table)
	data, err := ioutil.ReadFile(fname)
	if err != nil {
		log.WithFields(log.Fields{
			"File": fname,
			"Err":  err,
		}).Error("CentralCatalog unable to read table")
		return []byte{}, err
	}
	return data, nil
}

// Put method puts given table-records into Central Catalog
func (c *CentralCatalog) Put(table string, records []string) error {
	t := time.Now()
	path := fmt.Sprintf("%s/%d/%d/%d/%d", c.Path, t.Year(), t.Month(), t.Day(), t.Unix())
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		log.WithFields(log.Fields{
			"Dir": path,
			"Err": err,
		}).Error("CentralCatalog unable to create directory")
		return err
	}
	fname := fmt.Sprintf("%s/%s", path, table)
	file, err := os.Create(fname)
	defer file.Close()
	if err != nil {
		log.WithFields(log.Fields{
			"File": fname,
			"Err":  err,
		}).Error("CentralCatalog unable to create file")
		return err
	}
	for _, v := range records {
		_, err := file.WriteString(v + "\n")
		if err != nil {
			log.WithFields(log.Fields{
				"File":   fname,
				"Record": v,
				"Err":    err,
			}).Error("CentralCatalog unable to write record")
			return err
		}
	}
	return nil
}
