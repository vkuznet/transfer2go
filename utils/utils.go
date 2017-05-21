package utils

// transfer2go/utils - Go utilities for transfer2go
//
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"hash/adler32"
	"io/ioutil"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"text/template"

	log "github.com/sirupsen/logrus"
)

// STATICDIR defines location of all static files
var STATICDIR string

// ListFiles function list files in given directory
func ListFiles(dir string) []string {
	var out []string
	entries, err := ioutil.ReadDir(dir)
	if err != nil {
		log.WithFields(log.Fields{
			"Directory": dir,
			"Error":     err,
		}).Println("Unable to read directory")
		return nil
	}
	for _, f := range entries {
		if !f.IsDir() {
			out = append(out, f.Name())
		}
	}
	return out
}

// consume list of templates and release their full path counterparts
func fileNames(tdir string, filenames ...string) []string {
	flist := []string{}
	for _, fname := range filenames {
		flist = append(flist, filepath.Join(tdir, fname))
	}
	return flist
}

// ParseTmpl is a template parser with given data
func ParseTmpl(tdir, tmpl string, data interface{}) string {
	buf := new(bytes.Buffer)
	filenames := fileNames(tdir, tmpl)
	t := template.Must(template.ParseFiles(filenames...))
	err := t.Execute(buf, data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}

// Hash implements hash function for data, it returns a hash and number of bytes
func Hash(data []byte) (string, int64) {
	hasher := adler32.New()
	b, e := hasher.Write(data)
	if e != nil {
		log.WithFields(log.Fields{
			"Error": e,
		}).Error("Unable to write chunk of data via hasher.Write", e)
	}
	return hex.EncodeToString(hasher.Sum(nil)), int64(b)
}

// Stack helper function to return Stack
func Stack() string {
	trace := make([]byte, 2048)
	count := runtime.Stack(trace, false)
	return fmt.Sprintf("\nStack of %d bytes: %s\n", count, trace)
}

// ErrPropagate error helper function which can be used in defer ErrPropagate()
func ErrPropagate(api string) {
	if err := recover(); err != nil {
		log.WithFields(log.Fields{
			"API":   api,
			"Error": err,
			"Stack": Stack(),
		}).Error("DAS Fault")
		panic(fmt.Sprintf("%s:%s", api, err))
	}
}

// ErrPropagate2Channel error helper function which can be used in goroutines as
// ch := make(chan interface{})
// go func() {
//    defer ErrPropagate2Channel(api, ch)
//    someFunction()
// }()
func ErrPropagate2Channel(api string, ch chan interface{}) {
	if err := recover(); err != nil {
		log.WithFields(log.Fields{
			"API":   api,
			"Error": err,
			"Stack": Stack(),
		}).Println("DAS Fault")
		ch <- fmt.Sprintf("%s:%s", api, err)
	}
}

// GoDeferFunc helper function to run any given function in defered go routine
func GoDeferFunc(api string, f func()) {
	ch := make(chan interface{})
	go func() {
		defer ErrPropagate2Channel(api, ch)
		f()
		ch <- "ok" // send to channel that we can read it later in case of success of f()
	}()
	err := <-ch
	if err != nil && err != "ok" {
		panic(err)
	}
}

// FindInList helper function to find item in a list
func FindInList(a string, arr []string) bool {
	for _, e := range arr {
		if e == a {
			return true
		}
	}
	return false
}

// InList helper function to check item in a list
func InList(a string, list []string) bool {
	check := 0
	for _, b := range list {
		if b == a {
			check += 1
		}
	}
	if check != 0 {
		return true
	}
	return false
}

// MapKeys helper function to return keys from a map
func MapKeys(rec map[string]interface{}) []string {
	keys := make([]string, 0, len(rec))
	for k := range rec {
		keys = append(keys, k)
	}
	return keys
}

// List2Set helper function to convert input list into set
func List2Set(arr []string) []string {
	var out []string
	for _, key := range arr {
		if !InList(key, out) {
			out = append(out, key)
		}
	}
	return out
}

// HostIP provides a list of host IPs
func HostIP() []string {
	var out []string
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.WithFields(log.Fields{
			"Error": err,
		}).Error("Unable to resolve net.InterfaceAddrs")
	}
	for _, addr := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				out = append(out, ipnet.IP.String())
			}
			if ipnet.IP.To16() != nil {
				out = append(out, ipnet.IP.String())
			}
		}
	}
	return List2Set(out)
}

// CheckX509 function checks X509 settings
func CheckX509() {
	uproxy := os.Getenv("X509_USER_PROXY")
	uckey := os.Getenv("X509_USER_KEY")
	ucert := os.Getenv("X509_USER_CERT")
	var check int
	if uproxy == "" {
		// check if /tmp/x509up_u$UID exists
		u, err := user.Current()
		if err == nil {
			fname := fmt.Sprintf("/tmp/x509up_u%s", u.Uid)
			if _, err := os.Stat(fname); err != nil {
				check += 1
			}
		}
	}
	if uckey == "" && ucert == "" {
		check += 1
	}
	if check > 1 {
		msg := fmt.Sprintf("Neither X509_USER_PROXY or X509_USER_KEY/X509_USER_CERT are set. ")
		msg += "In order to run please obtain valid proxy via \"voms-proxy-init -voms cms -rfc\""
		msg += "and setup X509_USER_PROXY or setup X509_USER_KEY/X509_USER_CERT in your environment"
		log.Println(msg)
		os.Exit(-1)
	}
}
