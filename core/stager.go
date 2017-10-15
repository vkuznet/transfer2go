package core

// transfer2go stager data transfer module
// Author - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"hash/adler32"
	"io"
	"os"
	"path/filepath"

	logs "github.com/sirupsen/logrus"
)

// AgentStager represent instance of agent's stager
var AgentStager *FileSystemStager

// Stager interface defines abstract functionality of the file stage system
type Stager interface {
	Stage(lfn string) error
	Read(lfn string, chunk int64) ([]byte, error)
	Write([]byte) (string, int64, string)
	Exist(lfn string) bool
	Access(lfn string) string
}

// FileSystemStager defines simple file-based stager
type FileSystemStager struct {
	Pool    string  // pool area on file system
	Catalog Catalog // TFC catalog of the agent
}

// Stage implements stage functionality of the Stager interface
// this function takes given lfn and place into internal pool area
func (s *FileSystemStager) Stage(lfn string) error {
	fmt.Println("STAGE", lfn)
	for _, pfn := range s.Catalog.PfnFiles("", "", lfn) {
		// for simplicity we'll create a soft link to a pfn from a catalog
		fname := fmt.Sprintf("%s/%s", s.Pool, filepath.Base(lfn))
		err := os.Symlink(pfn, fname)
		if err != nil {
			return err
		}
		logs.WithFields(logs.Fields{
			"Lfn":  lfn,
			"Pfn":  pfn,
			"Pool": s.Pool,
		}).Info("staged")
	}
	return nil
}

// Access provides access path to the file
func (s *FileSystemStager) Access(lfn string) string {
	return fmt.Sprintf("%s/%s", s.Pool, filepath.Base(lfn))
}

// Read implements read functionality of the Stager interface
// this function get file associated with lfn from the pool area and return its content
func (s *FileSystemStager) Read(lfn string, chunk int64) ([]byte, error) {
	fname := fmt.Sprintf("%s/%s", s.Pool, filepath.Base(lfn))
	if _, err := os.Stat(fname); !os.IsNotExist(err) {
		// Define go pipe
		//         pr, pw := io.Pipe()
		//         writer := multipart.NewWriter(pw)
		//         part, err := writer.CreateFormFile("data", fname)
		//         if err != nil {
		//             return nil, err
		//         }
		// Use copy of writer to avoid deadlock condition
		//         out := io.MultiWriter(part)
		//         _, err = io.Copy(out, file)
		//         if err != nil {
		//             return nil, err
		//         }
		return nil, nil
	}
	return nil, nil
}

// Exist implements exists functionality of the Stager interface
func (s *FileSystemStager) Exist(lfn string) bool {
	fname := fmt.Sprintf("%s/%s", s.Pool, filepath.Base(lfn))
	if _, err := os.Stat(fname); !os.IsNotExist(err) {
		return true
	}
	return false
}

// Put implements put functionality of the Stager interface
func (s *FileSystemStager) Write(data []byte, lfn string) (string, int64, string, error) {
	// create a file (pfn) in local pool
	pfn := fmt.Sprintf("%s/%s", s.Pool, filepath.Base(lfn))
	fin, err := os.Create(pfn)
	if err != nil {
		logs.WithFields(logs.Fields{
			"Error": err,
			"Pfn":   pfn,
		}).Error("Unable to create file in local pool", err)
		return "", 0, "", err
	}
	// create a hasher to calculate data hash
	hasher := adler32.New()
	// create our writer with give file descriptor
	w := bufio.NewWriter(fin)
	// create multi-writer (data->hasher->writer)
	mw := io.MultiWriter(hasher, w)
	// write data through multi-writer (hasher->writer)
	bytes, err := mw.Write(data)
	if err != nil {
		logs.WithFields(logs.Fields{
			"Error": err,
		}).Error("Unable to write data through hasher->writer", err)
		return "", 0, "", err
	}
	hash := hex.EncodeToString(hasher.Sum(nil))
	return pfn, int64(bytes), hash, nil
}
