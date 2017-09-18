package core

import (
	"fmt"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
)

// transfer2go stager data transfer module
// Author - Valentin Kuznetsov <vkuznet@gmail.com>

// Stager interface defines abstract functionality of the file stage system
type Stager interface {
	Stage(lfn string) error
	Read(lfn string, chunk int64) ([]byte, error)
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
		log.WithFields(log.Fields{
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
