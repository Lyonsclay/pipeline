// Pipeline for processing tablular data.
// Features a paginated interface with customization.
// Basic unit of work is page. Named processes will operate on the level of Page.
// A Page has rows and the implementations of row processes will be defined in the
// specifics of a Stager definition.
package pipeline

import (
	"encoding/csv"
	"fmt"
	"github.com/pkg/profile"
	"log"
	"errors"
	"os"
	"sync"
)

type Range struct {
	Start int
	Stop  int
}

type Job struct {
	ConnString    string
	TotalRowsQuery string
	TableName     string
	IndexField    string
	RowType       Row

	TotalPages int
	MaxRows  int

	PageSize     int
	ProcessLimit int
	PageRange    Range
	Pages        []Page
	Pipeline     []Stager
}

type Stager interface {
	QueryPage(p *Page) error
	PaginateQuery(p Page) string
}

type JobRunner interface {
	// should return error
	Paginate(maxRows int) error
	Run() ([]Page, error)
	ProcessPage(p *Page, errc chan error) Page
}

type Packer interface {
	Unpack() []interface{}
	Pack([]interface{}) Packer
}

type Row Packer

type Page struct {
	Number     int
	TotalRows   int
	RowType    Row
	IndexField string
	Rows       []Row
	Errors     []error
}

func (j Job) ProcessPage(p *Page, errc chan error) Page {
	// Iterate through stages passing a Page.
	for _, s := range j.Pipeline {
		err := s.QueryPage(p)
		if err != nil {
			errc <- err
		}
	}
	return *p
}

func (j *Job) Paginate(maxRows int) error {
	if maxRows < 1 {
		err := errors.New("You must specify")
		log.Fatal(err)
		return err
	}
	j.MaxRows = maxRows
	if j.PageSize == 0 {
		j.PageSize = 10
	}
	if maxRows%j.PageSize > 0 {
		j.TotalPages = (maxRows / j.PageSize) + 1
	} else {
		j.TotalPages = maxRows / j.PageSize
	}
	if j.PageRange.Start < 1 {
		j.PageRange.Start = 1
	}
	if j.PageRange.Stop < 1 {
		j.PageRange.Stop = maxRows
	}
	j.TotalPages = (maxRows / j.PageSize) + 1
	var pages []Page
	for i := j.PageRange.Start; i <= j.PageRange.Stop; i++ {
		page := Page{}
		page.Number = i
		page.IndexField = j.IndexField
		page.TotalRows = j.PageSize
		page.RowType = j.RowType
		if i == j.TotalPages {
			page.TotalRows = j.MaxRows % j.PageSize
		}
		pages = append(pages, page)
	}
	j.Pages = pages

	return nil
}

// Consider returning a results object -- Stripping Rows of data
func (j Job) Run() ([]Page, error) {
	defer Proof().Stop()
	// Paginator closes the done channel when it returns; it may do so before
	// receiving all the values from c and errc.
	done := make(chan struct{})
	defer close(done)
	errc := make(chan error, 1)
	defer close(errc)
	pages := make(chan Page)
	go func() {
		defer close(pages)
		for _, page := range j.Pages {
			select {
			case pages <- page:
			case <-done:
				return
			}
		}
	}()

	c := make(chan Page)

	var wg sync.WaitGroup
	wg.Add(j.ProcessLimit)

	for i := 0; i < j.ProcessLimit; i++ {
		go func() {
			for page := range pages {
				select {
				case c <- j.ProcessPage(&page, errc):
				case <-done:
					return
				}
			}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(c)
	}()

	var results []Page

	for r := range c {
		results = append(results, r)
		log.Println("Compare :: ", r.Number)
	}
	errc <- nil
	if err := <-errc; err != nil {
		fmt.Println("Process errors :: ", err)
		return nil, err
	}

	return results, nil
}

func Proof() interface{ Stop() } {
	null, _ := os.Open(os.DevNull)
	log.SetOutput(null)
	defer null.Close()
	return profile.Start(profile.ProfilePath("./"))
}

func writetocsv(name string, header [][]string, rows [][]string) {
	rows = append(header, rows...)
	file, err := os.Create(name)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, value := range rows {
		err := writer.Write(value)
		if err != nil {
			log.Fatal(err)
		}
	}
}
