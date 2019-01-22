package main

// Utility that takes in a dataexport dump file and emits a directory containing events grouped by session (invdid, sessionId) and sorted by time.

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/buger/jsonparser"
	"github.com/coreos/pkg/multierror"
	"github.com/golang/glog"
	pkgerrors "github.com/pkg/errors"
)

var (
	outDir = flag.String("out", ".", "The dir we will ensure exists, and emit the session-grouped events to")
)

func runBysession(ctx context.Context, args []string) error {
	if len(args) < 1 {
		errorstr := fmt.Sprintf("Bysession tool requires at least 1 argument (name of the folder containing exports)\n%s",
			"Usage: ./fs-explore bysession <exports folder>")
		return errors.New(errorstr)
	}

	inDir := args[0]
	builtinArgs := make([]string, 1)
	builtinArgs[0] = "-stderrthreshold=INFO" // Making logging verbose by default
	flag.CommandLine.Parse(builtinArgs)
	orgName := filepath.Base(inDir)

	orgDir := path.Join(*outDir, orgName+"-bysession")

	glog.Infof("Creating output dir [%s]", orgDir)
	defer glog.Flush()

	if _, err := os.Stat(orgDir); !os.IsNotExist(err) {
		glog.Errorf("Aborting. Output dir [%s] already exists (and export is non-idempotent)", orgDir)
		return nil
	}

	if err := os.MkdirAll(orgDir, 0750); err != nil {
		return pkgerrors.Wrapf(err, "Failed to create data directory "+orgDir)
	}

	glog.Infof("processing data export files in [%s]...", inDir)
	startTime := time.Now()
	defer glog.Infof("processing data export files in [%s] finished in %s", inDir, time.Since(startTime))

	// Process each dump file in parallel up to NumCPU workers.
	numWorkers := runtime.NumCPU()
	processFileCh := make(chan string, numWorkers)
	processor := newExportProcesser(orgDir)
	var wg sync.WaitGroup

	// Close the channel after walking the files. Block until all workers are done.
	defer func() {
		close(processFileCh)
		wg.Wait()
	}()

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			processor.processFiles(ctx, processFileCh)
		}()
	}

	// Walk the dump dir to collect and process export files.
	return filepath.Walk(inDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return pkgerrors.Wrap(err, "walk called with error")
		} else if info.IsDir() {
			return nil
		}

		glog.Infof("Processing [%s]", path)
		if strings.HasSuffix(path, ".json") {
			processFileCh <- path
		}
		return nil
	})
}

type exportProcessor struct {
	// Where we output session events to.
	orgDir string

	// When writing a session to disk, we need to make sure that concurrent file processors linearize around specific sessions for sessions spanning export files.
	writeLocks *TransientLockMap
}

func newExportProcesser(orgDir string) *exportProcessor {
	return &exportProcessor{
		orgDir:     orgDir,
		writeLocks: NewTransientLockMap(),
	}
}

// Processes a dump file. Builds an in-memory table of all events swimlaned by indvId:sessionId.
// Emits a file per session with each session containing the events sorted by EventStart.
func (p *exportProcessor) processFiles(ctx context.Context, processFileCh chan string) {
	defer glog.Flush()
	for path := range processFileCh {
		sessions := map[string]sessionEvents{}

		jsonBytes, err := ioutil.ReadFile(path)
		if err != nil {
			glog.Errorf("Couldn't read file [%s]: %s", path, pkgerrors.Wrap(err, ""))
			continue
		}

		var errs multierror.Error
		eachEvent := func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
			// Iterate each event.
			if err != nil {
				errs = append(errs, err)
				return
			}

			indvId, err := jsonparser.GetInt(value, "IndvId")
			if err != nil {
				glog.Errorf("%s", pkgerrors.Wrap(err, "couldn't get json int value for IndyId"))
				return
			}

			sessionId, err := jsonparser.GetInt(value, "SessionId")
			if err != nil {
				glog.Errorf("%s", pkgerrors.Wrap(err, "couldn't get json int value for SessionId"))
				return
			}

			sessionKey := fmt.Sprintf("%d:%d", indvId, sessionId)
			session := sessions[sessionKey]
			sessions[sessionKey] = append(session, value)
		}

		if _, err := jsonparser.ArrayEach(jsonBytes, eachEvent); err != nil {
			glog.Errorf("%s", pkgerrors.Wrap(err, "couldn't parse json bytes as array"))
			continue
		}

		doInLock := func(sessionKey string, f func()) {
			acquired := p.writeLocks.Lock(ctx, sessionKey)
			defer p.writeLocks.Unlock(sessionKey)
			if acquired {
				f()
			} else {
				glog.Errorf("failed to acquire lock for [%s]", sessionKey)
			}
		}

		// Write the session files to disk.
		for sessionKey, sessionEvents := range sessions {
			glog.Infof("Saving session: [%s]", sessionKey)

			sessionFileName := filepath.Join(p.orgDir, sessionKey) + ".json"

			doInLock(sessionKey, func() {
				_, err := os.Stat(sessionFileName)
				if err == nil {
					// Exists. Let's chain to it.
					// Read existing file. Merge it and resort with ours.
					existingSession, err := ioutil.ReadFile(sessionFileName)
					if err != nil {
						glog.Errorf("%s", err)
						return
					}

					jsonparser.ArrayEach(existingSession, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
						sessionEvents = append(sessionEvents, value)
					})

					sort.Sort(sessionEvents)
					if err := ioutil.WriteFile(sessionFileName, sessionEvents.AsJson(), 0750); err != nil {
						glog.Errorf("%s", err)
					}
				} else if os.IsNotExist(err) {
					// Create it.
					sort.Sort(sessionEvents)
					if err := ioutil.WriteFile(sessionFileName, sessionEvents.AsJson(), 0750); err != nil {
						glog.Errorf("%s", err)
					}
				} else {
					// Some other error.
					glog.Errorf("%s", err)
				}
			})
		}

		if len(errs) > 0 {
			glog.Errorf("%s", errs.AsError())
		}
	}
}

// Events in a single session. Where each event is a []byte corresponding to it's JSON encoding.
type sessionEvents [][]byte

func (s sessionEvents) AsJson() []byte {
	buf := &bytes.Buffer{}
	buf.WriteString("[")
	for i, e := range s {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.Write(e)
	}
	buf.WriteString("]")
	return buf.Bytes()
}

func (s sessionEvents) Len() int      { return len(s) }
func (s sessionEvents) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s sessionEvents) Less(i, j int) bool {
	si, err := jsonparser.GetString(s[i], "EventStart")
	if err != nil {
		panic(err)
	}
	sj, err := jsonparser.GetString(s[j], "EventStart")
	if err != nil {
		panic(err)
	}

	// Lexicographic string comparison ought to work.
	return strings.TrimSuffix(si, "Z") < strings.TrimSuffix(sj, "Z")
}
