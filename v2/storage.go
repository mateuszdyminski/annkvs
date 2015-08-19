package v2

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

const (
	// SegmentSize describes how many kilobytes should have segment file.
	SegmentSize = 8 * 1024

	// SegmentNameLength number of chars in segment name.
	SegmentNameLength = 10

	// MaxPtr is max possible pointer value.
	MaxPtr = ^(uintptr(0))
)

type storage struct {
	storagePath string
	segments    []segment
	quit        chan struct{}
}

type segment struct {
	fileName   string
	filePath   string
	file       *os.File
	writeIndex int64
	index      map[string]value
}

type value struct {
	offset int64
	size   int
}

func newStorage(storagePath string) *storage {
	// create specified directory if not exists
	if _, err := os.Stat(storagePath); os.IsNotExist(err) {
		err = os.Mkdir(storagePath, 0777)
		if err != nil {
			panic(err)
		}
	}

	// read all files in directory
	allFiles, err := ioutil.ReadDir(storagePath)
	if err != nil {
		panic(err)
	}

	// create needed structs
	storage := &storage{segments: make([]segment, 0), storagePath: storagePath}

	files := allFiles[:0]
	for _, file := range allFiles {
		if !file.IsDir() && strings.HasPrefix(file.Name(), "tmp") {
			filePath := path.Join(storage.storagePath, file.Name())
			log.Printf("Deleting tmp file: %s \n", filePath)
			if err = os.Remove(filePath); err != nil {
				panic(err)
			}
		} else {
			files = append(files, file)
		}
	}

	// create new file if no files in directory
	if len(files) == 0 {
		log.Printf("Storage directory empty. \n")

		err = storage.createNewSegment("current")
		if err != nil {
			panic(err)
		}
	} else {
		for i := 0; i < len(files); i++ {
			segment := segment{filePath: path.Join(storagePath, files[i].Name()), index: make(map[string]value)}
			// open only last file
			if i == len(files)-1 {
				segment.file, err = os.OpenFile(segment.filePath, os.O_APPEND|os.O_RDWR, 0666)
				if err != nil {
					panic(err)
				}

				// and set proper index for writes
				stat, err := segment.file.Stat()
				if err != nil {
					panic(err)
				}

				segment.writeIndex = stat.Size()
			}

			err = segment.createIndex()
			if err != nil {
				panic(err)
			}

			// add segment to segments list
			storage.segments = append(storage.segments, segment)
		}
	}

	storage.startCompaction()

	return storage
}

func (s *storage) writeSegmentIndex() int {
	if len(s.segments) == 0 {
		return 0
	}

	return len(s.segments) - 1
}

func (s *storage) insert(key, val string) {
	if s.segments[s.writeSegmentIndex()].writeIndex > SegmentSize {
		err := s.segments[s.writeSegmentIndex()].file.Close()
		if err != nil {
			panic(err)
		}

		newName := path.Join(s.storagePath, s.nextSegmentName())
		if err = os.Rename(s.segments[s.writeSegmentIndex()].filePath, newName); err != nil {
			panic(err)
		}

		s.segments[s.writeSegmentIndex()].filePath = newName

		err = s.createNewSegment("current")
		if err != nil {
			panic(err)
		}
	}

	s.segments[s.writeSegmentIndex()].insert(key, val)
}

func (s *storage) get(key string) (string, bool) {
	// try to get value from indexes
	for i := len(s.segments) - 1; i >= 0; i-- {
		if val, ok := s.segments[i].getFromIndex(key); ok {
			return val, true
		}
	}

	return "", false
}

func (s *segment) getFromIndex(key string) (string, bool) {
	desc, ok := s.index[key]
	if ok {
		value := make([]byte, desc.size)
		var err error
		if s.file == nil || s.file.Fd() == MaxPtr {
			s.file, err = os.OpenFile(s.filePath, os.O_RDONLY, 0666)
			if err != nil {
				panic(err)
			}

			defer s.file.Close()
		}

		_, err = s.file.ReadAt(value, desc.offset)
		if err != nil {
			panic(err)
		}

		return strings.Split(string(value), ",")[1], true
	}

	return "", false
}

func (s *segment) createIndex() error {
	log.Printf("Creating index for segment file %s. \n", s.filePath)

	// check if file is nil or closed and open file if needed
	if s.file == nil || s.file.Fd() == MaxPtr {
		var err error
		s.file, err = os.OpenFile(s.filePath, os.O_RDONLY, 0666)
		if err != nil {
			panic(err)
		}
		defer s.file.Close()
	}

	s.file.Seek(0, 0)
	scanner := bufio.NewScanner(s.file)
	offset := 0
	for scanner.Scan() {
		line := scanner.Text()
		size := len(line) + 1
		kv := strings.Split(line, ",")
		s.index[kv[0]] = value{offset: int64(offset), size: size}
		offset += size
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

func (s *segment) insert(key, val string) {
	n, err := s.file.WriteString(fmt.Sprintf("%s,%s\n", key, val))
	if err != nil {
		panic(err)
	}

	s.index[key] = value{offset: s.writeIndex, size: n}
	s.writeIndex += int64(n)
}

func (s *storage) startCompaction() {
	ticker := time.NewTicker(3 * time.Second)
	s.quit = make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				log.Println("Compaction and Meging started!")
				d := time.Now()
				if err := s.compactAndMerge(); err != nil {
					log.Printf("Error during compaction merging phase. Err: %+v \n", err)
				}
				log.Printf("Compaction finished in %v \n", time.Since(d))
			case <-s.quit:
				ticker.Stop()
				return
			}
		}
	}()
}

func (s *storage) compactAndMerge() error {
	// start compaction only when number of segments is bigger than 1
	if len(s.segments) <= 1 {
		return nil
	}

	// placeholder for all new segments
	segments := make([]segment, 0)

	// create new file for compaction
	createSegment := func(segments []segment) *segment {
		var err error
		fileName := fmt.Sprintf("%s%s", "tmp", segmentName(len(segments)+1))
		segment := &segment{fileName: fileName, filePath: path.Join(s.storagePath, fileName), index: make(map[string]value)}
		segment.file, err = os.OpenFile(segment.filePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
		if err != nil {
			panic(err)
		}

		return segment
	}

	segment := createSegment(segments)

	compacted := make(map[string]bool)

	segmentsLength := len(s.segments)

	// go from oldest file and try to compact all files. ommit current file
	for i := 0; i < segmentsLength-1; i++ {

		log.Printf("Compacting segment: %v \n", s.segments[i].filePath)

		// check all keys in segments starting from oldest one
		for k := range s.segments[i].index {
			// start from latest segment
			for j := segmentsLength - 2; j >= 0; j-- {
				// if key has been already compacted - go to the next one
				_, done := compacted[k]
				if done {
					break
				}

				// try to get value
				entry, ok := s.segments[j].getFromIndex(k)
				if ok {
					if segment.writeIndex > SegmentSize {
						segment.file.Close()
						segments = append(segments, *segment)
						segment = createSegment(segments)
					}

					// trim last byte which indicates EOL
					segment.insert(k, entry[:len(entry)-1])
					compacted[k] = true
				}
			}
		}
	}
	// close and insert last segment
	segment.file.Close()
	segments = append(segments, *segment)

	// copy segments which could be removed
	toRemove := s.segments[:(segmentsLength - 1)]

	// append new segments at the beginning of the origin segments
	s.segments = append(segments, s.segments[(segmentsLength-1):]...)

	for _, seg := range toRemove {
		if err := os.Remove(seg.filePath); err != nil {
			panic(err)
		}
	}

	for i := 0; i < len(segments); i++ {
		if err := os.Rename(s.segments[i].filePath, path.Join(s.storagePath, s.segments[i].fileName[3:])); err != nil {
			panic(err)
		}

		s.segments[i].fileName = s.segments[i].fileName[3:]
		s.segments[i].filePath = path.Join(s.storagePath, s.segments[i].fileName)
	}

	return nil
}

func (s *storage) nextSegmentName() string {
	return segmentName(len(s.segments))
}

func (s *storage) createNewSegment(name string) error {
	var err error
	segment := segment{fileName: name, filePath: path.Join(s.storagePath, name), index: make(map[string]value)}
	segment.file, err = os.OpenFile(segment.filePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	s.segments = append(s.segments, segment)

	return nil
}

func segmentName(no int) string {
	return strings.Repeat("0", SegmentNameLength-len(strconv.Itoa(no))) + strconv.Itoa(no)
}
