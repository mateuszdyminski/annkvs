package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/mateuszdyminski/annkvs/v2"
)

func main() {
	db := v2.NewKv("/tmp/test")

	log.Printf("Key-Value store started! PID: %d \n", os.Getpid())

	start := time.Now()
	db.Open()
	fmt.Printf("Db open and indexed! Time: %v \n", time.Since(start))

	start = time.Now()
	for i := 1; i < 100000; i++ {
		db.Insert(strconv.Itoa(i), "test"+strconv.Itoa(i))
	}
	for i := 1; i < 100000; i++ {
		db.Insert(strconv.Itoa(i), "test"+strconv.Itoa(i))
	}
	for i := 1; i < 100000; i++ {
		db.Insert(strconv.Itoa(i), "test"+strconv.Itoa(i))
	}
	for i := 1; i < 100000; i++ {
		db.Insert(strconv.Itoa(i), "test"+strconv.Itoa(i))
	}
	fmt.Printf("Data inserted! Time: %v \n", time.Since(start))

	time.Sleep(4 * time.Second)

	key := "99999"
	start = time.Now()
	val, ok := db.Get(key)

	fmt.Printf("Found %v, Key: %v, Value %v, Time: %v \n", ok, key, val, time.Since(start))

	fmt.Printf("Done \n")

	db.Close()
}
