package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	warp "github.com/PierreZ/Warp10Exporter"
)

type Config struct {
	Endpoint string `json:"endpoint"`
	Token    string `json:"token"`
}

func main() {
	configuration := Config{}

	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Fatal(err)
	}

	// Config
	configFile, err := os.Open(dir + "/config.json")
	if err != nil {
		log.Fatal(err)
	}

	decoder := json.NewDecoder(configFile)
	err = decoder.Decode(&configuration)
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {

		if !strings.Contains(f.Name(), ".SIM") {
			continue
		}

		// get labels
		labelsStr := strings.Split(f.Name(), "-")
		labels := warp.Labels{
			"start": labelsStr[0],
			"id":    labelsStr[1],
		}

		// generating time
		year, _ := strconv.Atoi(labelsStr[2][0:4])
		month, _ := strconv.Atoi(labelsStr[2][4:6])
		day, _ := strconv.Atoi(labelsStr[2][6:8])

		hour, _ := strconv.Atoi(labelsStr[3][0:2])
		minute, _ := strconv.Atoi(labelsStr[3][2:4])
		second, _ := strconv.Atoi(labelsStr[3][4:6])

		currentTime := time.Date(year, time.Month(month), day, hour, minute, second, 0, time.UTC)

		file, err := os.Open(dir + "/" + f.Name())
		if err != nil {
			log.Fatal(err)
		}

		batch := warp.NewBatch()

		// Creating GTS
		fhra := warp.NewGTS("sisporto.fhra").WithLabels(labels)
		uc := warp.NewGTS("sisporto.uc").WithLabels(labels)
		fm := warp.NewGTS("sisporto.fm").WithLabels(labels)

		batch.Register(fhra)
		batch.Register(uc)
		batch.Register(fm)

		scanner := bufio.NewScanner(file)
		i := 0
		csvLines := ""
		for scanner.Scan() {

			if i < 2 {
				i++
				continue
			}
			line := strings.Replace(scanner.Text(), "\t", ",", -1)
			csvLines += line + "\n"

			i++
		}

		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}

		// Parsing CSV
		r := csv.NewReader(strings.NewReader(csvLines))

		records, err := r.Read()
		if err != nil {
			log.Fatal("error reading record:", err)
		}

		for {
			records, err = r.Read()

			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal("error reading record:", err)
			}

			t, err := strconv.ParseInt(string(records[0]), 10, 64)
			if err != nil {
				log.Fatal(err)
			}

			currentTime = currentTime.Add(time.Duration(t) * time.Millisecond)

			fhraValue, err := strconv.ParseInt(string(records[1]), 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			fhra.AddDatapoint(currentTime, fhraValue)

			ucValue, err := strconv.ParseInt(string(records[3]), 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			uc.AddDatapoint(currentTime, ucValue)

			fmValue, err := strconv.ParseInt(string(records[4]), 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			fm.AddDatapoint(currentTime, fmValue)
		}

		file.Close()

		err = batch.Push(configuration.Endpoint, configuration.Token)
		if err != nil {
			log.Fatal(err)
		}

		err = os.Remove(dir + "/" + f.Name())

		if err != nil {
			log.Fatalln(err)
		}
	}
}
