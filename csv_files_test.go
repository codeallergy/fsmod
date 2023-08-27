/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package fsmod_test

import (
	"bytes"
	"fmt"
	"github.com/sprintframework/fsmod"
	"github.com/stretchr/testify/require"
	"github.com/sprintframework/fs"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestCsvWriteAndRead(t *testing.T) {

	fs := fsmod.FileService()

	fd, err := ioutil.TempFile(os.TempDir(), "csv-test")
	require.NoError(t, err)
	filePath := fd.Name()
	fd.Close()
	os.Remove(filePath)

	// Test Plain
	filePath = filePath + ".csv"
	writeCsv(t, fs, filePath)
	var buf bytes.Buffer
	writeCsvStream(t, fs.NewCsvStream(&buf, false, strings.TrimSpace, fsmod.PandasFriendly))

	content, err := ioutil.ReadFile(filePath)
	require.NoError(t, err)
	require.Equal(t, buf.Bytes(), content)
	require.Equal(t, "123,#,#,#,#\n", string(content))
	readCsv(t, filePath)
	stream, err := fs.OpenCsvStream(bytes.NewReader(content), false, strings.TrimSpace, fsmod.RemoveHash)
	readCsvStream(t, stream)

	// Test With Header
	writeCsvWithHeader(t, filePath)
	readCsvWithHeader(t, filePath)

	os.Remove(filePath)

	// Test GZIP
	filePath = filePath + ".gz"
	writeCsv(t, fs, filePath)
	readCsv(t, filePath)
	os.Remove(filePath)

}

func readCsv(t *testing.T, filePath string) {

	fs := fsmod.FileService()

	reader, err := fs.OpenCsvFile(filePath, strings.TrimSpace, fsmod.RemoveHash)
	require.NoError(t, err)

	record, err := reader.Read()
	require.NoError(t, err)

	require.Equal(t, "123,,,,", strings.Join(record, ","))

	_, err = reader.Read()
	require.Equal(t, err, io.EOF)

	err = reader.Close()
	require.NoError(t, err)
}

func readCsvStream(t *testing.T, reader fs.CsvStream) {

	record, err := reader.Read()
	require.NoError(t, err)

	require.Equal(t, "123,,,,", strings.Join(record, ","))

	_, err = reader.Read()
	require.Equal(t, err, io.EOF)

	err = reader.Close()
	require.NoError(t, err)
}

func readCsvWithHeader(t *testing.T, filePath string) {

	fs := fsmod.FileService()

	reader, err := fs.OpenCsvFile(filePath, strings.TrimSpace, fsmod.RemoveHash)
	require.NoError(t, err)

	file, err := reader.ReadHeader()
	require.NoError(t, err)
	require.Equal(t, "name,value", strings.Join(file.Header(), ","))
	require.Equal(t, 0, file.Index()["name"])
	require.Equal(t, 1, file.Index()["value"])

	schema := fs.NewCsvSchema(file.Header())

	record, err := file.Next()
	require.NoError(t, err)

	require.Equal(t, "one,1", strings.Join(record.Record(), ","))
	require.Equal(t, "one", record.Field("name", ""))
	require.Equal(t, "1", record.Field("value", ""))

	fields := record.Fields()
	require.Equal(t, 2, len(fields))
	require.Equal(t, "one", fields["name"])
	require.Equal(t, "1", fields["value"])

	require.Equal(t, schema.Record(record.Record()).Fields(), fields)

	_, err = file.Next()
	require.Equal(t, err, io.EOF)

	err = reader.Close()
	require.NoError(t, err)
}


func writeCsv(t *testing.T, fs fs.FileService, filePath string) {

	csv, err := fs.NewCsvFile(filePath, strings.TrimSpace, fsmod.PandasFriendly)
	require.NoError(t, err)

	writeCsvStream(t, csv)
}

func writeCsvStream(t *testing.T, csv fs.CsvWriter) {

	err := csv.Write(" 123 ", "", " ", "null", "NaN")
	require.NoError(t, err)

	err = csv.Close()
	require.NoError(t, err)
}

func writeCsvWithHeader(t *testing.T, filePath string) {

	fs := fsmod.FileService()

	csv, err := fs.NewCsvFile(filePath, strings.TrimSpace, fsmod.PandasFriendly)
	require.NoError(t, err)

	err = csv.Write("name ", " value ")
	require.NoError(t, err)

	err = csv.Write(" one ", " 1 ")
	require.NoError(t, err)

	err = csv.Close()
	require.NoError(t, err)
}

func TestCsvSplit(t *testing.T) {

	fs := fsmod.FileService()

	fd, err := ioutil.TempFile(os.TempDir(), "csv-test")
	require.NoError(t, err)
	filePath := fd.Name()
	fd.Close()
	os.Remove(filePath)

	csvfilePath := filePath + ".csv"

	csv, err := fs.NewCsvFile(csvfilePath)
	require.NoError(t, err)

	err = csv.Write("name", "count")
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		err = csv.Write(fmt.Sprintf("name%d", i), strconv.Itoa(i))
		require.NoError(t, err)
	}

	err = csv.Close()
	require.NoError(t, err)

	parts, err := fs.SplitCsvFile(csvfilePath, 10, func(i int) string {
		return fmt.Sprintf("%s_part%d.csv", filePath, i)
	})
	require.NoError(t, err)

	println(csvfilePath)
	all, err := ioutil.ReadFile(csvfilePath)
	require.NoError(t, err)
	//println(string(all))

	err = fs.JoinCsvFiles(csvfilePath, parts)
	require.NoError(t, err)

	joined, err := ioutil.ReadFile(csvfilePath)
	require.NoError(t, err)

	require.Equal(t, all, joined)

	os.Remove(csvfilePath)
	for _, part := range parts {
		println("RemoveFile: ", part)
		os.Remove(part)
	}
}
