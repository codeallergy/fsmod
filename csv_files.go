/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package fsi

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"github.com/codeallergy/fs"
	"github.com/pkg/errors"
	"io"
	"os"
	"strings"
)

type csvStreamWriter struct {
	fw   io.Writer
	gzw   *gzip.Writer
	csvw  *csv.Writer
	valueProcessors []fs.CsvValueProcessor
}

func (t *fileServiceImpl) NewCsvStream(fw io.Writer, withGzip bool, valueProcessors ...fs.CsvValueProcessor) fs.CsvWriter {

	w := &csvStreamWriter{
		fw:              fw,
		valueProcessors: valueProcessors,
	}

	if withGzip {
		w.gzw = gzip.NewWriter(w.fw)
		w.csvw = csv.NewWriter(w.gzw)
	} else {
		w.csvw = csv.NewWriter(w.fw)
	}

	return w
}

func (w *csvStreamWriter) Close() (err error) {
	w.csvw.Flush()
	if w.gzw != nil {
		w.gzw.Flush()
		err = w.gzw.Close()
	}
	return err
}

func (w *csvStreamWriter) Write(values ...string) error {
	if w.valueProcessors != nil {
		return w.csvw.Write(zipValues(w.valueProcessors, values))
	} else {
		return w.csvw.Write(values)
	}
}

type csvFileWriter struct {
	fd   *os.File
	fw   *bufio.Writer
	gzw   *gzip.Writer
	csvw  *csv.Writer
	valueProcessors []fs.CsvValueProcessor
}

func (t *fileServiceImpl) NewCsvFile(filePath string, valueProcessors ...fs.CsvValueProcessor) (fs.CsvWriter, error) {

	var err error
	w := new(csvFileWriter)
	w.valueProcessors = valueProcessors

	w.fd, err = os.Create(filePath)
	if err != nil {
		return nil, errors.Errorf("file create error '%s', %v", filePath, err)
	}

	w.fw = bufio.NewWriterSize(w.fd, t.bufferSize)

	if strings.HasSuffix(filePath, ".gz") {
		w.gzw = gzip.NewWriter(w.fw)
		w.csvw = csv.NewWriter(w.gzw)
	} else {
		w.csvw = csv.NewWriter(w.fw)
	}

	return w, nil
}

func (w *csvFileWriter) Close() error {
	w.csvw.Flush()
	if w.gzw != nil {
		w.gzw.Flush()
		w.gzw.Close()
	}
	w.fw.Flush()
	return w.fd.Close()
}

func (w *csvFileWriter) Write(values ...string) error {
	if w.valueProcessors != nil {
		return w.csvw.Write(zipValues(w.valueProcessors, values))
	} else {
		return w.csvw.Write(values)
	}
}

func zipValues(processors []fs.CsvValueProcessor, list []string) []string {
	arr := make([]string, 0, len(list))
	for _, v := range list {
		for _, p := range processors {
			v = p(v)
		}
		arr = append(arr, v)
	}
	return arr
}

type csvStreamReader struct {
	fr   io.Reader
	gzr   *gzip.Reader
	csvr  *csv.Reader
	valueProcessors []fs.CsvValueProcessor
}

func (t *fileServiceImpl) OpenCsvStream(fr io.Reader, withGzip bool, valueProcessors ...fs.CsvValueProcessor) (fs.CsvStream, error) {

	var err error
	r := &csvStreamReader{
		fr: fr,
		valueProcessors: valueProcessors,
	}

	if withGzip {
		r.gzr, err = gzip.NewReader(r.fr)
		if err != nil {
			return nil, errors.Errorf("gzip read error, %v", err)
		}
		r.csvr = csv.NewReader(r.gzr)
	} else {
		r.csvr = csv.NewReader(r.fr)
	}

	return r, nil

}

func (r *csvStreamReader) Close() (err error) {
	if r.gzr != nil {
		err = r.gzr.Close()
	}
	return err
}

func (r *csvStreamReader) Read() ([]string, error) {
	record, err := r.csvr.Read()
	if err != nil {
		return nil, err
	}
	if r.valueProcessors != nil {
		record = zipValues(r.valueProcessors, record)
	}
	return record, nil
}

type csvFileReader struct {
	fd   *os.File
	fr   *bufio.Reader
	gzr   *gzip.Reader
	csvr  *csv.Reader
	valueProcessors []fs.CsvValueProcessor
}

func (t *fileServiceImpl) OpenCsvFile(filePath string, valueProcessors ...fs.CsvValueProcessor) (fs.CsvReader, error) {

	fd, err := os.Open(filePath)
	if err != nil {
		return nil, errors.Errorf("file open error '%s', %v", filePath, err)
	}

	return t.CsvFileReader(fd, valueProcessors...)
}

func (t *fileServiceImpl) CsvFileReader(fd *os.File, valueProcessors ...fs.CsvValueProcessor) (fs.CsvReader, error) {

	var err error
	r := &csvFileReader{
		fd: fd,
		valueProcessors: valueProcessors,
	}

	r.fr = bufio.NewReaderSize(r.fd, t.bufferSize)

	if strings.HasSuffix(fd.Name(), ".gz") {
		r.gzr, err = gzip.NewReader(r.fr)
		if err != nil {
			return nil, errors.Errorf("gzip read error in '%s', %v", fd.Name(), err)
		}
		r.csvr = csv.NewReader(r.gzr)
	} else {
		r.csvr = csv.NewReader(r.fr)
	}

	return r, nil

}

func (r *csvFileReader) Close() error {
	if r.gzr != nil {
		r.gzr.Close()
	}
	return r.fd.Close()
}

func (r *csvFileReader) ReadHeader() (fs.CsvFile, error) {
	header, err := r.Read()
	if err != nil {
		return nil, err
	}
	return newCsvFile(header, r), nil
}

func (r *csvFileReader) Read() ([]string, error) {
	record, err := r.csvr.Read()
	if err != nil {
		return nil, err
	}
	if r.valueProcessors != nil {
		record = zipValues(r.valueProcessors, record)
	}
	return record, nil
}

type csvFile struct {
	header []string
	index  map[string]int
	reader fs.CsvReader
}

func newCsvFile(header []string, reader fs.CsvReader) *csvFile {

	index := make(map[string]int)
	for i, name := range header {
		index[name] = i
	}

	return &csvFile {
		header: header,
		index: index,
		reader: reader,
	}
}

func (r *csvFile) Header() []string {
	return r.header
}

func (r *csvFile) Index() map[string]int {
	return r.index
}

func (r *csvFile) Next() (fs.CsvRecord, error) {
	record, err := r.reader.Read()
	if err != nil {
		return nil, err
	}
	return &csvRecord{record, r}, nil
}

type csvRecord struct {
	record []string
	file   *csvFile
}

func (r *csvRecord) Record() []string {
	return r.record
}

func (r *csvRecord) Field(name, def string) string {
	if idx, ok := r.file.index[name]; ok {
		if idx >= 0 && idx < len(r.record) {
			return r.record[idx]
		}
	}
	return def
}

func (r *csvRecord) Fields() map[string]string {
	m := make(map[string]string)
	for i, val := range r.record {
		name := ""
		if i < len(r.file.header) {
			name = r.file.header[i]
		}
		m[name] = val
	}
	return m
}

type csvSchema struct {
	header []string
	index  map[string]int
}

func (t *fileServiceImpl) NewCsvSchema(header []string) fs.CsvSchema {

	index := make(map[string]int)
	for i, name := range header {
		index[name] = i
	}

	return &csvSchema {
		header: header,
		index: index,
	}
}

func (s *csvSchema) Record(record []string) fs.CsvRecord {
	return &csvSchemaRecord {
		record,
		s,
	}
}

type csvSchemaRecord struct {
	record []string
	schema   *csvSchema
}

func (r *csvSchemaRecord) Record() []string {
	return r.record
}

func (r *csvSchemaRecord) Field(name, def string) string {
	if idx, ok := r.schema.index[name]; ok {
		if idx >= 0 && idx < len(r.record) {
			return r.record[idx]
		}
	}
	return def
}

func (r *csvSchemaRecord) Fields() map[string]string {
	m := make(map[string]string)
	for i, val := range r.record {
		name := ""
		if i < len(r.schema.header) {
			name = r.schema.header[i]
		}
		m[name] = val
	}
	return m
}

func (t *fileServiceImpl) SplitCsvFile(inputFilePath string, limit int, partFn func (int) string) ([]string, error) {

	reader, err := t.OpenCsvFile(inputFilePath)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	header, err := reader.Read()
	if err != nil {
		return nil, err
	}

	var parts []string
	var writer fs.CsvWriter

	partNum := 1
	for cnt := limit; err == nil; cnt++ {

		row, err := reader.Read()
		if err != nil {
			break
		}

		if cnt == limit {
			if writer != nil {
				writer.Close()
				writer = nil
			}
			partFilePath := partFn(partNum)
			writer, err = t.NewCsvFile(partFilePath)
			if err != nil {
				break
			}
			parts = append(parts, partFilePath)
			err = writer.Write(header...)
			if err != nil {
				break
			}
			cnt = 0
			partNum++
		}

		err = writer.Write(row...)
	}

	if err == io.EOF {
		err = nil
	}

	if writer != nil {
		writer.Close()
	}

	if err != nil {
		for _, part := range parts {
			os.Remove(part)
		}
		parts = nil
	}

	return parts, err
}

func (t *fileServiceImpl) JoinCsvFiles(outputFilePath string, parts []string) error {

	writer, err := t.NewCsvFile(outputFilePath)
	if err != nil {
		return err
	}
	defer writer.Close()

	for i, part := range parts {

		reader, err := t.OpenCsvFile(part)
		if err != nil {
			return errors.Errorf("can not open file '%s', %v", part, err)
		}

		header, err := reader.Read()
		if err != nil {
			reader.Close()
			return errors.Errorf("can not read header in file '%s', %v", part, err)
		}

		if i == 0 {
			err = writer.Write(header...)
			if err != nil {
				reader.Close()
				return errors.Errorf("can not write header to file '%s', %v", outputFilePath, err)
			}
		}

		for {

			row, err := reader.Read()
			if err != nil {
				break
			}

			err = writer.Write(row...)
			if err != nil {
				reader.Close()
				return errors.Errorf("can not write row to file '%s', %v", outputFilePath, err)
			}

		}

		if err == io.EOF {
			err = nil
		}

		reader.Close()

		if err != nil {
			return errors.Errorf("join read file '%s', %v", part, err)
		}

	}

	return nil
}
