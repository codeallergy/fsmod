/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package fsi

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"github.com/codeallergy/fs"
	"github.com/pkg/errors"
	"io"
	"os"
	"strings"
)

type jsonStreamWriter struct {
	fs    *fileServiceImpl
	fd    io.Writer
	fw    *bufio.Writer
	gzw   *gzip.Writer
	bw    *bufio.Writer
	w     io.Writer
}

func (t *fileServiceImpl) NewJsonStream(fd io.Writer, withGzip bool) fs.JsonWriter {

	w := &jsonStreamWriter{
		fs:              t,
		fd:              fd,
	}

	w.fw = bufio.NewWriterSize(w.fd, t.bufferSize)

	if withGzip {
		w.gzw = gzip.NewWriter(w.fw)
		w.bw = bufio.NewWriterSize(w.gzw, t.bufferSize)
		w.w = w.bw
	} else {
		w.w = w.fw
	}

	return w
}

func (w *jsonStreamWriter) Close() (err error) {
	if w.bw != nil {
		w.bw.Flush()
	}
	if w.gzw != nil {
		w.gzw.Flush()
		err = w.gzw.Close()
	}
	w.fw.Flush()
	return err
}

func (w *jsonStreamWriter) WriteRaw(message json.RawMessage) error {
	_, err := w.w.Write(append(message, '\n'))
	return err
}

func (w *jsonStreamWriter) Write(object interface{}) error {
	return jsonWrite(w.w, w.fs, object)
}

type jsonFileWriter struct {
	fs    *fileServiceImpl
	fd    *os.File
	fw    *bufio.Writer
	gzw   *gzip.Writer
	bw    *bufio.Writer
	w     io.Writer
}

func (t *fileServiceImpl) NewJsonFile(filePath string) (fs.JsonWriter, error) {

	var err error
	w := &jsonFileWriter {
		fs: t,
	}

	w.fd, err = os.Create(filePath)
	if err != nil {
		return nil, errors.Errorf("file create error '%s', %v", filePath, err)
	}

	w.fw = bufio.NewWriterSize(w.fd, t.bufferSize)

	if strings.HasSuffix(filePath, ".gz") {
		w.gzw = gzip.NewWriter(w.fw)
		w.bw = bufio.NewWriterSize(w.gzw, t.bufferSize)
		w.w = w.bw
	} else {
		w.w = w.fw
	}

	return w, nil
}

func (w *jsonFileWriter) Close() error {
	if w.bw != nil {
		w.bw.Flush()
	}
	if w.gzw != nil {
		w.gzw.Flush()
		w.gzw.Close()
	}
	w.fw.Flush()
	return w.fd.Close()
}

func (w *jsonFileWriter) WriteRaw(message json.RawMessage) error {
	_, err := w.w.Write(append(message, '\n'))
	return err
}

func (w *jsonFileWriter) Write(object interface{}) error {
	return jsonWrite(w.w, w.fs, object)
}

func jsonWrite(w io.Writer, fs *fileServiceImpl, object interface{}) error {

	var jsonBin []byte
	jsonBin, err := fs.marshaler.Marshal(object)
	if err != nil {
		return err
	}

	_, err = w.Write(append(jsonBin, '\n'))
	return err
}

type jsonStreamReader struct {
	fs    *fileServiceImpl
	fr    io.Reader
	gzr   *gzip.Reader
	r     *bufio.Reader
	lastErr error
}

func (t *fileServiceImpl) JsonStream(fr io.Reader, withGzip bool) (fs.JsonReader, error) {

	var err error
	r := &jsonStreamReader{
		fs: t,
		fr: fr,
	}

	if withGzip {
		r.gzr, err = gzip.NewReader(r.fr)
		if err != nil {
			return nil, errors.Errorf("gzip read error, %v", err)
		}
		r.r = bufio.NewReader(r.gzr)
	} else {
		r.r = bufio.NewReader(r.fr)
	}

	return r, nil

}

func (r *jsonStreamReader) Close() (err error) {
	if r.gzr != nil {
		err = r.gzr.Close()
	}
	return err
}

func (r *jsonStreamReader) ReadRaw() (json.RawMessage, error) {
	if r.lastErr != nil {
		return nil, r.lastErr
	}
	jsonBin, err := r.r.ReadBytes('\n')
	if len(jsonBin) > 0 {
		if err == nil {
			jsonBin = jsonBin[:len(jsonBin)-1]  // remove last '\n'
		} else if err == io.EOF {
			r.lastErr, err = err, nil
		}
	}
	return jsonBin, err
}

func (r *jsonStreamReader) Read(holder interface{}) error {
	if r.lastErr != nil {
		return r.lastErr
	}
	jsonBin, err := r.r.ReadBytes('\n')
	if err != nil {
		if err == io.EOF && len(jsonBin) > 0 {
			// last item
			r.lastErr, err = err, nil
		} else {
			return err
		}
	}
	return r.fs.marshaler.Unmarshal(jsonBin, holder)
}

type jsonFileReader struct {
	fs   *fileServiceImpl
	fd   *os.File
	fr   *bufio.Reader
	gzr  *gzip.Reader
	r    *bufio.Reader
	lastErr error
}

func (t *fileServiceImpl) OpenJsonFile(filePath string) (fs.JsonReader, error) {

	fd, err := os.Open(filePath)
	if err != nil {
		return nil, errors.Errorf("file open error '%s', %v", filePath, err)
	}

	return t.JsonFile(fd)
}

func (t *fileServiceImpl) JsonFile(fd *os.File) (fs.JsonReader, error) {

	var err error
	r := &jsonFileReader{
		fs: t,
		fd: fd,
	}

	r.fr = bufio.NewReaderSize(r.fd, t.bufferSize)

	if strings.HasSuffix(fd.Name(), ".gz") {
		r.gzr, err = gzip.NewReader(r.fr)
		if err != nil {
			return nil, errors.Errorf("gzip read error in '%s', %v", fd.Name(), err)
		}
		r.r = bufio.NewReader(r.gzr)
	} else {
		r.r = r.fr
	}

	return r, nil

}

func (r *jsonFileReader) Close() error {
	if r.gzr != nil {
		r.gzr.Close()
	}
	return r.fd.Close()
}

func (r *jsonFileReader) ReadRaw() (json.RawMessage, error) {
	if r.lastErr != nil {
		return nil, r.lastErr
	}
	jsonBin, err := r.r.ReadBytes('\n')
	if len(jsonBin) > 0 {
		if err == nil {
			jsonBin = jsonBin[:len(jsonBin)-1]  // remove last '\n'
		} else if err == io.EOF {
			r.lastErr, err = err, nil
		}
	}
	return jsonBin, err
}

func (r *jsonFileReader) Read(holder interface{}) error {
	if r.lastErr != nil {
		return r.lastErr
	}
	jsonBin, err := r.r.ReadBytes('\n')
	if err != nil {
		if err == io.EOF && len(jsonBin) > 0 {
			// last item
			r.lastErr, err = err, nil
		} else {
			return err
		}
	}
	return r.fs.marshaler.Unmarshal(jsonBin, holder)
}

func (t *fileServiceImpl) SplitJsonFile(inputFilePath string, limit int, partFn func (int) string) ([]string, error) {

	reader, err := t.OpenJsonFile(inputFilePath)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	var parts []string
	var writer fs.JsonWriter

	partNum := 1
	for cnt := limit; err == nil; cnt++ {

		raw, err := reader.ReadRaw()
		if err != nil {
			break
		}

		if cnt == limit {
			if writer != nil {
				writer.Close()
				writer = nil
			}
			partFilePath := partFn(partNum)
			writer, err = t.NewJsonFile(partFilePath)
			if err != nil {
				break
			}
			parts = append(parts, partFilePath)
			cnt = 0
			partNum++
		}

		err = writer.WriteRaw(raw)
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

func (t *fileServiceImpl) JoinJsonFiles(outputFilePath string, parts []string) error {

	writer, err := t.NewJsonFile(outputFilePath)
	if err != nil {
		return err
	}
	defer writer.Close()

	for _, part := range parts {

		reader, err := t.OpenJsonFile(part)
		if err != nil {
			return errors.Errorf("can not open file '%s', %v", part, err)
		}

		for {

			raw, err := reader.ReadRaw()
			if err != nil {
				break
			}

			err = writer.WriteRaw(raw)
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

