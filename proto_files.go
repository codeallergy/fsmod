/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package fsmod

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"github.com/sprintframework/fs"
	"google.golang.org/protobuf/proto"
	"github.com/pkg/errors"
	"io"
	"os"
	"strings"
)

type protoStreamReader struct {
	fd   io.Reader
	fr   *bufio.Reader
	gzr   *gzip.Reader
	r     io.Reader
	lenBuf  [4]byte
}

func (t *fileServiceImpl) ProtoStream(fr io.Reader, withGzip bool) (fs.ProtoReader, error) {

	var err error
	r := &protoStreamReader{
		fd: fr,
	}

	r.fr = bufio.NewReaderSize(r.fd, t.bufferSize)

	if withGzip {
		r.gzr, err = gzip.NewReader(r.fr)
		if err != nil {
			return nil, errors.Errorf("gzip read error  %v", err)
		}
		r.r = r.gzr
	} else {
		r.r = r.fr
	}

	return r, nil

}

func (r *protoStreamReader) Close() error {
	if r.gzr != nil {
		r.gzr.Close()
	}
	return nil
}

func (r *protoStreamReader) ReadTo(message proto.Message) error {

	lenBuf := r.lenBuf[:]

	n, err := io.ReadFull(r.r, lenBuf)
	if err != nil {
		return err
	} else if n != len(lenBuf) {
		return errors.Errorf("wrong number read %d, expected %d", n, len(lenBuf))
	}

	blockLen := int(binary.BigEndian.Uint32(lenBuf))

	block := make([]byte, blockLen)
	n, err = io.ReadFull(r.r, block)
	if err != nil {
		return err
	} else if n != len(block) {
		return errors.Errorf("wrong read bytes %d expected %d", n, len(block))
	}

	return proto.Unmarshal(block, message)
}

type protoFileReader struct {
	fd   *os.File
	fr   *bufio.Reader
	gzr   *gzip.Reader
	r     io.Reader
	lenBuf  [4]byte
}

func (t *fileServiceImpl) OpenProtoFile(filePath string) (fs.ProtoReader, error) {

	fd, err := os.Open(filePath)
	if err != nil {
		return nil, errors.Errorf("file open error '%s', %v", filePath, err)
	}

	return t.ProtoFile(fd)
}

func (t *fileServiceImpl) ProtoFile(fd *os.File) (fs.ProtoReader, error) {

	var err error
	r := &protoFileReader{
		fd: fd,
	}

	r.fr = bufio.NewReaderSize(r.fd, t.bufferSize)

	if strings.HasSuffix(fd.Name(), ".gz") {
		r.gzr, err = gzip.NewReader(r.fr)
		if err != nil {
			return nil, errors.Errorf("gzip read error in '%s', %v", fd.Name(), err)
		}
		r.r = r.gzr
	} else {
		r.r = r.fr
	}

	return r, nil

}

func (r *protoFileReader) Close() error {
	if r.gzr != nil {
		r.gzr.Close()
	}
	return r.fd.Close()
}

func (r *protoFileReader) ReadTo(message proto.Message) error {

	lenBuf := r.lenBuf[:]

	n, err := io.ReadFull(r.r, lenBuf)
	if err != nil {
		return err
	} else if n != len(lenBuf) {
		return errors.Errorf("wrong number read %d, expected %d", n, len(lenBuf))
	}

	blockLen := int(binary.BigEndian.Uint32(lenBuf))

	block := make([]byte, blockLen)
	n, err = io.ReadFull(r.r, block)
	if err != nil {
		return err
	} else if n != len(block) {
		return errors.Errorf("wrong read bytes %d expected %d", n, len(block))
	}

	return proto.Unmarshal(block, message)
}

type protoStreamWriter struct {
	fd   io.Writer
	fw   *bufio.Writer
	gzw  *gzip.Writer
	bw   *bufio.Writer
	w    io.Writer
}

func (t *fileServiceImpl) NewProtoStream(fd io.Writer, withGzip bool) fs.ProtoWriter {

	w := &protoStreamWriter{
		fd:              fd,
	}

	w.fw = bufio.NewWriterSize(fd, t.bufferSize)

	if withGzip {
		w.gzw = gzip.NewWriter(w.fw)
		w.bw = bufio.NewWriterSize(w.gzw, t.bufferSize)
		w.w = w.bw
	} else {
		w.w = w.fw
	}

	return w
}

func (w *protoStreamWriter) Close() (err error) {
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

func (w *protoStreamWriter) Write(message proto.Message) ([]byte, error) {
	return protobufWrite(w.w, message)
}

func protobufWrite(w io.Writer, message proto.Message) ([]byte, error) {

	var lenBufArr  [4]byte
	lenBuf := lenBufArr[:]

	blob, err := proto.Marshal(message)
	if err != nil {
		return nil, errors.Errorf("proto marshal error, %v", err)
	}

	binary.BigEndian.PutUint32(lenBuf, uint32(len(blob)))

	if n, err := w.Write(lenBuf); err != nil {
		return blob, err
	} else if n != len(lenBuf) {
		return blob, errors.Errorf("wrong number written %d, expected %d", n, len(lenBuf))
	}

	if n, err := w.Write(blob); err != nil {
		return blob, err
	} else if n != len(blob) {
		return blob, errors.Errorf("wrong number written %d, expected %d", n, len(blob))
	}

	return blob, nil
}

type protoBufWriter struct {
	fw   bytes.Buffer
	gzw  *gzip.Writer
	bw   *bufio.Writer
	w    io.Writer
}

func (t *fileServiceImpl) NewProtoBuf(withGzip bool) (fs.ProtoWriter, error) {

	w := new(protoBufWriter)

	if withGzip {
		w.gzw = gzip.NewWriter(&w.fw)
		w.bw = bufio.NewWriterSize(w.gzw, t.bufferSize)
		w.w = w.bw
	} else {
		w.w = &w.fw
	}

	return w, nil
}

func (w *protoBufWriter) Close() error {
	if w.bw != nil {
		w.bw.Flush()
	}
	if w.gzw != nil {
		w.gzw.Flush()
		w.gzw.Close()
	}
	return nil
}

func (w *protoBufWriter) Buffer() io.Reader {
	return &w.fw
}

func (w *protoBufWriter) Bytes() []byte {
	return w.fw.Bytes()
}

func (w *protoBufWriter) Write(message proto.Message) ([]byte, error) {
	return protobufWrite(w.w, message)
}

type protoFileWriter struct {
	fd   *os.File
	fw   *bufio.Writer
	gzw  *gzip.Writer
	bw   *bufio.Writer
	w    io.Writer
}

func (t *fileServiceImpl) NewProtoFile(filePath string) (fs.ProtoWriter, error) {

	var err error
	w := new(protoFileWriter)

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

func (w *protoFileWriter) Close() error {
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

func (w *protoFileWriter) Write(message proto.Message) ([]byte, error) {
	return protobufWrite(w.w, message)
}

func (t *fileServiceImpl) SplitProtoFile(inputFilePath string, holder proto.Message, limit int, partFn func (int) string) ([]string, error) {

	reader, err := t.OpenProtoFile(inputFilePath)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	var parts []string
	var writer fs.ProtoWriter

	partNum := 1
	for cnt := limit; err == nil; cnt++ {

		err = reader.ReadTo(holder)
		if err != nil {
			break
		}

		if cnt == limit {
			if writer != nil {
				writer.Close()
				writer = nil
			}
			partFilePath := partFn(partNum)
			writer, err = t.NewProtoFile(partFilePath)
			if err != nil {
				break
			}
			parts = append(parts, partFilePath)
			cnt = 0
			partNum++
		}

		_, err = writer.Write(holder)
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

func (t *fileServiceImpl) JoinProtoFiles(outputFilePath string, row proto.Message, parts []string) error {

	writer, err := t.NewProtoFile(outputFilePath)
	if err != nil {
		return err
	}
	defer writer.Close()

	for _, part := range parts {

		reader, err := t.OpenProtoFile(part)
		if err != nil {
			return errors.Errorf("can not open file '%s', %v", part, err)
		}

		for {

			err = reader.ReadTo(row)
			if err != nil {
				break
			}

			_, err = writer.Write(row)
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

