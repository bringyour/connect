package connect

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"testing"
)

func TestReadHttpResponseBodyLimit(t *testing.T) {
	const limit = int64(32)
	exact := bytes.Repeat([]byte{0x42}, int(limit))

	response := &http.Response{
		Body:          io.NopCloser(bytes.NewReader(exact)),
		ContentLength: int64(len(exact)),
	}
	body, err := readHttpResponseBody(response, limit)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, exact) {
		t.Fatal("exact-limit body changed")
	}

	// Unknown/chunked lengths are limited while reading, not trusted merely
	// because Content-Length is absent.
	response = &http.Response{
		Body:          io.NopCloser(bytes.NewReader(append(exact, 0x01))),
		ContentLength: -1,
	}
	body, err = readHttpResponseBody(response, limit)
	if !errors.Is(err, ErrHttpResponseBodyTooLarge) {
		t.Fatalf("chunked oversized error = %v, want %v", err, ErrHttpResponseBodyTooLarge)
	}
	if body != nil {
		t.Fatal("chunked oversized response returned a body")
	}
}

func TestReadHttpResponseBodyRejectsKnownOversizeBeforeRead(t *testing.T) {
	reader := &countingReader{Reader: bytes.NewReader([]byte("not read"))}
	response := &http.Response{
		Body:          io.NopCloser(reader),
		ContentLength: 1024,
	}

	_, err := readHttpResponseBody(response, 32)
	if !errors.Is(err, ErrHttpResponseBodyTooLarge) {
		t.Fatalf("known oversized error = %v, want %v", err, ErrHttpResponseBodyTooLarge)
	}
	if reader.readCount != 0 {
		t.Fatalf("oversized body was read %d times", reader.readCount)
	}
}

func TestEvalResultReadsOnlySelectedResponseBody(t *testing.T) {
	selectedReader := &countingReader{Reader: bytes.NewReader([]byte("selected"))}
	selected := newEvalResultFromHttpResponse(&http.Response{
		Body:          io.NopCloser(selectedReader),
		ContentLength: 8,
	}, nil, 32)
	defer selected.Close()

	losingReader := &countingReader{Reader: bytes.NewReader([]byte("losing"))}
	losing := newEvalResultFromHttpResponse(&http.Response{
		Body:          io.NopCloser(losingReader),
		ContentLength: 6,
	}, nil, 32)
	losing.Close()

	selected.Selected()
	if selected.err != nil {
		t.Fatal(selected.err)
	}
	if string(selected.bodyBytes) != "selected" {
		t.Fatalf("selected body = %q", selected.bodyBytes)
	}
	if selectedReader.readCount == 0 {
		t.Fatal("selected response was not read")
	}
	if losingReader.readCount != 0 {
		t.Fatalf("losing response was read %d times", losingReader.readCount)
	}
}

type countingReader struct {
	io.Reader
	readCount int
}

func (r *countingReader) Read(p []byte) (int, error) {
	r.readCount++
	return r.Reader.Read(p)
}
