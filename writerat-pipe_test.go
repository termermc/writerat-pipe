package writerat_pipe

import (
	"fmt"
	"io"
	"testing"
	"time"
)

func TestBasicPipeline(t *testing.T) {
	reader, writer := WriterAtPipe()

	const toWrite = "Hello, world!"

	wroteBytesChan := make(chan int)
	errChan := make(chan error)
	go func() {
		n, err := writer.WriteAt([]byte(toWrite), 0)
		if err != nil {
			errChan <- err
			return
		}

		wroteBytesChan <- n
	}()

	readBuf := make([]byte, 1024)
	n, err := reader.Read(readBuf)
	if err != nil {
		t.Fatal(err)
	}

	if n != len(toWrite) {
		t.Fatalf("Expected to write %d bytes, but wrote %d", len(toWrite), n)
	}

	if string(readBuf[:n]) != toWrite {
		t.Fatalf("Expected to read %s, but read %s", toWrite, string(readBuf[:n]))
	}

	select {
	case n := <-wroteBytesChan:
		if n != len(toWrite) {
			t.Fatalf("Expected to write %d bytes, but wrote %d", len(toWrite), n)
		}
	case err := <-errChan:
		t.Fatal(err)
	}
}

func TestWaitForContiguous(t *testing.T) {
	reader, writer := WriterAtPipe()

	const toWrite1 = "Hello, world!"
	const toWrite2 = "Hi, "

	const expected = toWrite2 + toWrite1

	errChan := make(chan error)
	go func() {
		n, err := writer.WriteAt([]byte(toWrite1), int64(len(toWrite2)))
		if err != nil {
			errChan <- err
			return
		}
		if n != len(toWrite1) {
			errChan <- fmt.Errorf("expected to write %d bytes, but wrote %d", len(toWrite1), n)
			return
		}

		n, err = writer.WriteAt([]byte(toWrite2), 0)
		if err != nil {
			errChan <- err
			return
		}
		if n != len(toWrite2) {
			errChan <- fmt.Errorf("expected to write %d bytes, but wrote %d", len(toWrite2), n)
			return
		}

		errChan <- nil
	}()

	readBuf := make([]byte, 1024)
	n, err := reader.Read(readBuf)
	if err != nil {
		t.Fatal(err)
	}

	if n != len(expected) {
		t.Fatalf("Expected to write %d bytes, but wrote %d", len(expected), n)
	}

	if string(readBuf[:n]) != expected {
		t.Fatalf("Expected to read %s, but read %s", expected, string(readBuf[:n]))
	}

	if err := <-errChan; err != nil {
		t.Fatal(err)
	}
}

func TestCheckeredWrites(t *testing.T) {
	reader, writer := WriterAtPipe()

	const writeSize = 5
	const toWrite1 = "part1"
	const toWrite2 = "part4"
	const toWrite3 = "part3"
	const toWrite4 = "part2"

	const expected1 = toWrite1
	const expected2 = toWrite4 + toWrite3 + toWrite2

	errChan := make(chan error)
	go func() {
		time.Sleep(time.Millisecond * 1000)

		// O...
		n, err := writer.WriteAt([]byte(toWrite1), 0)
		if err != nil {
			errChan <- err
			return
		}
		if n != len(toWrite1) {
			errChan <- fmt.Errorf("expected to write %d bytes, but wrote %d", len(toWrite1), n)
			return
		}

		time.Sleep(time.Millisecond * 1000)

		// ...O
		n, err = writer.WriteAt([]byte(toWrite2), writeSize*3)
		if err != nil {
			errChan <- err
			return
		}
		if n != len(toWrite2) {
			errChan <- fmt.Errorf("expected to write %d bytes, but wrote %d", len(toWrite2), n)
			return
		}

		// ..O.
		n, err = writer.WriteAt([]byte(toWrite3), writeSize*2)
		if err != nil {
			errChan <- err
			return
		}
		if n != len(toWrite3) {
			errChan <- fmt.Errorf("expected to write %d bytes, but wrote %d", len(toWrite3), n)
			return
		}

		// .O..
		n, err = writer.WriteAt([]byte(toWrite4), writeSize)
		if err != nil {
			errChan <- err
			return
		}
		if n != len(toWrite4) {
			errChan <- fmt.Errorf("expected to write %d bytes, but wrote %d", len(toWrite4), n)
			return
		}

		_ = writer.Close()

		errChan <- nil
	}()

	readBuf := make([]byte, 1024)
	n, err := reader.Read(readBuf)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(expected1) {
		t.Fatalf("Read #1: Expected to read %d bytes, but read %d", len(expected1), n)
	}
	if string(readBuf[:n]) != expected1 {
		t.Fatalf("Read #1: Expected to read %s, but read %s", expected1, string(readBuf[:n]))
	}

	n, err = reader.Read(readBuf[n:])
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != len(expected2) {
		t.Fatalf("Read #2: Expected to read %d bytes, but read %d", len(expected2), n)
	}
	if string(readBuf[writeSize:writeSize+n]) != expected2 {
		t.Fatalf("Read #2: Expected to read %s, but read %s", expected2, string(readBuf[n:n+len(expected2)]))
	}

	// The pipe will already be closed at this point, so it should return EOF
	_, err = reader.Read(readBuf[n:])
	if err != io.EOF {
		t.Fatalf("Expected to read EOF, but read %v", err)
	}

	if err := <-errChan; err != nil {
		t.Fatal(err)
	}
}

func TestReadAfterClose(t *testing.T) {
	reader, writer := WriterAtPipe()

	const toWrite = "Hello, world!"

	n, err := writer.WriteAt([]byte(toWrite), 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(toWrite) {
		t.Fatalf("Expected to write %d bytes, but wrote %d", len(toWrite), n)
	}

	_ = writer.Close()

	readBuf := make([]byte, 1024)
	n, err = reader.Read(readBuf)
	if err != io.EOF {
		t.Fatalf("Expected to read EOF, but read %v", err)
	}
	if n != len(toWrite) {
		t.Fatalf("Expected to read %d bytes, but read %d", len(toWrite), n)
	}
	if string(readBuf[:n]) != toWrite {
		t.Fatalf("Expected to read %s, but read %s", toWrite, string(readBuf[:n]))
	}
}

func TestReadAfterCloseWithLostBytes(t *testing.T) {
	reader, writer := WriterAtPipe()

	const toWrite = "Hello, world!"
	const toLose = "Test"

	n, err := writer.WriteAt([]byte(toWrite), 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(toWrite) {
		t.Fatalf("Expected to write %d bytes, but wrote %d", len(toWrite), n)
	}

	// Write with gap of 1 byte to avoid contiguous readable bytes
	n, err = writer.WriteAt([]byte(toLose), int64(len(toWrite)+1))
	if err != nil {
		t.Fatal(err)
	}
	if n != len(toLose) {
		t.Fatalf("Expected to write %d bytes, but wrote %d", len(toLose), n)
	}

	_ = writer.Close()

	readBuf := make([]byte, 1024)
	n, err = reader.Read(readBuf)
	if err != io.EOF {
		t.Fatalf("Expected to read EOF, but read %v", err)
	}
	if n != len(toWrite) {
		t.Fatalf("Expected to read %d bytes, but read %d", len(toWrite), n)
	}
	if string(readBuf[:n]) != toWrite {
		t.Fatalf("Expected to read %s, but read %s", toWrite, string(readBuf[:n]))
	}

	// Make sure no more than n bytes were written
	if string(readBuf[:n+1]) != toWrite+"\x00" {
		t.Fatalf("Expected only %d bytes were written, but at least one more was written to buffer", len(toWrite))
	}
}
