package writerat_pipe

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"
)

// Generates a byte slice of the given length.
// Each value is 1-255, starting from 1 and incrementing each byte.
// If the value exceeds 255, it will reset to 1.
// This ensures that no bytes in the slice are 0, making clear when bytes were written.
func genBytes(len int) []byte {
	var curVal byte = 0

	b := make([]byte, len)
	for i := 0; i < len; i++ {
		curVal++
		if curVal == 0 {
			curVal = 1
		}

		b[i] = curVal
	}

	return b
}

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
		time.Sleep(time.Millisecond * 100)

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

		time.Sleep(time.Millisecond * 100)

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

func TestCloseByReader(t *testing.T) {
	reader, writer := WriterAtPipe()

	_ = reader.Close()

	const toWrite = "Hello, world!"

	_, err := writer.WriteAt([]byte(toWrite), 0)
	if err != io.ErrClosedPipe {
		if err == nil {
			t.Fatalf("Expected to get ErrClosedPipe, but got nil")
		} else {
			t.Fatalf("Expected to get ErrClosedPipe, but got %v", err)
		}
	}

	readBuf := make([]byte, 1024)
	_, err = reader.Read(readBuf)
	if err != io.EOF {
		if err == nil {
			t.Fatalf("Expected to get EOF, but got nil")
		} else {
			t.Fatalf("Expected to read EOF, but read %v", err)
		}
	}
}

func TestCloseByReaderAfterBytesRead(t *testing.T) {
	reader, writer := WriterAtPipe()

	const toWrite = "Hello, world!"

	n, err := writer.WriteAt([]byte(toWrite), 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(toWrite) {
		t.Fatalf("Expected to write %d bytes, but wrote %d", len(toWrite), n)
	}

	readBuf := make([]byte, 1024)
	n, err = reader.Read(readBuf)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(toWrite) {
		t.Fatalf("Expected to read %d bytes, but read %d", len(toWrite), n)
	}
	if string(readBuf[:n]) != toWrite {
		t.Fatalf("Expected to read %s, but read %s", toWrite, string(readBuf[:n]))
	}

	_ = reader.Close()

	_, err = writer.WriteAt([]byte(toWrite), 0)
	if err != io.ErrClosedPipe {
		if err == nil {
			t.Fatalf("Expected to get ErrClosedPipe, but got nil")
		} else {
			t.Fatalf("Expected to get ErrClosedPipe, but got %v", err)
		}
	}

	_, err = reader.Read(readBuf)
	if err != io.EOF {
		if err == nil {
			t.Fatalf("Expected to get EOF, but got nil")
		} else {
			t.Fatalf("Expected to read EOF, but read %v", err)
		}
	}
}

func TestRead1MiB(t *testing.T) {
	reader, writer := WriterAtPipe()

	toWrite := genBytes(1024 * 1024)
	firstHalf := toWrite[:len(toWrite)/2]
	secondHalf := toWrite[len(toWrite)/2:]

	// Write first half
	n, err := writer.WriteAt(firstHalf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(firstHalf) {
		t.Fatalf("Expected to write %d bytes, but wrote %d", len(firstHalf), n)
	}

	reconstructed := make([]byte, len(toWrite)+1)
	totalRead := 0

	// Read as much as possible without blocking
	readBuf := make([]byte, 1024)
	for totalRead < len(firstHalf) {
		n, err := reader.Read(readBuf)

		if n > 0 {
			copy(reconstructed[totalRead:totalRead+n], readBuf[:n])
			totalRead += n
		}

		if err != nil {
			t.Fatal(err)
		}
	}

	// Expect to have read firstHalf
	if !bytes.Equal(reconstructed[:totalRead], firstHalf) {
		t.Fatalf("Expected to read first half (%d bytes), but read %d bytes", len(firstHalf), totalRead)
	}

	// Write second half
	n, err = writer.WriteAt(secondHalf, int64(len(firstHalf)))
	if err != nil {
		t.Fatal(err)
	}
	if n != len(secondHalf) {
		t.Fatalf("Expected to write %d bytes, but wrote %d", len(secondHalf), n)
	}

	_ = writer.Close()

	// Read as much as possible
	for {
		n, err := reader.Read(readBuf)

		if n > 0 {
			copy(reconstructed[totalRead:totalRead+n], readBuf[:n])
			totalRead += n
		}

		if err != nil {
			if err == io.EOF {
				break
			}

			t.Fatal(err)
		}
	}

	// Expect to have read everything
	if !bytes.Equal(reconstructed[:totalRead], toWrite) {
		t.Fatalf("Expected to read everything (%d bytes), but read %d bytes", len(toWrite), totalRead)
	}

	// The last byte in reconstructed should be 0, because it was not written.
	if reconstructed[totalRead] != 0 {
		t.Fatalf("Expected last byte to be 0, but it was %d", reconstructed[totalRead])
	}

	// All chunks except optionally the last (because it was not full) should be freed.
	numChunks := 0
	for range reader.pipe.chunks {
		numChunks++
	}
	if numChunks > 1 {
		t.Fatalf("Expected that all but 1 or 0 chunks were freed, but there were %d chunks", numChunks)
	}
}

func TestRead1MiBWithMultiChunkReads(t *testing.T) {
	reader, writer := WriterAtPipe()

	toWrite := genBytes(1024 * 1024)
	firstHalf := toWrite[:len(toWrite)/2]
	secondHalf := toWrite[len(toWrite)/2:]

	// Write first half
	n, err := writer.WriteAt(firstHalf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(firstHalf) {
		t.Fatalf("Expected to write %d bytes, but wrote %d", len(firstHalf), n)
	}

	reconstructed := make([]byte, len(toWrite)+1)

	n, err = reader.Read(reconstructed[:len(firstHalf)])
	if err != nil {
		t.Fatal(err)
	}

	// Expect to have read firstHalf
	if !bytes.Equal(reconstructed[:n], firstHalf) {
		t.Fatalf("Expected to read first half (%d bytes), but read %d bytes", len(firstHalf), n)
	}

	// Write second half
	n, err = writer.WriteAt(secondHalf, int64(len(firstHalf)))
	if err != nil {
		t.Fatal(err)
	}
	if n != len(secondHalf) {
		t.Fatalf("Expected to write %d bytes, but wrote %d", len(secondHalf), n)
	}

	n, err = reader.Read(reconstructed[len(firstHalf) : len(reconstructed)-1])
	if err != nil {
		t.Fatal(err)
	}

	// Expect to have read everything
	if !bytes.Equal(reconstructed[:len(reconstructed)-1], toWrite) {
		t.Fatalf("Expected to read everything (%d bytes), but read %d bytes", len(toWrite), n)
	}

	// The last byte in reconstructed should be 0, because it was not written.
	if reconstructed[len(reconstructed)-1] != 0 {
		t.Fatalf("Expected last byte to be 0, but it was %d", reconstructed[len(reconstructed)-1])
	}

	_ = writer.Close()

	// All chunks except optionally the last (because it was not full) should be freed.
	numChunks := 0
	for range reader.pipe.chunks {
		numChunks++
	}
	if numChunks > 1 {
		t.Fatalf("Expected that all but 1 or 0 chunks were freed, but there were %d chunks", numChunks)
	}
}

func TestOffsetsLessThanConsumedError(t *testing.T) {
	reader, writer := WriterAtPipe()

	const toWrite = "Hello, world!"

	n, err := writer.WriteAt([]byte(toWrite), 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(toWrite) {
		t.Fatalf("Expected to write %d bytes, but wrote %d", len(toWrite), n)
	}

	readBuf := make([]byte, 1024)
	_, err = reader.Read(readBuf[:len(toWrite)])
	if err != nil {
		t.Fatal(err)
	}

	// Write at offset less than consumed
	_, err = writer.WriteAt([]byte("Test"), 0)
	if err != ErrOffsetLessThanConsumed {
		t.Fatalf("Expected to get ErrOffsetLessThanConsumed, but got %v", err)
	}
}

func TestOffsetWritesWithChunkGaps(t *testing.T) {
	reader, writer := WriterAtPipe()

	fullChunk := genBytes(int(chunkSize))
	almostFullChunk := genBytes(int(chunkSize) - 10)
	almostEmptyChunk := genBytes(10)

	errChan := make(chan error)
	go func() {
		time.Sleep(time.Millisecond * 100)

		n, err := writer.WriteAt(almostFullChunk, 0)
		if err != nil {
			errChan <- err
			return
		}
		if n != len(almostFullChunk) {
			errChan <- fmt.Errorf("expected to write %d bytes, but wrote %d", len(almostFullChunk), n)
			return
		}

		// Leave full chunk gap
		n, err = writer.WriteAt(almostFullChunk, int64(chunkSize)*2)
		if err != nil {
			errChan <- err
			return
		}
		if n != len(almostFullChunk) {
			errChan <- fmt.Errorf("expected to write %d bytes, but wrote %d", len(almostFullChunk), n)
			return
		}

		time.Sleep(time.Millisecond * 100)

		// Fill in gap
		n, err = writer.WriteAt(fullChunk, int64(chunkSize)*1)
		if err != nil {
			errChan <- err
			return
		}
		if n != len(fullChunk) {
			errChan <- fmt.Errorf("expected to write %d bytes, but wrote %d", len(fullChunk), n)
			return
		}
		n, err = writer.WriteAt(almostEmptyChunk, int64(len(almostFullChunk)))
		if err != nil {
			errChan <- err
			return
		}
		if n != len(almostEmptyChunk) {
			errChan <- fmt.Errorf("expected to write %d bytes, but wrote %d", len(almostEmptyChunk), n)
			return
		}

		_ = writer.Close()

		errChan <- nil
	}()

	readBuf := make([]byte, int(chunkSize)*3)

	// Consume first almost full chunk
	n, err := reader.Read(readBuf)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(almostFullChunk) {
		t.Fatalf("Expected to read %d bytes, but read %d", len(almostFullChunk), n)
	}
	if !bytes.Equal(almostFullChunk, readBuf[:n]) {
		t.Fatalf("Did not get expected data from first chunk read")
	}

	if !bytes.Equal(almostFullChunk, readBuf[:n]) {
		t.Fatalf("Did not get expected data from first chunk read")
	}

	// Consume everything else
	n, err = reader.Read(readBuf)
	if err == nil {
		t.Fatal("Expected to get an io.EOF error, but got nil")
	} else if err != io.EOF {
		t.Fatalf("Expected to get an io.EOF error, but got %v", err)
	}
	expectCount := len(almostEmptyChunk) + len(fullChunk) + len(almostFullChunk)
	if n != expectCount {
		t.Fatalf("Expected to read %d bytes, but read %d", expectCount, n)
	}

	expectBytes := append(append(almostEmptyChunk, fullChunk...), almostFullChunk...)

	if !bytes.Equal(expectBytes, readBuf[:n]) {
		t.Fatalf("Got wrong bytes for the rest of the pipe")
	}

	if err := <-errChan; err != nil {
		t.Fatal(err)
	}
}

func TestManyConcurrentWrites(t *testing.T) {
	const tries = 100
	const writes = 100

	// Time it
	startTime := time.Now()

	for tryId := 0; tryId < tries; tryId++ {
		reader, writer := WriterAtPipe()

		errChan := make(chan error)

		const toWrite = "Hello, world!"

		for i := 0; i < writes; i++ {
			offset := int64(i) * int64(len(toWrite))

			go func() {
				n, err := writer.WriteAt([]byte(toWrite), offset)
				if err != nil {
					errChan <- err
					return
				}
				if n != len(toWrite) {
					errChan <- fmt.Errorf("expected to write %d bytes, but wrote %d", len(toWrite), n)
					return
				}

				errChan <- nil
			}()
		}

		for i := 0; i < writes; i++ {
			err := <-errChan
			if err != nil {
				t.Fatal(err)
			}
		}

		for i := 0; i < int(writes/chunkSize); i++ {
			_, hasChunk := reader.pipe.chunks[uint64(i)]
			if !hasChunk {
				t.Fatalf("Expected to have chunk %d, but it was missing", i)
			}
		}

		readBuf := make([]byte, len(toWrite)*writes)
		n, err := reader.Read(readBuf)
		if err != nil {
			t.Fatal(err)
		}
		if n != len(toWrite)*writes {
			t.Fatalf("Expected to read %d bytes, but read %d", len(toWrite)*writes, n)
		}

		_ = writer.Close()
	}

	elapsed := time.Since(startTime)

	if elapsed.Milliseconds() > 100 {
		t.Fatalf("Performing %d concurrent writes and then sequential read took over 100ms. Implementation is too slow.", tries*writes)
	}
}
