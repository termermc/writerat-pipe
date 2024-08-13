package writerat_pipe

import (
	"errors"
	"io"
	"math"
	"sort"
	"sync"
)

// chunkRegionMarker is a marker for a region of data that has been written in a pipeChunk
type chunkRegionMarker struct {
	offset uint16
	length uint16
}

const chunkSize = uint16(65535)

// pipeChunk is a chunk of data that has been written to a WriterAtPipe.
// Chunks can be a max of 65535 bytes because their length is stored in a uint16.
// A small chunk size was chosen to keep markers short and to avoid making too many micro-allocations.
type pipeChunk struct {
	// The underlying data.
	// The length will always be 65536 bytes, but not all of it is guaranteed to be written.
	// The regions of the data which have been written are kept track of in markers.
	data []byte

	// The markers of the regions of data within the pipeChunk that have been written.
	markers []chunkRegionMarker
}

// See WriterAtPipe().
type writerAtPipe struct {
	// Synchronizes write operations.
	mutex sync.Mutex

	// Whether the pipe has been closed.
	isClosed bool

	// Channel used to signal that there are possibly new bytes to read.
	hasBytesChan chan struct{}

	// The total number of bytes which have been read from the pipe.
	// Requires mutex.
	consumed uint64

	// Map of chunks and their offsets.
	// Offsets are multiples of 65535.
	chunks map[uint64]*pipeChunk
}

type WriterAtPipeReader struct {
	pipe writerAtPipe
}
type WriterAtPipeWriter struct {
	r WriterAtPipeReader
}

// ErrOffsetLessThanConsumed is returned when a write is attempted at an offset less than the offset that has already been consumed by the reader.
var ErrOffsetLessThanConsumed = errors.New("tried to write at offset less than the offset that has already been consumed")

// ErrMissingChunk is returned when a read is attempted from a chunk that does not exist
// This should never happen, and indicates a bug in writerat-pipe's implementation.
var ErrMissingChunk = errors.New("tried to read from an internal chunk that does not exist (bug in writerat-pipe)")

// ErrChunkHasNoMarkers is returned when a read is attempted from a chunk that has no markers.
// This should never happen, and indicates a bug in writerat-pipe's implementation.
var ErrChunkHasNoMarkers = errors.New("tried to read from a chunk that has no markers (bug in writerat-pipe)")

// WriteAt writes to the pipe at the given offset.
// It returns the number of bytes written, and an error if one occurred.
// If either side of the pipe is closed, io.ErrClosedPipe will be returned.
//
// Writes at offsets less than the offset that has already been consumed by the reader will result in an ErrOffsetLessThanConsumed error.
func (w *WriterAtPipeWriter) WriteAt(p []byte, off int64) (n int, err error) {
	w.r.pipe.mutex.Lock()
	if w.r.pipe.isClosed {
		w.r.pipe.mutex.Unlock()
		return 0, io.ErrClosedPipe
	}
	w.r.pipe.mutex.Unlock()

	totalToWrite := len(p)
	if totalToWrite == 0 {
		return 0, nil
	}

	written := 0

	// Synchronize writes
	w.r.pipe.mutex.Lock()

	// After everything is done, unlock the mutex and notify readers
	defer func() {
		w.r.pipe.mutex.Unlock()

		if written > 0 {
			// If there is a waiting read call, notify it that there are possibly new bytes to read.
			select {
			case w.r.pipe.hasBytesChan <- struct{}{}:
			default:
			}
		}
	}()

	consumed := w.r.pipe.consumed
	if uint64(off) < consumed {
		return 0, ErrOffsetLessThanConsumed
	}

	// Figure out which chunk this write begins in
	chunkKey := uint64(math.Floor(float64(off) / float64(chunkSize)))
	for written < totalToWrite {
		// Create the chunk if it does not exist
		var chunk *pipeChunk
		var chunkExists bool
		if chunk, chunkExists = w.r.pipe.chunks[chunkKey]; !chunkExists {
			chunk = &pipeChunk{
				data: make([]byte, chunkSize),
			}
			w.r.pipe.chunks[chunkKey] = chunk
		}

		// Only the first chunk will have an offset, everything else is just a normal sequential write
		var offsetInChunk uint16
		if written == 0 {
			offsetInChunk = uint16(off % int64(chunkSize))
		} else {
			offsetInChunk = 0
		}

		// Calculate total bytes in the chunk to write
		var writeUntil uint16
		leftToWrite := totalToWrite - written
		if int(offsetInChunk)+leftToWrite < int(chunkSize) {
			writeUntil = offsetInChunk + uint16(leftToWrite)
		} else {
			writeUntil = chunkSize
		}

		// Write the chunk
		writtenInChunk := writeUntil - offsetInChunk
		copy(chunk.data[offsetInChunk:], p[written:written+int(writtenInChunk)])
		written += int(writtenInChunk)

		// Create the marker
		chunk.markers = append(chunk.markers, chunkRegionMarker{
			offset: offsetInChunk,
			length: writtenInChunk,
		})

		// Merge markers if needed
		if len(chunk.markers) > 1 {
			sort.Slice(chunk.markers, func(i, j int) bool {
				return chunk.markers[i].offset < chunk.markers[j].offset
			})

			var res []chunkRegionMarker
			lastOffset := chunk.markers[0].offset
			lastEnd := chunk.markers[0].offset + chunk.markers[0].length

			for _, marker := range chunk.markers[1:] {
				if marker.offset <= lastEnd {
					if marker.offset+marker.length > lastEnd {
						lastEnd = marker.offset + marker.length
					}
				} else {
					res = append(res, chunkRegionMarker{
						offset: lastOffset,
						length: lastEnd - lastOffset,
					})

					lastOffset = marker.offset
					lastEnd = marker.offset + marker.length
				}
			}
			res = append(res, chunkRegionMarker{
				offset: lastOffset,
				length: lastEnd - lastOffset,
			})
			chunk.markers = res
		}

		chunkKey++
	}

	return written, nil
}

// Read reads up to len(p) bytes into p. It will always read at least one byte, unless EOF is reached.
// It returns the number of bytes read, and an error if one occurred.
// If the pipe is closed, io.EOF will be returned.
//
// Conforms to the spec of io.Reader.
func (r *WriterAtPipeReader) Read(p []byte) (n int, err error) {
	r.pipe.mutex.Lock()
	isClosed := r.pipe.isClosed
	r.pipe.mutex.Unlock()

	totalToRead := len(p)
	if totalToRead == 0 {
		return 0, nil
	}

	alreadyRead := 0

	for alreadyRead < 1 {
		// Stop writes while reading contiguous bytes
		r.pipe.mutex.Lock()

		// Read as many contiguous bytes as possible
		readThisTry := 0
		for alreadyRead < totalToRead {
			readThisTry = 0

			// Read the next chunk
			chunkKey := uint64(math.Floor(float64(r.pipe.consumed) / float64(chunkSize)))
			chunk, chunkExists := r.pipe.chunks[chunkKey]
			if !chunkExists {
				// The chunk we need does not exist, so we need to wait for it to be written.
				// Breaking here will cause us to wait for a message on pipe.hasBytesChan.
				break
			}

			if len(chunk.markers) == 0 {
				// Chunks should always have at least one marker, so this is a bug in writerat-pipe.
				r.pipe.mutex.Unlock()
				return 0, ErrChunkHasNoMarkers
			}

			startOffsetInChunk := uint16(r.pipe.consumed % uint64(chunkSize))

			var startByte uint16
			var endByte uint16

			// Find marker that intersects with the start offset
			for _, marker := range chunk.markers {
				if marker.offset <= startOffsetInChunk && marker.offset+marker.length > startOffsetInChunk {
					startByte = startOffsetInChunk
					endByte = marker.offset + marker.length

					// Make sure the amount to read in this chunk does not exceed the amount we have left to read
					if int(endByte-startByte) > totalToRead-alreadyRead {
						endByte = startByte + uint16(totalToRead-alreadyRead)
					}

					break
				}
			}

			if endByte <= startByte {
				// No intersecting marker found, so we need to wait for more bytes to be written.
				break
			}

			// Copy data to input buffer
			copy(p[alreadyRead:], chunk.data[startByte:endByte])

			// If the chunk was fully consumed, remove it
			if endByte >= chunkSize {
				delete(r.pipe.chunks, chunkKey)
			}

			r.pipe.consumed += uint64(endByte - startByte)
			alreadyRead += int(endByte - startByte)
			readThisTry = int(endByte - startByte)
		}

		// Update isClosed and shouldWait before releasing the lock so that they are up to date
		isClosed = r.pipe.isClosed
		shouldWait := alreadyRead == 0 && !isClosed

		r.pipe.mutex.Unlock()

		if shouldWait {
			// Nothing was read but the pipe is still open, so we need to wait for bytes to be written and then try again
			<-r.pipe.hasBytesChan
		} else if readThisTry < 1 {
			// There were some bytes read or the pipe was closed, so there is no need to try again; return with the bytes we have read
			break
		}
	}

	var resErr error

	// We only want to return EOF on a closed pipe if we have read all possible bytes
	if isClosed && (alreadyRead < totalToRead) {
		resErr = io.EOF
	}

	return alreadyRead, resErr
}

// Close closes the pipe.
// It will block until any ongoing reads or writes have completed.
// It will always return nil.
// Writes after close will result in an io.ErrClosedPipe error.
func (r *WriterAtPipeReader) Close() error {
	r.pipe.mutex.Lock()
	defer r.pipe.mutex.Unlock()

	if r.pipe.isClosed {
		return nil
	}

	r.pipe.isClosed = true

	return nil
}

// Close closes the pipe.
// It will block until any ongoing reads or writes have completed.
// It will always return nil.
// If there are currently readable bytes, they will be able to be read, but read calls after those bytes have been consumed will result in an io.EOF error.
//
// If there are any bytes that have been written but are not readable, they will be lost.
// For example, if our buffer looks like this:
//
// ++++------++
//
// The first 4 bytes will be read, but the final 2 bytes will be lost.
func (w *WriterAtPipeWriter) Close() error {
	return w.r.Close()
}

// WriterAtPipe returns a pair of Reader and Writer which can be used to write to a pipe.
// It functions similarly to io.Pipe, but with the following differences:
//   - Writes are buffered and do not wait on the reader.
//   - The writer implements io.WriterAt and allows out-of-order writes.
//
// While the writer implements io.WriterAt, it has the following caveats:
//   - Writes cannot be at offsets less than the offset that has already been consumed by the reader.
//
// For example, if there is a write 100 byte at offset 0, the reader reads 50 bytes, then the writer attempts to write 10 bytes at offset 30, it will fail.
// This is because 50 bytes have already been consumed, so all writes from that point cannot be at an offset less than 50.
func WriterAtPipe() (*WriterAtPipeReader, *WriterAtPipeWriter) {
	pw := &WriterAtPipeWriter{r: WriterAtPipeReader{pipe: writerAtPipe{
		chunks:       make(map[uint64]*pipeChunk),
		hasBytesChan: make(chan struct{}, 1),
	}}}

	return &pw.r, pw
}
