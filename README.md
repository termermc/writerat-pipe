# writerat-pipe
Implementation of Go's `io.Pipe` whose writer implements `io.WriterAt`, with some [caveats](#caveats).

It can be thought of as a normal pipe, except with a buffered writer that implements `io.WriterAt`.

## Example

```go
package main

import "github.com/termermc/writerat-pipe"

func main() {
	reader, writer := writerat_pipe.WriterAtPipe()
	
	go func() {
		// Write at an offset so that the reader cannot read anything yet.
		// Currently, bytes 1-13 have been written, but since 0 bytes have been consumed, the reader cannot read anything.
		_, err := writer.WriteAt([]byte("Hello, world!"), 1)
		if err != nil {
			panic(err)
		}
		
		// Write at offset 0, which will make a line of contiguous bytes from 0-13 readable.
		// The reader will now be able to read everything.
		_, err = writer.WriteAt([]byte("!"), 0)
		if err != nil {
			panic(err)
		}
	}()
	
	readBuf := make([]byte, 1024)
	n, err := reader.Read(readBuf)
	if err != nil {
		panic(err)
	}
	
	println(string(readBuf[:n])) // "!Hello, world!"
	
	_ = writer.Close()
}
```

For more examples, see the `writerat-pipe_test.go` file.

## Use Case

This library was created to support piping the output of the AWS SDK's `s3manager.Downloader` to an `s3manager.Uploader` without having to rely on a file or excessive memory usage.

## Caveats

The following caveats apply to this implementation:

 1. Writes cannot be at offsets less than the number of bytes that have already been consumed by the reader.
 2. There is no limit on the buffer size.

### Caveat #1

The reader only implements `io.Reader`, not `io.ReaderAt`, so bytes are read sequentially once, not randomly at any time.
This means that any bytes that are written at an offset less than the number of consumed bytes will be unreadable; effectively a memory leak.
To prevent this, `io.ErrOffsetLessThanConsumed` will be returned when a write at such an offset is attempted.

### Caveat #2

I just did not implement a buffer limit. Feel free to send in a PR with appropriate tests if you need this feature.
