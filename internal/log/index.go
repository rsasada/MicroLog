package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

const (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {

	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())
	err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes))
	if err != nil {
		return nil, err
	}
	idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	)
	if err != nil {
		return nil, err
	}
	return idx, nil
}

func (idx *index) Close() error {

	if err := idx.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := idx.file.Sync(); err != nil {
		return err
	}
	if err := idx.file.Truncate(int64(idx.size)); err != nil {
		return err
	}
	return idx.file.Close()
}

func (idx *index) Read(offIn int64) (offOut uint32, pos uint64, err error) {

	if idx.size == 0 {
		return 0, 0, io.EOF
	}
	if offIn < 0 {
		offOut = uint32((idx.size / entWidth) - 1)
	} else {
		offOut = uint32(offIn)
	}
	pos = uint64(offOut) * entWidth
	if idx.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	offOut = binary.BigEndian.Uint32(idx.mmap[pos : pos+offWidth])
	pos = binary.BigEndian.Uint64(idx.mmap[pos+offWidth : pos+entWidth])
	return offOut, pos, nil
}

func (idx *index) Write(off uint32, pos uint64) error {

	if idx.isMaxed() {
		return io.EOF
	}
	if _, _, err := idx.Read(int64(off)); err == nil {
		return errors.New(fmt.Sprintf("Error: %s", "this offset is arleady existing!!"))
	}
	next_off := uint32(idx.size / entWidth)
	if next_off != off {
		return errors.New(fmt.Sprintf("Error: You are skipping numbers from the expected offset : %d", next_off))
	}
	binary.BigEndian.PutUint32(idx.mmap[idx.size:idx.size+offWidth], off)
	binary.BigEndian.PutUint64(idx.mmap[idx.size+offWidth:idx.size+entWidth], pos)
	idx.size += uint64(entWidth)
	return nil
}

func (idx *index) isMaxed() bool {
	return uint64(len(idx.mmap)) < idx.size+entWidth
}

func (idx *index) Name() string {
	return idx.file.Name()
}
