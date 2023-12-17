package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = [][]byte{
		
	}
)

func TestStoreAppendRead(t *testing.T) {

	f, err := os.CreateTemp("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	store, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, store)
	testRead(t, store)
	testReadAt(t, store)

	store, err = newStore(f)
	require.NoError(t, err)
	testRead(t, store)
}

func testAppend(t *testing.T, s *store) {

	t.Helper()
	var pool uint64
	for _, testCase := range write {
		test_size := uint64(len(testCase))
		n, pos, err := s.Append(testCase)
		require.NoError(t, err)
		require.Equal(t, pos+n, pool+test_size+lenWidth)
		pool += test_size + lenWidth
	}
}

func testRead(t *testing.T, s *store) {

	t.Helper()
	var pos uint64
	for i, testCase := range write {
		read, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, write[i], read)
		pos += lenWidth + uint64(len(testCase))
	}
}

func testReadAt(t *testing.T, s *store) {

	t.Helper()
	var off int64
	for _, testCase := range write {
		buf := make([]byte, lenWidth)
		read_size, err := s.ReadAt(buf, off)
		require.NoError(t, err)
		require.Equal(t, lenWidth, read_size)
		off += int64(read_size)

		record_size := enc.Uint64(buf)
		buf = make([]byte, record_size)
		read_size, err = s.ReadAt(buf, off)
		require.NoError(t, err)
		require.Equal(t, testCase, buf)
		require.Equal(t, int(record_size), read_size)
		off += int64(read_size)
	}
}
