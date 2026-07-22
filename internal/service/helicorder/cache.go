package helicorder

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/anyshake/observer/internal/hardware/explorer"
	cachepkg "github.com/anyshake/observer/pkg/cache"
)

var (
	diskCacheHeaderMagic = [8]byte{'H', 'E', 'L', 'I', 'C', 'A', 'C', 1}
	diskCacheFooterMagic = [8]byte{'H', 'E', 'L', 'I', 'I', 'D', 'X', 1}
)

const diskCacheFooterSize = 8 + len(diskCacheFooterMagic)

type queryCacheKey struct {
	StartUnixMilli int64
	EndUnixMilli   int64
}

type queryCacheData struct {
	SampleRate  int
	Timestamp   int64
	ChannelData []explorer.ChannelData
}

type queryCacheRecordConsumer func(sampleRate int, timestamp int64, samples []int32)

type queryCache interface {
	Read(key queryCacheKey, channelCode string, consume queryCacheRecordConsumer) (bool, error)
	NewWriter(key queryCacheKey) (queryCacheWriter, error)
	Clear() error
}

type queryCacheWriter interface {
	Write(record queryCacheData) error
	Commit() error
	Abort()
}

func newQueryCache(storage, cachePath string) (queryCache, error) {
	switch storage {
	case CACHE_STORAGE_DISABLED:
		return nil, nil
	case CACHE_STORAGE_MEMORY:
		return &memoryQueryCache{
			cache: cachepkg.NewKv[[]queryCacheData](time.Hour),
		}, nil
	case CACHE_STORAGE_DISK:
		if cachePath == "" {
			return nil, errors.New("cache path cannot be empty")
		}
		cachePath = filepath.Clean(cachePath)
		if err := os.MkdirAll(cachePath, 0750); err != nil {
			return nil, fmt.Errorf("failed to create disk cache directory: %w", err)
		}
		return &diskQueryCache{path: cachePath}, nil
	default:
		return nil, fmt.Errorf("unsupported cache storage %q", storage)
	}
}

type memoryQueryCache struct {
	cache cachepkg.KvCache[[]queryCacheData]
}

func (c *memoryQueryCache) Read(key queryCacheKey, channelCode string, consume queryCacheRecordConsumer) (bool, error) {
	records, found := c.cache.Get(key)
	if !found {
		return false, nil
	}
	for _, record := range records {
		for i := range record.ChannelData {
			if record.ChannelData[i].ChannelCode == channelCode {
				consume(record.SampleRate, record.Timestamp, record.ChannelData[i].Data)
				break
			}
		}
	}
	return true, nil
}

func (c *memoryQueryCache) NewWriter(key queryCacheKey) (queryCacheWriter, error) {
	return &memoryQueryCacheWriter{cache: c, key: key}, nil
}

func (c *memoryQueryCache) Clear() error {
	c.cache.Clear()
	return nil
}

type memoryQueryCacheWriter struct {
	cache   *memoryQueryCache
	key     queryCacheKey
	records []queryCacheData
}

func (w *memoryQueryCacheWriter) Write(record queryCacheData) error {
	w.records = append(w.records, record)
	return nil
}

func (w *memoryQueryCacheWriter) Commit() error {
	w.cache.cache.Set(w.key, w.records)
	w.records = nil
	return nil
}

func (w *memoryQueryCacheWriter) Abort() {
	w.records = nil
}

type diskQueryCache struct {
	mu   sync.Mutex
	path string
}

func (c *diskQueryCache) Read(key queryCacheKey, channelCode string, consume queryCacheRecordConsumer) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	filePath := c.filePath(key)
	file, err := os.Open(filePath)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to open cache file: %w", err)
	}

	readErr := readDiskCacheChannel(file, channelCode, consume)
	closeErr := file.Close()
	if readErr != nil {
		_ = os.Remove(filePath)
		return false, fmt.Errorf("failed to read cache file: %w", readErr)
	}
	if closeErr != nil {
		return false, fmt.Errorf("failed to close cache file: %w", closeErr)
	}
	return true, nil
}

func (c *diskQueryCache) NewWriter(key queryCacheKey) (queryCacheWriter, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	tempFile, err := os.CreateTemp(c.path, ".span-*.tmp")
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary cache file: %w", err)
	}
	if err := binary.Write(tempFile, binary.LittleEndian, diskCacheHeaderMagic); err != nil {
		_ = tempFile.Close()
		_ = os.Remove(tempFile.Name())
		return nil, fmt.Errorf("failed to write cache header: %w", err)
	}
	return &diskQueryCacheWriter{
		cache:   c,
		key:     key,
		file:    tempFile,
		offsets: make(map[string][]uint64),
	}, nil
}

func (c *diskQueryCache) Clear() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	entries, err := os.ReadDir(c.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to read cache directory: %w", err)
	}

	for _, entry := range entries {
		isCacheFile := strings.HasPrefix(entry.Name(), "span-") && strings.HasSuffix(entry.Name(), ".bin")
		isLegacyCacheFile := strings.HasPrefix(entry.Name(), "span-") && strings.HasSuffix(entry.Name(), ".gob")
		isTempFile := strings.HasPrefix(entry.Name(), ".span-") && strings.HasSuffix(entry.Name(), ".tmp")
		if entry.IsDir() || (!isCacheFile && !isLegacyCacheFile && !isTempFile) {
			continue
		}
		if err := os.Remove(filepath.Join(c.path, entry.Name())); err != nil {
			return fmt.Errorf("failed to remove cache file %s: %w", entry.Name(), err)
		}
	}
	return nil
}

func (c *diskQueryCache) filePath(key queryCacheKey) string {
	return filepath.Join(c.path, fmt.Sprintf("span-%d-%d.bin", key.StartUnixMilli, key.EndUnixMilli))
}

type diskQueryCacheWriter struct {
	cache     *diskQueryCache
	key       queryCacheKey
	file      *os.File
	offsets   map[string][]uint64
	committed bool
}

func (w *diskQueryCacheWriter) Write(record queryCacheData) error {
	if w.file == nil {
		return errors.New("cache writer is closed")
	}
	for _, channel := range record.ChannelData {
		offset, err := w.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("failed to get cache record offset: %w", err)
		}
		if err := writeDiskCacheRecord(w.file, record.SampleRate, record.Timestamp, channel.Data); err != nil {
			return err
		}
		w.offsets[channel.ChannelCode] = append(w.offsets[channel.ChannelCode], uint64(offset))
	}
	return nil
}

func (w *diskQueryCacheWriter) Commit() error {
	if w.file == nil {
		return errors.New("cache writer is closed")
	}

	indexOffset, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		w.Abort()
		return fmt.Errorf("failed to get cache index offset: %w", err)
	}
	if err := writeDiskCacheIndex(w.file, w.offsets); err != nil {
		w.Abort()
		return err
	}
	if err := binary.Write(w.file, binary.LittleEndian, uint64(indexOffset)); err != nil {
		w.Abort()
		return fmt.Errorf("failed to write cache footer offset: %w", err)
	}
	if err := binary.Write(w.file, binary.LittleEndian, diskCacheFooterMagic); err != nil {
		w.Abort()
		return fmt.Errorf("failed to write cache footer: %w", err)
	}

	tempPath := w.file.Name()
	if err := w.file.Close(); err != nil {
		w.file = nil
		_ = os.Remove(tempPath)
		return fmt.Errorf("failed to close cache file: %w", err)
	}
	w.file = nil

	w.cache.mu.Lock()
	err = os.Rename(tempPath, w.cache.filePath(w.key))
	w.cache.mu.Unlock()
	if err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("failed to publish cache file: %w", err)
	}
	w.committed = true
	return nil
}

func (w *diskQueryCacheWriter) Abort() {
	if w.committed || w.file == nil {
		return
	}
	tempPath := w.file.Name()
	_ = w.file.Close()
	w.file = nil
	_ = os.Remove(tempPath)
}

func writeDiskCacheRecord(writer io.Writer, sampleRate int, timestamp int64, samples []int32) error {
	if sampleRate < 0 || uint64(sampleRate) > uint64(^uint32(0)>>1) || uint64(len(samples)) > uint64(^uint32(0)) {
		return errors.New("cache record is too large")
	}
	if err := binary.Write(writer, binary.LittleEndian, timestamp); err != nil {
		return fmt.Errorf("failed to write cache record timestamp: %w", err)
	}
	if err := binary.Write(writer, binary.LittleEndian, int32(sampleRate)); err != nil {
		return fmt.Errorf("failed to write cache record sample rate: %w", err)
	}
	if err := binary.Write(writer, binary.LittleEndian, uint32(len(samples))); err != nil {
		return fmt.Errorf("failed to write cache record sample count: %w", err)
	}
	if err := binary.Write(writer, binary.LittleEndian, samples); err != nil {
		return fmt.Errorf("failed to write cache record samples: %w", err)
	}
	return nil
}

func writeDiskCacheIndex(writer io.Writer, offsets map[string][]uint64) error {
	if len(offsets) > int(^uint16(0)) {
		return errors.New("too many channels in cache index")
	}
	channelCodes := make([]string, 0, len(offsets))
	for channelCode := range offsets {
		channelCodes = append(channelCodes, channelCode)
	}
	sort.Strings(channelCodes)

	if err := binary.Write(writer, binary.LittleEndian, uint16(len(channelCodes))); err != nil {
		return fmt.Errorf("failed to write cache channel count: %w", err)
	}
	for _, channelCode := range channelCodes {
		if len(channelCode) > int(^uint16(0)) || uint64(len(offsets[channelCode])) > uint64(^uint32(0)) {
			return errors.New("cache index entry is too large")
		}
		if err := binary.Write(writer, binary.LittleEndian, uint16(len(channelCode))); err != nil {
			return fmt.Errorf("failed to write cache channel code length: %w", err)
		}
		if _, err := io.WriteString(writer, channelCode); err != nil {
			return fmt.Errorf("failed to write cache channel code: %w", err)
		}
		if err := binary.Write(writer, binary.LittleEndian, uint32(len(offsets[channelCode]))); err != nil {
			return fmt.Errorf("failed to write cache offset count: %w", err)
		}
		if err := binary.Write(writer, binary.LittleEndian, offsets[channelCode]); err != nil {
			return fmt.Errorf("failed to write cache offsets: %w", err)
		}
	}
	return nil
}

func readDiskCacheChannel(file *os.File, channelCode string, consume queryCacheRecordConsumer) error {
	var headerMagic [len(diskCacheHeaderMagic)]byte
	if err := binary.Read(file, binary.LittleEndian, &headerMagic); err != nil {
		return fmt.Errorf("failed to read cache header: %w", err)
	}
	if headerMagic != diskCacheHeaderMagic {
		return errors.New("invalid cache header")
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat cache file: %w", err)
	}
	if fileInfo.Size() < int64(len(diskCacheHeaderMagic)+diskCacheFooterSize) {
		return errors.New("cache file is truncated")
	}
	if _, err := file.Seek(-int64(diskCacheFooterSize), io.SeekEnd); err != nil {
		return fmt.Errorf("failed to seek cache footer: %w", err)
	}

	var indexOffset uint64
	var footerMagic [len(diskCacheFooterMagic)]byte
	if err := binary.Read(file, binary.LittleEndian, &indexOffset); err != nil {
		return fmt.Errorf("failed to read cache index offset: %w", err)
	}
	if err := binary.Read(file, binary.LittleEndian, &footerMagic); err != nil {
		return fmt.Errorf("failed to read cache footer: %w", err)
	}
	if footerMagic != diskCacheFooterMagic || indexOffset >= uint64(fileInfo.Size()-int64(diskCacheFooterSize)) {
		return errors.New("invalid cache footer")
	}

	indexEnd := uint64(fileInfo.Size() - int64(diskCacheFooterSize))
	offsets, err := readDiskCacheOffsets(file, indexOffset, indexEnd, channelCode)
	if err != nil {
		return err
	}
	var samples []int32
	for _, offset := range offsets {
		if offset < uint64(len(diskCacheHeaderMagic)) || offset >= indexOffset {
			return errors.New("invalid cache record offset")
		}
		if _, err := file.Seek(int64(offset), io.SeekStart); err != nil {
			return fmt.Errorf("failed to seek cache record: %w", err)
		}

		var timestamp int64
		var sampleRate int32
		var sampleCount uint32
		if err := binary.Read(file, binary.LittleEndian, &timestamp); err != nil {
			return fmt.Errorf("failed to read cache record timestamp: %w", err)
		}
		if err := binary.Read(file, binary.LittleEndian, &sampleRate); err != nil {
			return fmt.Errorf("failed to read cache record sample rate: %w", err)
		}
		if err := binary.Read(file, binary.LittleEndian, &sampleCount); err != nil {
			return fmt.Errorf("failed to read cache record sample count: %w", err)
		}
		currentOffset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("failed to get cache sample offset: %w", err)
		}
		if uint64(sampleCount) > (indexOffset-uint64(currentOffset))/4 {
			return errors.New("cache record samples exceed data section")
		}
		if uint64(sampleCount) > uint64(^uint(0)>>1) {
			return errors.New("cache record sample count exceeds platform limit")
		}
		if cap(samples) < int(sampleCount) {
			samples = make([]int32, sampleCount)
		} else {
			samples = samples[:sampleCount]
		}
		if err := binary.Read(file, binary.LittleEndian, samples); err != nil {
			return fmt.Errorf("failed to read cache record samples: %w", err)
		}
		consume(int(sampleRate), timestamp, samples)
	}
	return nil
}

func readDiskCacheOffsets(file *os.File, indexOffset, indexEnd uint64, channelCode string) ([]uint64, error) {
	if _, err := file.Seek(int64(indexOffset), io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek cache index: %w", err)
	}
	var channelCount uint16
	if err := binary.Read(file, binary.LittleEndian, &channelCount); err != nil {
		return nil, fmt.Errorf("failed to read cache channel count: %w", err)
	}

	for range channelCount {
		var codeLength uint16
		if err := binary.Read(file, binary.LittleEndian, &codeLength); err != nil {
			return nil, fmt.Errorf("failed to read cache channel code length: %w", err)
		}
		currentOffset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, fmt.Errorf("failed to get cache channel code offset: %w", err)
		}
		if uint64(currentOffset) > indexEnd || uint64(codeLength) > indexEnd-uint64(currentOffset) {
			return nil, errors.New("cache channel code exceeds index section")
		}
		code := make([]byte, codeLength)
		if _, err := io.ReadFull(file, code); err != nil {
			return nil, fmt.Errorf("failed to read cache channel code: %w", err)
		}
		var offsetCount uint32
		if err := binary.Read(file, binary.LittleEndian, &offsetCount); err != nil {
			return nil, fmt.Errorf("failed to read cache offset count: %w", err)
		}
		currentOffset, err = file.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, fmt.Errorf("failed to get cache offsets position: %w", err)
		}
		offsetBytes := uint64(offsetCount) * 8
		if uint64(currentOffset) > indexEnd || offsetBytes > indexEnd-uint64(currentOffset) {
			return nil, errors.New("cache offsets exceed index section")
		}
		if string(code) == channelCode {
			offsets := make([]uint64, offsetCount)
			if err := binary.Read(file, binary.LittleEndian, offsets); err != nil {
				return nil, fmt.Errorf("failed to read cache offsets: %w", err)
			}
			return offsets, nil
		}
		if _, err := file.Seek(int64(offsetCount)*8, io.SeekCurrent); err != nil {
			return nil, fmt.Errorf("failed to skip cache offsets: %w", err)
		}
	}
	return nil, nil
}
