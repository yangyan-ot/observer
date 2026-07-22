package helicorder

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/anyshake/observer/internal/hardware/explorer"
)

func TestQueryCacheBackends(t *testing.T) {
	t.Parallel()

	key := queryCacheKey{StartUnixMilli: 1_700_000_000_000, EndUnixMilli: 1_700_000_900_000}
	records := []queryCacheData{
		{
			SampleRate: 100,
			Timestamp:  key.StartUnixMilli,
			ChannelData: []explorer.ChannelData{
				{ChannelCode: "HNE", Data: []int32{1, 2, 3}},
				{ChannelCode: "HNZ", Data: []int32{4, 5, 6}},
			},
		},
		{
			SampleRate: 50,
			Timestamp:  key.StartUnixMilli + 1_000,
			ChannelData: []explorer.ChannelData{
				{ChannelCode: "HNE", Data: []int32{7, 8}},
				{ChannelCode: "HNZ", Data: []int32{9, 10}},
			},
		},
	}
	want := []cachedChannelRecord{
		{SampleRate: 100, Timestamp: key.StartUnixMilli, Samples: []int32{4, 5, 6}},
		{SampleRate: 50, Timestamp: key.StartUnixMilli + 1_000, Samples: []int32{9, 10}},
	}

	tests := []struct {
		name    string
		storage string
		path    string
	}{
		{name: "memory", storage: CACHE_STORAGE_MEMORY},
		{name: "disk", storage: CACHE_STORAGE_DISK, path: filepath.Join(t.TempDir(), "cache")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache, err := newQueryCache(tt.storage, tt.path)
			if err != nil {
				t.Fatalf("newQueryCache() error = %v", err)
			}
			writeCacheRecords(t, cache, key, records)

			got, found, err := readCachedChannel(cache, key, "HNZ")
			if err != nil {
				t.Fatalf("Read() error = %v", err)
			}
			if !found {
				t.Fatal("Read() did not find cached data")
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("Read() = %#v, want %#v", got, want)
			}

			missing, found, err := readCachedChannel(cache, key, "HNN")
			if err != nil {
				t.Fatalf("Read() missing channel error = %v", err)
			}
			if !found || len(missing) != 0 {
				t.Fatalf("missing channel result = %#v, found = %v", missing, found)
			}

			if err := cache.Clear(); err != nil {
				t.Fatalf("Clear() error = %v", err)
			}
			_, found, err = readCachedChannel(cache, key, "HNZ")
			if err != nil {
				t.Fatalf("Read() after Clear() error = %v", err)
			}
			if found {
				t.Fatal("Read() found data after Clear()")
			}
		})
	}
}

func TestDiskQueryCacheClearOnlyRemovesCacheFiles(t *testing.T) {
	t.Parallel()

	cachePath := filepath.Join(t.TempDir(), "cache")
	cache, err := newQueryCache(CACHE_STORAGE_DISK, cachePath)
	if err != nil {
		t.Fatalf("newQueryCache() error = %v", err)
	}

	unrelatedPath := filepath.Join(cachePath, "keep.txt")
	if err := os.WriteFile(unrelatedPath, []byte("keep"), 0600); err != nil {
		t.Fatalf("failed to create unrelated file: %v", err)
	}
	tempPath := filepath.Join(cachePath, ".span-interrupted.tmp")
	if err := os.WriteFile(tempPath, []byte("partial"), 0600); err != nil {
		t.Fatalf("failed to create interrupted cache file: %v", err)
	}

	if err := cache.Clear(); err != nil {
		t.Fatalf("Clear() error = %v", err)
	}
	if _, err := os.Stat(unrelatedPath); err != nil {
		t.Fatalf("Clear() removed unrelated file: %v", err)
	}
	if _, err := os.Stat(tempPath); !os.IsNotExist(err) {
		t.Fatalf("Clear() left temporary cache file, stat error = %v", err)
	}
}

func TestDiskQueryCacheRejectsCorruptOffsets(t *testing.T) {
	t.Parallel()

	cachePath := t.TempDir()
	key := queryCacheKey{StartUnixMilli: 1, EndUnixMilli: 2}
	cacheValue, err := newQueryCache(CACHE_STORAGE_DISK, cachePath)
	if err != nil {
		t.Fatalf("newQueryCache() error = %v", err)
	}
	cache := cacheValue.(*diskQueryCache)
	if err := os.WriteFile(cache.filePath(key), []byte("not a cache"), 0600); err != nil {
		t.Fatalf("failed to write corrupt cache: %v", err)
	}

	if _, err := cache.Read(key, "HNZ", func(int, int64, []int32) {}); err == nil {
		t.Fatal("Read() accepted a corrupt cache file")
	}
	if _, err := os.Stat(cache.filePath(key)); !os.IsNotExist(err) {
		t.Fatalf("corrupt cache file was not removed, stat error = %v", err)
	}
}

func TestNewQueryCacheStorageModes(t *testing.T) {
	t.Parallel()

	cache, err := newQueryCache(CACHE_STORAGE_DISABLED, "")
	if err != nil {
		t.Fatalf("disabled cache returned error: %v", err)
	}
	if cache != nil {
		t.Fatalf("disabled cache = %#v, want nil", cache)
	}

	if _, err := newQueryCache("invalid", ""); err == nil {
		t.Fatal("invalid cache storage did not return an error")
	}
}

func TestCacheStorageConfigOptions(t *testing.T) {
	t.Parallel()

	config := &helicorderConfigCacheStorageImpl{}
	if config.GetDefaultValue() != CACHE_STORAGE_DISABLED {
		t.Fatalf("default cache storage = %v, want %q", config.GetDefaultValue(), CACHE_STORAGE_DISABLED)
	}

	options := config.GetOptions()
	for _, storage := range []string{CACHE_STORAGE_DISABLED, CACHE_STORAGE_DISK, CACHE_STORAGE_MEMORY} {
		found := false
		for _, value := range options {
			if value == storage {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("cache storage option %q is missing", storage)
		}
	}
}

func TestCachePathConfig(t *testing.T) {
	t.Parallel()

	config := &helicorderConfigCachePathImpl{}
	if config.GetKey() != "cache_path" {
		t.Fatalf("cache path key = %q, want %q", config.GetKey(), "cache_path")
	}
	if config.GetDefaultValue() != CACHE_DEFAULT_PATH {
		t.Fatalf("default cache path = %v, want %q", config.GetDefaultValue(), CACHE_DEFAULT_PATH)
	}

	found := false
	for _, constraint := range (&HelicorderServiceImpl{}).GetConfigConstraint() {
		if constraint.GetKey() == config.GetKey() {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("cache path constraint is not registered")
	}
}

type cachedChannelRecord struct {
	SampleRate int
	Timestamp  int64
	Samples    []int32
}

func writeCacheRecords(t *testing.T, cache queryCache, key queryCacheKey, records []queryCacheData) {
	t.Helper()
	writer, err := cache.NewWriter(key)
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}
	defer writer.Abort()
	for _, record := range records {
		if err := writer.Write(record); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}
	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
}

func readCachedChannel(cache queryCache, key queryCacheKey, channelCode string) ([]cachedChannelRecord, bool, error) {
	var records []cachedChannelRecord
	found, err := cache.Read(key, channelCode, func(sampleRate int, timestamp int64, samples []int32) {
		records = append(records, cachedChannelRecord{
			SampleRate: sampleRate,
			Timestamp:  timestamp,
			Samples:    append([]int32(nil), samples...),
		})
	})
	return records, found, err
}
