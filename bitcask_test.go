package bitcask

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
)

func TestConcurrentOperations(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcask-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const numOps = 1000
	const numGoroutines = 10

	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				value := []byte(fmt.Sprintf("value-%d-%d", id, j))

				err := db.Put(key, value)
				if err != nil {
					t.Errorf("Put failed: %v", err)
				}

				_, err = db.Get(key)
				if err != nil {
					t.Errorf("Get failed: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()
}

func BenchmarkPut(b *testing.B) {
	dir, err := os.MkdirTemp("", "bitcask-bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := db.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	dir, err := os.MkdirTemp("", "bitcask-bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := db.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%10000)
		_, err := db.Get(key)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcask-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const numOps = 10000
	const numReaders = 5
	const numWriters = 3

	var wg sync.WaitGroup

	// Writers
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				value := []byte(fmt.Sprintf("value-%d-%d", id, j))
				if err := db.Put(key, value); err != nil {
					t.Errorf("Put failed: %v", err)
				}
			}
		}(i)
	}

	// Readers
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				writerID := j % numWriters
				key := fmt.Sprintf("key-%d-%d", writerID, j)
				_, err := db.Get(key)
				if err != nil && !errors.Is(err, ErrKeyNotFound) {
					t.Errorf("Get failed: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestRecovery(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcask-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// First session: write some data
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := db.Put(key, value); err != nil {
			t.Fatal(err)
		}
	}

	// TODO Simulate a crash by not calling Close()
	//  when set db = nil will be failed
	db.Close()

	// Second session: recover and verify data
	db, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		expectedValue := []byte(fmt.Sprintf("value-%d", i))
		value, err := db.Get(key)
		if err != nil {
			t.Errorf("Get failed for key %s: %v", key, err)
		}
		if !bytes.Equal(value, expectedValue) {
			t.Errorf("Unexpected value for key %s. Got %s, want %s", key, value, expectedValue)
		}
		//t.Logf("Got value %s for key %s", string(value), key)
	}
}

func TestDataConsistency(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcask-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const numOps = 10000
	const numGoroutines = 5

	keyValues := make(map[string][]byte)
	var mu sync.Mutex

	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				value := []byte(fmt.Sprintf("value-%d-%d", id, j))

				if err := db.Put(key, value); err != nil {
					t.Errorf("Put failed: %v", err)
				}

				mu.Lock()
				keyValues[key] = value
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// Verify all key-value pairs
	for key, expectedValue := range keyValues {
		value, err := db.Get(key)
		if err != nil {
			t.Errorf("Get failed for key %s: %v", key, err)
		}
		if !bytes.Equal(value, expectedValue) {
			t.Errorf("Unexpected value for key %s. Got %s, want %s", key, value, expectedValue)
		}
	}
}
