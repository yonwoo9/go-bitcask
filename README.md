##### Translate to: [简体中文](README-zh.md)

⚠️ Most of the code was generated by Claude Sonnet 3.5 and GitHub Copilot, so there may be errors. Please use with caution.

# go-bitcask

go-bitcask is a simple, fast, and efficient key-value store written in Go. It is inspired by the Bitcask storage model used in Riak(https://riak.com/assets/bitcask-intro.pdf).


## Features

- Simple API for storing and retrieving key-value pairs
- Batch operations for efficient bulk writes and reads
- Data compression support
- Periodic data file merging
- Snapshot creation for backups
- Iterator for traversing keys

## Installation

To install go-Bitcask, use `go get`:

```sh
go get github.com/yonwoo9/go-bitcask
```

## Usage
Here is a simple example of how to use go-bitcask:

```go
package main

import (
    "fmt"
    "github.com/yonwoo9/go-bitcask"
)

func main() {
    db, err := bitcask.Open("test")
    if err != nil {
    panic(err)
}
defer db.Close()

	// Put a key-value pair
	if err = db.Put("key1", []byte("value1")); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Put key1 success")

	// Get the value for a key
	value, err := db.Get("key1")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Get key1:", string(value))

	// Batch put key-value pairs
	batch := map[string][]byte{
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}
	if err = db.BatchPut(batch); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Batch put success")

	// Batch get values for keys
	keys := []string{"key2", "key3"}
	values, err := db.BatchGet(keys)
	if err != nil {
		fmt.Println(err)
		return
	}
	for k, v := range values {
		fmt.Printf("Batch get key:%s, val:%s\n", k, string(v))
	}

	// Delete a key
	if err = db.Delete("key1"); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Delete key1 success")

	// Iterate over keys
	iterator := db.Iterator()
	for iterator.Next() {
		key := iterator.Key()
		value, err := iterator.Value()
		if err != nil {
			fmt.Println(err)
			continue
		}
		fmt.Printf("Iterator key:%s, val:%s\n", key, string(value))
	}
}
```

## API
Here is the API documentation for go-bitcask:
```go
func Open(dir string, opts ...ConfOption) (*Bitcask, error)
```
Opens a Bitcask database in the specified directory with optional configuration options.

```go
func (b *Bitcask) Put(key string, value []byte) error
```
Stores a key-value pair in the Bitcask database.

```go
func (b *Bitcask) Get(key string) ([]byte, error)
```
Retrieves the value associated with the specified key from the Bitcask database.
```go
func (b *Bitcask) BatchPut(batch map[string][]byte) error
```
Stores multiple key-value pairs in the Bitcask database in a batch operation.
```go
func (b *Bitcask) BatchGet(keys []string) (map[string][]byte,
error)
```
Retrieves the values associated with multiple keys from the Bitcask database in a batch operation.
```go
func (b *Bitcask) Delete(key string) error
```
Deletes the specified key from the Bitcask database.
```go
func (b *Bitcask) Iterator() *Iterator
```
Creates an iterator over the keys in the Bitcask database.
```go
func (b *Bitcask) Close() error
```
Closes the Bitcask database and releases any resources associated with it.

## License
go-bitcask is licensed under the MIT License. See the LICENSE file for details.