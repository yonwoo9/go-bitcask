Translations: [English](README.md) | [简体中文](README-zh.md)

⚠️ 大部分代码由 Claude Sonnet 3.5 和 GitHub Copilot 生成，因此可能存在错误，请谨慎使用。


# go-bitcask

go-bitcask 是一个用 Go 语言编写的简单、快速且高效的键值存储。它的设计灵感来自 Riak 中使用的 Bitcask 存储模型(https://riak.com/assets/bitcask-intro.pdf)。

## 特性

- 简单的 API 用于存储和检索键值对
- 批量操作以提高批量写入和读取的效率
- 数据压缩支持
- 定期数据文件合并
- 快照创建用于备份
- 迭代器用于遍历键

## 安装

使用 `go get` 安装 go-bitcask：

```sh
go get github.com/yonwoo9/go-bitcask
```

## 使用
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

	// 存储一个键值对
	if err = db.Put("key1", []byte("value1")); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("存储 key1 成功")

	// 获取键对应的值
	value, err := db.Get("key1")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("获取 key1:", string(value))

	// 批量存储键值对
	batch := map[string][]byte{
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}
	if err = db.BatchPut(batch); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("批量存储成功")

	// 批量获取键对应的值
	keys := []string{"key2", "key3"}
	values, err := db.BatchGet(keys)
	if err != nil {
		fmt.Println(err)
		return
	}
	for k, v := range values {
		fmt.Printf("批量获取 key:%s, val:%s\n", k, string(v))
	}

	// 删除一个键
	if err = db.Delete("key1"); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("删除 key1 成功")

	// 遍历键
	iterator := db.Iterator()
	for iterator.Next() {
		key := iterator.Key()
		value, err := iterator.Value()
		if err != nil {
			fmt.Println(err)
			continue
		}
		fmt.Printf("迭代器 key:%s, val:%s\n", key, string(value))
	}
}
```
## API
go-bitcask 的 API 文档如下：
```go
func Open(dir string, opts ...ConfOption) (*Bitcask, error)
```
Open 函数用于打开一个 Bitcask 数据库，并返回一个 Bitcask 实例。
```go
func (b *Bitcask) Put(key string, value []byte) error
```
Put 函数用于将一个键值对存储到 Bitcask 数据库中。
```go
func (b *Bitcask) Get(key string) ([]byte, error)
```
Get 函数用于从 Bitcask 数据库中获取一个键对应的值。
```go
func (b *Bitcask) BatchPut(batch map[string][]byte) error
```
BatchPut 函数用于将多个键值对存储到 Bitcask 数据库中。
```go
func (b *Bitcask) BatchGet(keys []string) (map[string][]byte,error)
```
BatchGet 函数用于从 Bitcask 数据库中获取多个键对应的值。
```go
func (b *Bitcask) Delete(key string) error
```
Delete 函数用于从 Bitcask 数据库中删除一个键。
```go
func (b *Bitcask) Iterator() *Iterator
```
Iterator 函数用于创建一个迭代器，用于遍历 Bitcask 数据库中的键。
```go
func (b *Bitcask) Close() error
```
Close 函数用于关闭 Bitcask 数据库。
## 许可证
go-bitcask 采用 MIT 许可证。
