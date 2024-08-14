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
