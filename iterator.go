package bitcask

type Iterator struct {
	bitcask *Bitcask
	keys    []string
	index   int
}

// Next advances the iterator to the next key-value pair.
func (it *Iterator) Next() bool {
	it.index++
	return it.index < len(it.keys)
}

// Key returns the key of the current key-value pair.
func (it *Iterator) Key() string {
	return it.keys[it.index]
}

// Value returns the value of the current key-value pair.
func (it *Iterator) Value() ([]byte, error) {
	return it.bitcask.Get(it.Key())
}
