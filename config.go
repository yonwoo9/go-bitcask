package bitcask

import "time"

type ConfOption func(*Config)

// Config is the configuration for a Bitcask instance.
type Config struct {
	MaxFileSize    int64
	MergeThreshold int
	SyncWrites     bool
	CompressData   bool
	MergeInterval  time.Duration
}

// DefaultMaxDatafileSize is the default maximum size of a datafile.
const DefaultMaxDatafileSize = 1020 * 1024 * 10

// MaxDatafileSize sets the maximum size of a datafile.
func MaxDatafileSize(size int64) ConfOption {
	return func(c *Config) {
		c.MaxFileSize = size
	}
}

// MergeThreshold sets the threshold for merging datafiles.
func MergeThreshold(threshold int) ConfOption {
	return func(c *Config) {
		c.MergeThreshold = threshold
	}
}

// SyncWrites sets whether to sync writes to disk.
func SyncWrites(sync bool) ConfOption {
	return func(c *Config) {
		c.SyncWrites = sync
	}
}

// CompressData sets whether to compress data.
func CompressData(compress bool) ConfOption {
	return func(c *Config) {
		c.CompressData = compress
	}
}

// MergeInterval sets the interval for merging datafiles.
func MergeInterval(interval time.Duration) ConfOption {
	return func(c *Config) {
		c.MergeInterval = interval
	}
}

// DefaultConfig returns the default configuration.
func DefaultConfig() *Config {
	return &Config{
		MaxFileSize:    DefaultMaxDatafileSize,
		MergeThreshold: 10,
		SyncWrites:     false,
		CompressData:   false,
		MergeInterval:  time.Minute * 10,
	}
}
