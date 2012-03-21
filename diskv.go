package diskv

type Key string
type Value []byte

type Store interface {
	Read(k Key) (Value, error)
	Write(k Key, v Value) error
	Erase(k Key) error
	Flush() error
}

type TransformFunc func(k Key) []string
