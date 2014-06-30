package util

import (
	"strconv"
)

func IntToBytes(i int) (bytes []byte, err error) {
	bytes = []byte(strconv.Itoa(i))
	return bytes, err
}

func Int64ToBytes(i int64) (bytes []byte, err error) {
	bytes = []byte(strconv.FormatInt(i, 10))
	return bytes, err
}

func ByteToInt(b []byte) (n int, err error) {
	m, err := strconv.Atoi(string(b))
	if err != nil {
		return 0, err
	}
	return int(m), err
}

func ByteToInt64(b []byte) (n int64, err error) {
	n, err = strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, err
	}
	return n, err
}
