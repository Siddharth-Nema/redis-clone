package models

type Waiter struct {
	Ch chan Result
}

type Result struct {
	Val string
	Ok  bool
}

type StringEntry struct {
	Key     string
	Val     string
	ExpInMs int64
}
