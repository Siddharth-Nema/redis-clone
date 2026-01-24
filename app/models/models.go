package models

type Waiter struct {
	Ch chan Result
}

type Result struct {
	Val string
	Ok  bool
}
