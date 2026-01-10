package main

type waiter struct {
	ch chan result
}

type result struct {
	val string
	ok  bool
}

type listState struct {
	items []string
}
