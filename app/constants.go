package main

import (
	"math"

	"github.com/codecrafters-io/redis-starter-go/app/models"
)

var MaxStreamID = models.StreamEntryID{
	Time: math.MaxInt64,
	Seq:  math.MaxInt64,
}

var MinStreamID = models.StreamEntryID{
	Time: 0,
	Seq:  0,
}
