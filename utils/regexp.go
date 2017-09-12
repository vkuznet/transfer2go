package utils

// transfer2go/utils - regexp for transfer2go
//
// Author: Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"regexp"
)

// PatternUrl represents http pattern
var PatternUrl = regexp.MustCompile("(https|http)://[-A-Za-z0-9_+&@#/%?=~_|!:,.;]*[-A-Za-z0-9+&@#/%=~_|]")
