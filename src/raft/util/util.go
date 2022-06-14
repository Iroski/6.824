package util

import (
	jsoniter "github.com/json-iterator/go"
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func ToJson(info interface{}) string {
	result, _ := json.MarshalToString(info)
	return result
}
