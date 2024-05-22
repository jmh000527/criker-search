package utils

import (
	"log"
	"os"
)

var Log = log.New(os.Stdout, "[radic] ", log.Lshortfile|log.Ldate|log.Ltime)
