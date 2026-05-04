package config

import (
	"time"

	"github.com/fatih/color"
)

const (
	VERSION_STRING = "v2.3.0"

	SPINER_INDEX    = 9
	SPINER_DURATION = 77 * time.Millisecond

	REPOSITORY = "https://github.com/ad3n/kmt.git"

	CONFIG_FILE = "Kmtfile.yml"
)

var (
	BoldColor    = color.New(color.Bold)
	ErrorColor   = color.New(color.FgRed)
	SuccessColor = color.New(color.FgGreen)
)
