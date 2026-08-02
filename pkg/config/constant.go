package config

import (
	"time"

	"github.com/fatih/color"
)

const (
	VersionString = "v2.6.21"

	SpinnerIndex    = 9
	SpinnerDuration = 77 * time.Millisecond

	Repository = "https://github.com/ad3n/kmt.git"

	ConfigFile = "Kmtfile.yml"
)

var (
	BoldColor    = color.New(color.Bold)
	ErrorColor   = color.New(color.FgRed)
	SuccessColor = color.New(color.FgGreen)
)
