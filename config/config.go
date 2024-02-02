package config

type ServerProperties struct {
	// for public configuration
	Databases int `cfg:"databases"`
}

var Properties *ServerProperties
