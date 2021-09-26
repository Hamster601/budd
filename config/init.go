package config

import (
	"io/ioutil"

	"github.com/BurntSushi/toml"
)

// NewConfig creates a Config from data.
func newConfig(data string) (*Config, error) {
	var c Config

	_, err := toml.Decode(data, &c)
	if err != nil {
		return nil, err
	}

	return &c, nil
}

// Read config file
func NewConfigWithFile(path string) (*Config, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return newConfig(string(data))
}
