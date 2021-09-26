package config

import "testing"

func TestNewConfigWithFile(t *testing.T) {
	filePath := "../config.toml"
	config ,err := NewConfigWithFile(filePath)
	if err != nil {
		t.Fatal(err.Error())
	}
	t.Log(config)
}
