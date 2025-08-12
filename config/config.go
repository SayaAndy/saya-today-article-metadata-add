package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/go-playground/validator/v10"
)

type Config struct {
	LogLevel          slog.Level    `json:"LogLevel" validate:"required"`
	Storage           StorageConfig `json:"Storage" validate:"required"`
	MaxConcurrentJobs int           `json:"MaxConcurrentJobs" validate:"required,min=1"`
}

type StorageConfig struct {
	Type   string `json:"Type" validate:"required,oneof=b2"`
	Config any    `json:"Config" validate:"required"`
}

func (sc *StorageConfig) UnmarshalJSON(data []byte) error {
	var tmp struct {
		Type   string          `json:"Type"`
		Config json.RawMessage `json:"Config"`
	}

	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	sc.Type = tmp.Type

	switch tmp.Type {
	case "b2":
		var b2Config B2Config
		if err := json.Unmarshal(tmp.Config, &b2Config); err != nil {
			return fmt.Errorf("unmarshal B2Config: %w", err)
		}
		sc.Config = &b2Config
	default:
		return fmt.Errorf("unsupported storage type: %s", tmp.Type)
	}

	return nil
}

type B2Config struct {
	BucketName     string `json:"BucketName" validate:"required,min=1"`
	Region         string `json:"Region" validate:"required,min=1"`
	Prefix         string `json:"Prefix"`
	KeyID          string `json:"KeyID"`
	ApplicationKey string `json:"ApplicationKey"`
}

func LoadConfig(path string, config *Config) error {
	fileBytes, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	expandedFileBytes := []byte(os.ExpandEnv(string(fileBytes)))

	if err = json.Unmarshal(expandedFileBytes, config); err != nil {
		return err
	}

	return nil
}

func InitConfig(path string) (*Config, error) {
	config := &Config{}
	if err := LoadConfig(path, config); err != nil {
		return nil, err
	}

	validate := validator.New(validator.WithRequiredStructEnabled())
	if err := validate.Struct(config); err != nil {
		return nil, err
	}

	return config, nil
}
