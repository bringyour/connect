package tether

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type ByWgConfig struct {
	Name       string // inferred from config file name
	Address    []string
	ListenPort *int
	PrivateKey string // mandatory
	PreUp      []string
	PostUp     []string
	PreDown    []string
	PostDown   []string
}

func Test() {
	filePath := "/root/connect/tetherctl/bywg0.conf"

	config, err := ParseConfig(filePath)
	if err != nil {
		fmt.Printf("Error parsing config: %v\n", err)
		return
	}

	// Print the parsed config
	fmt.Printf("Parsed Config: %+v\n", config)
}

func ParseConfig(filePath string) (ByWgConfig, error) {
	configName := strings.TrimSuffix(filepath.Base(filePath), filepath.Ext(filePath))

	file, err := os.Open(filePath)
	if err != nil {
		return ByWgConfig{}, fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	// TODO make sure this is a proper name?
	config := ByWgConfig{Name: configName}
	scanner := bufio.NewScanner(file)
	var currentSection string

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Handle section headers
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			currentSection = strings.ToLower(line[1 : len(line)-1])
			continue
		}

		// Handle key-value pairs only in the "interface" section
		if currentSection == "interface" {

			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				key := strings.TrimSpace(parts[0])
				value := strings.TrimSpace(parts[1])

				if idx := strings.Index(value, "#"); idx != -1 {
					value = strings.TrimSpace(value[:idx]) // remove inline comments
				}

				switch key {
				case "Address":
					addresses := strings.Split(value, ",")
					for _, addr := range addresses {
						addr = strings.TrimSpace(addr)
						if addr != "" {
							config.Address = append(config.Address, addr)
						}
					}
				case "ListenPort":
					port, err := strconv.Atoi(value)
					if err != nil {
						return ByWgConfig{}, fmt.Errorf("invalid ListenPort: %w", err)
					}
					config.ListenPort = &port
				case "PrivateKey":
					config.PrivateKey = value
				case "PreUp":
					config.PreUp = append(config.PreUp, value)
				case "PostUp":
					config.PostUp = append(config.PostUp, value)
				case "PreDown":
					config.PreDown = append(config.PreDown, value)
				case "PostDown":
					config.PostDown = append(config.PostDown, value)
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return ByWgConfig{}, fmt.Errorf("error reading file: %w", err)
	}

	if config.PrivateKey == "" {
		return ByWgConfig{}, fmt.Errorf("missing mandatory field PrivateKey")
	}

	return config, nil
}
