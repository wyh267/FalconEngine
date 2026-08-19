// Package config 负责加载与校验 falcon 节点的 YAML 配置。
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config 节点配置
type Config struct {
	Node    NodeConfig    `yaml:"node"`
	HTTP    HTTPConfig    `yaml:"http"`
	GRPC    GRPCConfig    `yaml:"grpc"`
	Data    DataConfig    `yaml:"data"`
	Cluster ClusterConfig `yaml:"cluster"`
	Log     LogConfig     `yaml:"log"`
}

type NodeConfig struct {
	Name   string `yaml:"name"`   // 节点名称，默认 hostname
	Master bool   `yaml:"master"` // 是否参与控制面 raft
	Data   bool   `yaml:"data"`   // 是否持有分片数据
}

type HTTPConfig struct {
	Port int `yaml:"port"` // REST API 端口，默认 9990
}

type GRPCConfig struct {
	Port int `yaml:"port"` // 节点间通信端口，默认 9991
}

type DataConfig struct {
	Path string `yaml:"path"` // 数据目录，默认 ./data
}

type ClusterConfig struct {
	Name  string   `yaml:"name"`  // 集群名称，默认 falcon
	Seeds []string `yaml:"seeds"` // 种子节点地址（grpc host:port），单机可为空
}

type LogConfig struct {
	Level string `yaml:"level"` // debug/info/warn/error
}

// Default 返回默认配置
func Default() *Config {
	return &Config{
		Node:    NodeConfig{Name: "", Master: true, Data: true},
		HTTP:    HTTPConfig{Port: 9990},
		GRPC:    GRPCConfig{Port: 9991},
		Data:    DataConfig{Path: "./data"},
		Cluster: ClusterConfig{Name: "falcon"},
		Log:     LogConfig{Level: "info"},
	}
}

// Load 从文件加载配置，文件不存在时返回默认配置
func Load(path string) (*Config, error) {
	cfg := Default()
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read config %s: %w", path, err)
		}
		if err := yaml.Unmarshal(b, cfg); err != nil {
			return nil, fmt.Errorf("parse config %s: %w", path, err)
		}
	}
	if cfg.Node.Name == "" {
		host, _ := os.Hostname()
		cfg.Node.Name = host
	}
	return cfg, nil
}
