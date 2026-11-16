package queueconfig

import "fmt"

type Config struct {
	Tenant           string
	Priority         string
	MaxConcurrency   int
	RateCapacity     int
	RateRefillPerSec int
}

func (c Config) Name() string {
	return fmt.Sprintf("queue:%s:%s", c.Tenant, c.Priority)
}

func DefaultConfigs() []Config {
	return []Config{
		{Tenant: "default", Priority: "high", MaxConcurrency: 4, RateCapacity: 20, RateRefillPerSec: 10},
		{Tenant: "default", Priority: "low", MaxConcurrency: 2, RateCapacity: 5, RateRefillPerSec: 2},
		{Tenant: "acme", Priority: "high", MaxConcurrency: 4, RateCapacity: 20, RateRefillPerSec: 10},
		{Tenant: "acme", Priority: "low", MaxConcurrency: 2, RateCapacity: 5, RateRefillPerSec: 2},
	}
}

func ByName(configs []Config) map[string]Config {
	m := make(map[string]Config, len(configs))
	for _, c := range configs {
		m[c.Name()] = c
	}
	return m
}
