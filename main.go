package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Println("system service is ready to read from Kafka...")
	host := GetEnvParam("KAFKA_HOST", "localhost")
	port := GetEnvParam("KAFKA_PORT", "3888")

	fmt.Printf("Kafka host:%s , ,port:%s \n", host, port)
}

// GetEnvParam : return string environmental param if exists, otherwise return default
func GetEnvParam(param string, dflt string) string {
	if v, exists := os.LookupEnv(param); exists {
		return v
	}
	return dflt
}
