package redis

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	DefaultExpiration = 5 * time.Millisecond
)

// Client represents a Redis client wrapper
type Client struct {
	//client *redis.Client
	client *redis.ClusterClient
}

// Config holds Redis connection configuration
type Config struct {
	Host     string
	Port     string
	Password string
	Database int
}

// ConnectionInfo holds parsed connection string information
type ConnectionInfo struct {
	Config      Config
	UseTLS      bool
	SSLCertReqs string
}

// ParseConnectionString parses Redis connection strings and detects connection type
func ParseConnectionString(connectionString string) (*ConnectionInfo, error) {
	parsedURL, err := url.Parse(connectionString)
	if err != nil {
		return nil, fmt.Errorf("invalid connection string format: %w", err)
	}

	info := &ConnectionInfo{}

	// Determine if TLS should be used based on scheme
	switch parsedURL.Scheme {
	case "rediss":
		info.UseTLS = true
	case "redis":
		info.UseTLS = false
	default:
		return nil, fmt.Errorf("unsupported scheme: %s (use 'redis' or 'rediss')", parsedURL.Scheme)
	}

	// Extract host and port
	host := parsedURL.Hostname()
	port := parsedURL.Port()

	if host == "" {
		return nil, fmt.Errorf("host is required in connection string")
	}

	// Set default ports if not specified
	if port == "" {
		if info.UseTLS {
			port = "6380" // Default SSL port
		} else {
			port = "6379" // Default Redis port
		}
	}

	info.Config.Host = host
	info.Config.Port = port

	// Extract password
	if parsedURL.User != nil {
		password, _ := parsedURL.User.Password()
		info.Config.Password = password
	}

	// Extract database number from path
	database := 0
	if parsedURL.Path != "" && parsedURL.Path != "/" {
		dbStr := strings.TrimPrefix(parsedURL.Path, "/")
		if db, err := strconv.Atoi(dbStr); err == nil {
			database = db
		}
	}
	info.Config.Database = database

	// Parse query parameters
	queryParams := parsedURL.Query()
	if sslCertReqs := queryParams.Get("ssl_cert_reqs"); sslCertReqs != "" {
		info.SSLCertReqs = sslCertReqs
	}

	return info, nil
}

// isAzureRedis detects if the host is an Azure Redis Cache instance
func isAzureRedis(host string) bool {
	azurePatterns := []string{
		".redis.cache.windows.net",
		".redis.cache.chinacloudapi.cn",
		".redis.cache.usgovcloudapi.net",
		".redis.cache.cloudapi.de",
	}

	hostLower := strings.ToLower(host)
	for _, pattern := range azurePatterns {
		if strings.Contains(hostLower, pattern) {
			return true
		}
	}
	return false
}

// isLocalhost detects if the host is a local Redis instance
func isLocalhost(host string) bool {
	localPatterns := []string{
		"localhost",
		"127.0.0.1",
		"::1",
	}

	hostLower := strings.ToLower(host)
	for _, pattern := range localPatterns {
		if hostLower == pattern {
			return true
		}
	}
	return false
}

// NewClientFromConnectionString creates a Redis client from connection string
func NewClientFromConnectionString(connectionString string) (*Client, error) {
	info, err := ParseConnectionString(connectionString)
	if err != nil {
		return nil, err
	}

	return NewClientFromConnectionInfo(info), nil
}

// NewClientFromConnectionInfo creates a Redis client from parsed connection info
func NewClientFromConnectionInfo(info *ConnectionInfo) *Client {
	//options := &redis.Options{
	options := &redis.ClusterOptions{
		Addrs:    []string{fmt.Sprintf("%s:%s", info.Config.Host, info.Config.Port)},
		Password: info.Config.Password,
		//DB:       info.Config.Database,
	}

	// Configure TLS if required
	if info.UseTLS {
		tlsConfig := &tls.Config{
			ServerName: info.Config.Host,
			MinVersion: tls.VersionTLS12,
		}

		// Handle ssl_cert_reqs parameter
		if info.SSLCertReqs == "none" {
			tlsConfig.InsecureSkipVerify = true
		}

		options.TLSConfig = tlsConfig

		if isAzureRedis(info.Config.Host) {
			log.Printf("Detected Azure Redis Cache: %s (TLS enabled)\n", info.Config.Host)
		} else {
			log.Printf("Detected Redis with TLS: %s (TLS enabled)\n", info.Config.Host)
		}
	} else {
		if isLocalhost(info.Config.Host) {
			log.Printf("Detected local Redis: %s (no TLS)\n", info.Config.Host)
		} else {
			log.Printf("Detected remote Redis: %s (no TLS)\n", info.Config.Host)
		}
	}

	//rdb := redis.NewClient(options)
	rdb := redis.NewClusterClient(options)

	return &Client{
		client: rdb,
	}
}

// NewClient creates a new Redis client (legacy method for backward compatibility)
func NewClient(config Config) *Client {
	info := &ConnectionInfo{
		Config: config,
		UseTLS: false,
	}

	// Auto-detect if TLS should be used
	if isAzureRedis(config.Host) || config.Port == "6380" {
		info.UseTLS = true
	}

	return NewClientFromConnectionInfo(info)
}

// Connect tests the connection to Redis server
func (c *Client) Connect() error {
	// Use longer timeout for potential remote connections
	timeout := 5 * time.Second
	if c.isRemoteConnection() {
		timeout = 10 * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, err := c.client.Ping(ctx).Result()
	if err != nil {
		return fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return nil
}

// isRemoteConnection checks if this is likely a remote connection
func (c *Client) isRemoteConnection() bool {
	addr := c.client.Options().Addrs
	host := strings.Split(addr[0], ":")[0]
	return !isLocalhost(host)
}

// Get retrieves a value from Redis by key
func (c *Client) Get(key string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultExpiration)
	defer cancel()

	val, err := c.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", fmt.Errorf("key '%s' not found", key)
		}
		return "", fmt.Errorf("failed to get key '%s': %w", key, err)
	}

	return val, nil
}

// Set stores a value in Redis with optional expiration
func (c *Client) Set(key, value string, expiration time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultExpiration)
	defer cancel()

	err := c.client.Set(ctx, key, value, expiration).Err()
	if err != nil {
		return fmt.Errorf("failed to set key '%s': %w", key, err)
	}

	return nil
}

// Close closes the Redis connection
func (c *Client) Close() error {
	return c.client.Close()
}

// IsConnected checks if the Redis client is connected
func (c *Client) IsConnected() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := c.client.Ping(ctx).Result()
	return err == nil
}

// GetConnectionInfo returns information about the current connection
func (c *Client) GetConnectionInfo() string {
	addr := c.client.Options().Addrs[0]
	tlsEnabled := c.client.Options().TLSConfig != nil
	host := strings.Split(addr, ":")[0]

	connectionType := "Remote Redis"
	if isAzureRedis(host) {
		connectionType = "Azure Redis Cache"
	} else if isLocalhost(host) {
		connectionType = "Local Redis"
	}

	return fmt.Sprintf("%s: %s (TLS: %t)", connectionType, addr, tlsEnabled)
}
