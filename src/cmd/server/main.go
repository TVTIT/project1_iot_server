package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	_ "github.com/golang-jwt/jwt/v5"
	_ "github.com/google/uuid"
	_ "github.com/gorilla/websocket"
	_ "github.com/jackc/pgx/v5"
)

func main() {
	port := os.Getenv("SERVER_PORT")
	if port == "" {
		port = "8080"
	}

	env := os.Getenv("SERVER_ENV")
	if env == "production" {
		gin.SetMode(gin.ReleaseMode)
	} else {
		gin.SetMode(gin.DebugMode)
	}

	router := gin.Default()

	// Health check endpoints
	router.GET("/healthz", func(c *gin.Context) {
		c.String(http.StatusOK, "OK")
	})

	// V1 API Group
	v1 := router.Group("/v1")
	{
		v1.GET("/health", func(c *gin.Context) {
			c.JSON(http.StatusOK, gin.H{
				"status":  "running",
				"service": "iot-backend",
				"version": "v1",
			})
		})

		// Telemetry REST API
		v1.GET("/telemetry/history", func(c *gin.Context) {
			c.JSON(http.StatusOK, gin.H{
				"message": "telemetry history endpoint placeholder",
			})
		})

		// Realtime WebSocket Endpoint
		v1.GET("/ws", func(c *gin.Context) {
			c.JSON(http.StatusOK, gin.H{
				"message": "websocket endpoint placeholder",
			})
		})

		// Digital Twin Endpoints (Section 10.8 of AGENTS.md)
		v1.GET("/digital-twins", func(c *gin.Context) {
			c.JSON(http.StatusOK, gin.H{
				"message": "digital twins list endpoint placeholder",
			})
		})
	}

	log.Printf("Gin Server starting on port %s...", port)
	if err := router.Run(fmt.Sprintf(":%s", port)); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
