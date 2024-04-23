package main

import (
	"fmt"
	"github.com/golang-jwt/jwt/v4"
	"os"
	"time"
)

func main() {

	privateKeyPath := "private_key.pem"
	privateKeyPEM, err := os.ReadFile(privateKeyPath)
	if err != nil {
		fmt.Println("Error reading private key file:", err)
		return
	}

	// Parse the PEM-encoded private key
	privateKey, err := jwt.ParseRSAPrivateKeyFromPEM(privateKeyPEM)
	if err != nil {
		fmt.Println("Error parsing private key:", err)
		return
	}

	// 100 years
	expirationTime := time.Now().Add(24 * 100 * 365 * time.Hour)

	// Subject must be 'test'
	claims := jwt.RegisteredClaims{
		ExpiresAt: jwt.NewNumericDate(expirationTime),
		Issuer:    "gotrino",
		Subject:   "test",
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	signedToken, err := token.SignedString(privateKey)

	if err != nil {
		fmt.Println("Error generating token:", err)
		return
	}

	fmt.Println("JWT Token:", signedToken)
}
