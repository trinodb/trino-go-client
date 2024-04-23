## Instructions to generate SSL certificate and JWT Token for Integration testing

### Generate an RSA private key without encryption
```
openssl genpkey -algorithm RSA -out private_key.pem
```

### Create a Certificate Signing Request (CSR) using the private key
```
openssl req -new -key private_key.pem -out certificate.csr
```

### Generate a self-signed certificate using the CSR
```
openssl x509 -req -days 36500 -in certificate.csr -signkey private_key.pem -out certificate.pem
```

### Extract Public key
```
openssl x509 -pubkey -noout -in certificate.pem> public_key.pem
```

### Add keys to Certificate
```
cat certificate.pem private_key.pem > certificate_with_key.pem
openssl x509 -in certificate.pem -pubkey -noout >> certificate_with_key.pem
```

### Generate JWT Token
```
go get -u github.com/golang-jwt/jwt
go run generate_jwt_token.go
```
