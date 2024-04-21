import jwt
from datetime import datetime, timedelta

# Define the payload for the JWT token
payload = {
    'sub': 'test',
    'exp': datetime.utcnow() + timedelta(days=36500)  # Expiration time (e.g., 1 day)
}

# Load the RSA private key
with open('private_key.pem', 'rb') as f:
    private_key = f.read()

# Generate JWT token
token = jwt.encode(payload, private_key, algorithm='RS256')

print(token) 
