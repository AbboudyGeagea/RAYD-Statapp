# SSL Certificates

Place your SSL certificate files here before running docker-compose:

- `fullchain.pem` — your full certificate chain (from Let's Encrypt or your CA)
- `privkey.pem`   — your private key

## Getting certs with Let's Encrypt (on the host before docker):
```
apt install certbot
certbot certonly --standalone -d yourdomain.com
cp /etc/letsencrypt/live/yourdomain.com/fullchain.pem ./fullchain.pem
cp /etc/letsencrypt/live/yourdomain.com/privkey.pem ./privkey.pem
```

## Self-signed (for internal/testing use):
```
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout privkey.pem -out fullchain.pem \
  -subj "/CN=rayd.internal"
```
