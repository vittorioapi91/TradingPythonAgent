# SSL Certificates for Local Development

This directory contains self-signed SSL certificates for HTTPS local development domains.

## Certificates

- `jenkins.crt` / `jenkins.key` - Certificate for `jenkins.local.info`

## Trusting the Certificate (Remove Browser Warning)

### Chrome / Edge / Brave

1. Open `chrome://settings/certificates` (or `edge://settings/certificates`)
2. Go to **Authorities** tab
3. Click **Import**
4. Select `.ops/.docker/ssl/jenkins.crt`
5. Check **Trust this certificate for identifying websites**
6. Click **OK**
7. Restart browser

### Firefox

1. Open `about:preferences#privacy`
2. Scroll to **Certificates** section
3. Click **View Certificates**
4. Go to **Authorities** tab
5. Click **Import**
6. Select `.ops/.docker/ssl/jenkins.crt`
7. Check **Trust this CA to identify websites**
8. Click **OK**

### macOS Keychain (for Safari/Chrome)

1. Double-click `jenkins.crt` to open in Keychain Access
2. Find the certificate in **login** keychain
3. Double-click it
4. Expand **Trust** section
5. Set **When using this certificate** to **Always Trust**
6. Close and enter your password to save

## After Trusting

Once you trust the certificate:
- Access Jenkins at `https://jenkins.local.info` (with **https://**)
- Browser will show a secure connection (padlock icon)
- No more "Not secure" warnings

## Regenerating Certificates

If you need to regenerate certificates:

```bash
cd .ops/.docker/ssl
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout jenkins.key \
  -out jenkins.crt \
  -subj "/CN=jenkins.local.info/O=Local Development/C=US"
```

Then restart nginx-proxy:
```bash
docker-compose -f .ops/.docker/docker-compose.infra-platform.yml restart nginx-proxy
```
