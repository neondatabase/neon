## Authentication

### Overview

Current state of authentication includes usage of JWT tokens in communication between compute and pageserver and between CLI and pageserver. JWT token is signed using RSA keys. CLI generates a key pair during call to `zenith init`. Using following openssl commands:

```bash
openssl genrsa -out private_key.pem 2048
openssl rsa -in private_key.pem -pubout -outform PEM -out public_key.pem
```

CLI also generates signed token and saves it in the config for later access to pageserver. Now authentication is optional. Pageserver has two variables in config: `auth_validation_public_key_path` and `auth_type`, so when auth type present and set to `ZenithJWT` pageserver will require authentication for connections. Actual JWT is passed in password field of connection string. There is a caveat for psql, it silently truncates passwords to 100 symbols, so to correctly pass JWT via psql you have to either use PGPASSWORD environment variable, or store password in psql config file.

Currently there is no authentication between compute and safekeepers, because this communication layer is under heavy refactoring. After this refactoring support for authentication will be added there too. Now safekeeper supports "hardcoded" token passed via environment variable to be able to use callmemaybe command in pageserver.

Compute uses token passed via environment variable to communicate to pageserver and in the future to the safekeeper too.

JWT authentication now supports two scopes: tenant and pageserverapi. Tenant scope is intended for use in tenant related api calls, e.g. create_branch. Compute launched for particular tenant also uses this scope. Scope pageserver api is intended to be used by console to manage pageserver. For now we have only one management operation - create tenant.

Examples for token generation in python:

```python
# generate pageserverapi token
management_token = jwt.encode({"scope": "pageserverapi"}, auth_keys.priv, algorithm="RS256")

# generate tenant token
tenant_token = jwt.encode({"scope": "tenant", "tenant_id": ps.initial_tenant}, auth_keys.priv, algorithm="RS256")
```

Utility functions to work with jwts in rust are located in libs/utils/src/auth.rs
