# Simplified auth flow for Neon Proxy

## Overview

This is how authentication works at the moment.

```
             cloud (console)
             ^
             | // http calls: get_auth_info, wake_compute
             v
client <-> proxy <-> compute node
        .         .
        .         . // Step B. Do authentication one more time.
        .
        . // Step A. Client authentication takes place here.
```

First, the client connects to the proxy. Before we even consider waking up the compute node and connecting to it, we perform client authentication to make sure that the client is allowed to operate the compute node. Currently, there are 3 ways to authenticate in step `A`:

1. **Link** -- the whole authentication is delegated to a cloud console's web page; the user is expected to open a link we've sent them via a libpq's `NOTICE` message. After the user has successfully authenticated, the console wakes the compute node and asynchronously sends a connection string to the proxy. This method isn't really suitable for production scenarios because of its numerous limitations: it's not very practical to visit a link per connection; moreover, some clients may not even show the `NOTICE` before a proper connection has been established. There are some security concerns as well (which are beyond this RFC).

2. **Console (aka [SCRAM](https://www.rfc-editor.org/rfc/rfc5802))** -- the proxy asks the console for the user's auth info (identified by a pair of `(role, project)`) which it immediately uses to choose an authentication method and run it. At the moment we support SCRAM exclusively. By and large, this is the most secure authentication method:
    * It prevents [MITM](https://en.wikipedia.org/wiki/Man-in-the-middle_attack) attacks with the help of mutual client-server auth and a [channel binding](https://www.postgresql.org/docs/current/sasl-authentication.html#SASL-SCRAM-SHA-256) mode.
    * The previous property does not rely on any certificate authority (client doesn't need to install root CA).
    * We don't have to store a cleartext password anywhere (in fact, **we don't even know the password**).

3. **Password hack** -- connection params story is [known to be very complicated](https://github.com/neondatabase/cloud/issues/1620): long story short, we can't pick option 2 if the user [couldn't specify `project`](https://neon.tech/docs/how-to-guides/connectivity-issues/). If that's the case, the user is expected to provide the missing parameter via password prompt (i.e. `project=<NAME>;<PASSWORD>`). This will allow the proxy to wake the compute node identified by `project` and then use the password to establish a connection on the user's behalf. Needless to say, this absolutely requires [TLS](https://www.rfc-editor.org/rfc/rfc8446) & [`verify-full`](https://www.postgresql.org/docs/current/libpq-ssl.html#LIBQ-SSL-CERTIFICATES). **The sole purpose of this method is to help overcome the limitations of legacy clients.**

Regardless of the option that has been chosen in step `A`, the compute node still performs its own authentication in step `B`; oftentimes it uses the same authentication method. However, some of the combinations of authentication methods in `A` and `B` are prohibited: for instance, it's impossible to perform cleartext-password-based authentication in step `B` if we've chosen SCRAM in step `A`. Clearly, this is not a good thing. Furthermore, it's not obvious whether we _need_ to do a full-blown auth in step `B`.

## Limitations

Things get worse once we introduce partner integrations. Now there might be:

* **End users**, i.e. the ones who consciously created their password-protected projects.
* **Third parties** acting on behalf of their end users. **They might not know end users' passwords**.

Now, what if a user wanted to combine the benefits of third-party integrations with the ability to operate compute node manually (e.g. via psql)? Then there are multiple possibilities:

1. We create a postgres role per each party. Each role uses its own auth method for flexibility. This introduces object ownership problems: every role potentially owns a subset of objects, and it's _very hard_ to share them with other roles.
2. All of those parties (user & integrations) share the user's credentials. This is either complicated or insecure.
3. No passwords or other secrets are shared. Parties conflict with each other because they have to reset the password in order to access the data.

## Motivation

Partner integrations are very important because they add various features on top of what we have to offer, so the last thing we want is to handicap them. At the moment we stick with the option 3 (_password reset_, see above), but this is very incovenient for users and has to change.

This proposal aims to streamline the whole authentication process and lift some of its restrictions.

## Proposed solution

One of the reasons to add support for SCRAM authentication to the proxy was the fact that it's possible ([and explicitly allowed](https://github.com/sfackler/rust-postgres/pull/887#issue-1208411878)) to use server auth info + `ClientKey` (restored in step `A`) to successfully authenticate in step `B` using **the same** mechanism. That's right, we don't need to know the password provided that we have the server auth info (that the console gave us via an http call).

But now the question is: do we really need step `B`? The proxy mediates **every** client connection to compute node; it's not possible to connect otherwise. This means step `A` might be sufficient and we don't need to do the same thing all over again.

If we'd like to completely rule out unsolicited access to compute nodes from within our private network, we might use an authentication mechanism that doesn't depend on any user input (e.g. passwords), such as [certificate authentication](https://www.postgresql.org/docs/current/auth-cert.html). **The key idea is to eliminate special cases and always use one mechanism**. In any case, the control plane (console) would be in charge of generating auth tokens for the proxy once step `A` is over. Below we explore various options.

### Step `B`: add support for opt-in [JWT](https://www.rfc-editor.org/rfc/rfc7519) authentication

Pros:
* We use it elsewhere, so it must have some virtues?
* It's possible to specify token's due date and _eventually_ revoke access.

Cons:
* Requires patches to both client- (tokio-postgres / whatever) and server (postgres) sides.
* Introduces an unprecedented connection option or protocol packet at server side (to opt-in the method).

This change would let us overcome the limitations of [`pg_hba.conf`](https://www.postgresql.org/docs/current/auth-pg-hba-conf.html) and select either the default auth mechanism (e.g. SCRAM) for regular clients or JWT for partner integrations. Overall, this looks needlessly overcomplicated.

### Step `B`: use [PAM](https://www.postgresql.org/docs/current/auth-pam.html)

Pros:
* It's possible to implement virtually _any_ authentication mechanism.
* There might be an existing PAM module (or template) to solve _some_ of our problems.

Cons:
* Appears to require more coding that any other option.
* "Any" doesn't necessarily mean "good" or "secure". We might make mistakes.
* Requires tinkering with PAM's config in `/etc` of the Docker image.

This idea sounds cool in principle but doesn't seem to be worth exploring at the moment.

### Step `B`: use [Kerberos](https://www.postgresql.org/docs/current/gssapi-auth.html) or [LDAP](https://www.postgresql.org/docs/current/auth-ldap.html)

Pros:
* _Presumably_ doesn't suffer from cert auth's infra problems.
* It's possible to write our own LDAP server or tune an existing one.

Cons:
* Intoduces (at least) one more roundtrip for credentials check.
* Requires tinkering with [`pg_ident.conf`](https://www.postgresql.org/docs/current/auth-username-maps.html).

TODO: This option requires more exploration.

### Step `B`: use [certificate authentication](https://www.postgresql.org/docs/current/auth-cert.html) ([example](https://www.crunchydata.com/blog/ssl-certificate-authentication-postgresql-docker-containers))

Pros:
* User might change the password without affecting auth in step `B` (because certs don't depend on password).
* Already implemented on both client- and server sides (no need for protocol hacks or any other patches).
* It's possible to specify certificate's due date and revoke access ([`ssl_crl_file`](https://en.wikipedia.org/wiki/Certificate_revocation_list)).

Cons:
* Requires some changes to infrastructure.
* Requires tinkering with [`pg_ident.conf`](https://www.postgresql.org/docs/current/auth-username-maps.html).

Prerequisites:
* One `CA.{key,crt}` pair for verification purposes.
* A `server.{key,crt}` pair per compute node (for TLS).
* A `client.{key,crt}` pair per client (or role / whatever).

We'd start by generating a root certificate authority (`CA.{key,crt}`) and storing the key in a secure place. Then, for any compute node we'd generate `server.{key,crt}` signed by CA which we don't have to share with anyone (we could do this a. just once or b. from time to time or c. every time we wake the compute). As for the client cert, we'd better setup some kind of retention policy, e.g. make it short-lived (~30s). This would allow us to cache it on proxy's side in a straightforward manner.

In this scheme the console is supposed to either store `{client,server}.{key,crt}` in a secure manner or generate them on the fly. NOTE: keep in mind that client cert is useless unless it's signed by CA.

This is how it could work at the proxy's side:

* **Link**:
    - Show a URL to user.
    - User opens the web UI and does what's necessary.
    - Console asynchronously responds with connstr + client cert + key.

* **Console (SCRAM)**:
    - http: Fetch auth info using `project`.
    - Authenticate user.
    - http: Request wake-up.
    - Console responds with connstr + client cert + key.

* **Password hack**:
    - No `project`, user passes it via "password".
    - http: Request wake-up.
    - Console responds with connstr + client cert + key.
