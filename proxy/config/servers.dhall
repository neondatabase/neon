let Server = { server_name : Text, certificate : Text, private_key : Text }

let servers
    : List Server
    = [ ./foo.bar.localhost/server.dhall, ./neon.localhost/server.dhall ]

in  servers
