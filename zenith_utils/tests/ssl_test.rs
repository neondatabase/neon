use std::{
    collections::HashMap,
    io::{Cursor, Read, Write},
    net::{TcpListener, TcpStream},
    sync::Arc,
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use lazy_static::lazy_static;
use rustls::Session;

use zenith_utils::postgres_backend::{AuthType, Handler, PostgresBackend};

fn make_tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let client_stream = TcpStream::connect(addr).unwrap();
    let (server_stream, _) = listener.accept().unwrap();
    (server_stream, client_stream)
}

lazy_static! {
    static ref KEY: rustls::PrivateKey = {
        let mut cursor = Cursor::new(include_bytes!("key.pem"));
        rustls::internal::pemfile::rsa_private_keys(&mut cursor).unwrap()[0].clone()
    };
    static ref CERT: rustls::Certificate = {
        let mut cursor = Cursor::new(include_bytes!("cert.pem"));
        rustls::internal::pemfile::certs(&mut cursor).unwrap()[0].clone()
    };
}

#[test]
fn ssl() {
    let (mut client_sock, server_sock) = make_tcp_pair();

    const QUERY: &[u8] = b"hello world";

    let client_jh = std::thread::spawn(move || {
        // SSLRequest
        client_sock.write_u32::<BigEndian>(8).unwrap();
        client_sock.write_u32::<BigEndian>(80877103).unwrap();

        let ssl_response = client_sock.read_u8().unwrap();
        assert_eq!(b'S', ssl_response);

        let mut cfg = rustls::ClientConfig::new();
        cfg.root_store.add(&CERT).unwrap();
        let client_config = Arc::new(cfg);

        let dns_name = webpki::DNSNameRef::try_from_ascii_str("localhost").unwrap();
        let mut session = rustls::ClientSession::new(&client_config, dns_name);

        session.complete_io(&mut client_sock).unwrap();
        assert!(!session.is_handshaking());

        let mut stream = rustls::Stream::new(&mut session, &mut client_sock);

        // StartupMessage
        stream.write_u32::<BigEndian>(9).unwrap();
        stream.write_u32::<BigEndian>(196608).unwrap();
        stream.write_u8(0).unwrap();
        stream.flush().unwrap();

        // wait for ReadyForQuery
        let mut msg_buf = Vec::new();
        loop {
            let msg = stream.read_u8().unwrap();
            let size = stream.read_u32::<BigEndian>().unwrap() - 4;
            msg_buf.resize(size as usize, 0);
            stream.read_exact(&mut msg_buf).unwrap();

            if msg == b'Z' {
                // ReadyForQuery
                break;
            }
        }

        // Query
        stream.write_u8(b'Q').unwrap();
        stream
            .write_u32::<BigEndian>(4u32 + QUERY.len() as u32)
            .unwrap();
        stream.write_all(QUERY).unwrap();
        stream.flush().unwrap();

        // ReadyForQuery
        let msg = stream.read_u8().unwrap();
        assert_eq!(msg, b'Z');
    });

    struct TestHandler {
        got_query: bool,
    }
    impl Handler for TestHandler {
        fn process_query(
            &mut self,
            _pgb: &mut PostgresBackend,
            query_string: bytes::Bytes,
        ) -> anyhow::Result<()> {
            self.got_query = query_string.as_ref() == QUERY;
            Ok(())
        }
    }
    let mut handler = TestHandler { got_query: false };

    let mut cfg = rustls::ServerConfig::new(rustls::NoClientAuth::new());
    cfg.set_single_cert(vec![CERT.clone()], KEY.clone())
        .unwrap();
    let tls_config = Some(Arc::new(cfg));

    let pgb = PostgresBackend::new(server_sock, AuthType::Trust, tls_config, true).unwrap();
    pgb.run(&mut handler).unwrap();
    assert!(handler.got_query);

    client_jh.join().unwrap();

    // TODO consider shutdown behavior
}

#[test]
fn no_ssl() {
    let (mut client_sock, server_sock) = make_tcp_pair();

    let client_jh = std::thread::spawn(move || {
        let mut buf = BytesMut::new();

        // SSLRequest
        buf.put_u32(8);
        buf.put_u32(80877103);
        client_sock.write_all(&buf).unwrap();
        buf.clear();

        let ssl_response = client_sock.read_u8().unwrap();
        assert_eq!(b'N', ssl_response);
    });

    struct TestHandler;

    impl Handler for TestHandler {
        fn process_query(
            &mut self,
            _pgb: &mut PostgresBackend,
            _query_string: bytes::Bytes,
        ) -> anyhow::Result<()> {
            panic!()
        }
    }

    let mut handler = TestHandler;

    let pgb = PostgresBackend::new(server_sock, AuthType::Trust, None, true).unwrap();
    pgb.run(&mut handler).unwrap();

    client_jh.join().unwrap();
}

#[test]
fn server_forces_ssl() {
    let (mut client_sock, server_sock) = make_tcp_pair();

    let client_jh = std::thread::spawn(move || {
        // StartupMessage
        client_sock.write_u32::<BigEndian>(9).unwrap();
        client_sock.write_u32::<BigEndian>(196608).unwrap();
        client_sock.write_u8(0).unwrap();
        client_sock.flush().unwrap();

        // ErrorResponse
        assert_eq!(client_sock.read_u8().unwrap(), b'E');
        let len = client_sock.read_u32::<BigEndian>().unwrap() - 4;

        let mut body = vec![0; len as usize];
        client_sock.read_exact(&mut body).unwrap();
        let mut body = Bytes::from(body);

        let mut errors = HashMap::new();
        loop {
            let field_type = body.get_u8();
            if field_type == 0u8 {
                break;
            }

            let end_idx = body.iter().position(|&b| b == 0u8).unwrap();
            let mut value = body.split_to(end_idx + 1);
            assert_eq!(value[end_idx], 0u8);
            value.truncate(end_idx);
            let old = errors.insert(field_type, value);
            assert!(old.is_none());
        }

        assert!(!body.has_remaining());

        assert_eq!("must connect with TLS", errors.get(&b'M').unwrap());

        // TODO read failure
    });

    struct TestHandler;
    impl Handler for TestHandler {
        fn process_query(
            &mut self,
            _pgb: &mut PostgresBackend,
            _query_string: bytes::Bytes,
        ) -> anyhow::Result<()> {
            panic!()
        }
    }
    let mut handler = TestHandler;

    let mut cfg = rustls::ServerConfig::new(rustls::NoClientAuth::new());
    cfg.set_single_cert(vec![CERT.clone()], KEY.clone())
        .unwrap();
    let tls_config = Some(Arc::new(cfg));

    let pgb = PostgresBackend::new(server_sock, AuthType::Trust, tls_config, true).unwrap();
    let res = pgb.run(&mut handler).unwrap_err();
    assert_eq!("client did not connect with TLS", format!("{}", res));

    client_jh.join().unwrap();

    // TODO consider shutdown behavior
}
