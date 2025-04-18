-- Basic functionality tests for pg_session_jwt

-- Test auth.init() function
SELECT auth.init();

-- Test an invalid JWT
SELECT auth.jwt_session_init('INVALID-JWT');

-- Test creating a session with an expired JWT
SELECT auth.jwt_session_init('eyJhbGciOiJFZERTQSJ9.eyJleHAiOjE3NDI1NjQ0MzIsImlhdCI6MTc0MjU2NDI1MiwianRpIjo0MjQyNDIsInN1YiI6InVzZXIxMjMifQ.A6FwKuaSduHB9O7Gz37g0uoD_U9qVS0JNtT7YABGVgB7HUD1AMFc9DeyhNntWBqncg8k5brv-hrNTuUh5JYMAw');

-- Test creating a session with a valid JWT
SELECT auth.jwt_session_init('eyJhbGciOiJFZERTQSJ9.eyJleHAiOjQ4OTYxNjQyNTIsImlhdCI6MTc0MjU2NDI1MiwianRpIjo0MzQzNDMsInN1YiI6InVzZXIxMjMifQ.2TXVgjb6JSUq6_adlvp-m_SdOxZSyGS30RS9TLB0xu2N83dMSs2NybwE1NMU8Fb0tcAZR_ET7M2rSxbTrphfCg');

-- Test auth.session() function
SELECT auth.session();

-- Test auth.user_id() function
SELECT auth.user_id() AS user_id;