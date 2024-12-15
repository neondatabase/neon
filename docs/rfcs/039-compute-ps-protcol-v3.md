# V3 version of compute-page server protocol

Created on: 2024-08-15

Author: Konstantin Knizhnik

## Summary

Current version of compute-PS protocol doesn't allow to verify that received response actually corresponds to the request.
In most cases it should not cause any problems, because Neon SMGR follows classical server request-response pattern.
If response is not received due to some reasons (network error, ...), then connection is dropped and error is reported.

But we also actively use prefetch, which allows to minimize network round-trip overhead.
In case of prefetch compute sends multiple getpage requests and then gets responses to all of them.
It is expected that responses are received in the same order as requests are sent, i.e. for each request we receive response.

Unfortunately it can be violated in case of errors. In this case it can happen that connection is reset but prefetch ring - not.
As a result we treat response of new request as response to some older prefetch request and place wrong page image in shared buffer.
If this page is modified, then it is saved in WAL and database file. So we are not able to recover original (correct) page image by applying WAL.
So we can mix pages of one relation or even pages of different relations. Most frequently such corruption is detected for indexes,
but just because there are more invariants which can be checked.  And there is no good universal way to detect and recover such
corruption.

## Motivation

This bug in prefetch was fixed, but we want to prevent similar problems in future.
For example prewarm is also similar with prefetch and sens several requests and only
after it wait for response.

## Previous work

We already changed protocol version from V1 to V2 when replaced single request LSN with pair
(request LSN,not modified since LSN). It was done by introducing new command codes.
So there was no explicit check for protocol version: if server receives new command,
it assumes that it is new protocol version. After both clients and servers were upgraded to new version,
new command codes were removed. Now V21 version of protocol is not supported.

Client has `neon.protocol_version` GUC specifying which protocol version it should use.

## Requirements

- Be able to verify that page server response corresponds to the requests
- Provide backward compatibility: old clients should work with new server.

## Non Goals

- Detect page corruption (include CRC)
- Support of vector operation (merge several requests into one)
- Forward compatibility: support new clients with page server


## Solution

Include in response extra fields making it possible to verify that response corresponds to the particular request.
Such extra fields may include:

- tablespace OID
- database OID
- relation OID
- fork number
- block id (for getpage)
- request LSN
- last modified LSN

All this fields can be replaced just with one field: request ID. But we have to somehow store this request ID in case of prefetch,
and bugs in prefetch logical can once again cause undetected mismatch of request and response.
From the other hand, if all the fields above in request and response are matched,
then no data corruption can happen even if request was no actually sent by our backend.

Disadvantage of this solution is increase of response size. But it is very unlikely that it can have any impact on
network traffic and performance. Sending extra tens bytes doesn't significantly affect packet size if it includes page image.
And other cases (relation/database size) are less frequent and actually speed of network round-trip almost not depends
on response size.

Request ID can also be used to match log messages produced by client and server.
Now sure how important it is, because we will not dump all requests and responses in any case, just errors.

If we prefer to use request ID, then the question is how to generate it. Two possibilities:
1. Use atomic variable in shared memory
2. Combine backend ID or process id with locally incremented counter. The problem here is that after
backend restart, it's ID (or even pid) can be reused and so generated ID will not be unique.
It will not cause problem with verifying responses, because unlikely than one process will receive
responses to another process, but may complicate matching messages at client and server side.

Another question: do we need to extend all responses or only `getpage` response? Corruption can happen only with mismatch
of `getpage` responses. And only `getpage` requests are used by prefetch. But for extra safety it is better to check all responses,
and there are no drawbacks except may be some extra coding.


## Compatibility

Current client-server protocol doesn't include handshake and protocol version.

We can follow the same approach as with V1->V2 upgrade: introduce new command and response codes.
For example, new codes can be produced by shifting old ones by 1000 (1->1001). If page server receives
new request code then it responds with new response (included extra information allowing to verify
response). When server receives old request code, it responds with old response.

We should deploy new page server and new client with `neon.protocol_version=2` and wait for some time
to ensure that we do not need to rollback to previous version. After it and ensuring that new page server
version is used in all regions we can start bumping protocol version to 3 for clients. Once we make sure
that all clients and [age servers are supporting new protocol version, we can drop V2 support
and eliminate the logic of handling new tags (as it was done for V1 protocol).


Alternatively we can introduce new `handshake` command and response. They can be send on establishing
connection to page server to negotiate used protocol version. In this case in future we do not need
this trick with introducing new tags (adding shift) and provide not only backward, but also forward
compatibility (make it possible for new client to work with old servers).
