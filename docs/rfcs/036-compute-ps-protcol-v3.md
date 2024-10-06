# V3 version of compute-page server protocol

Created on: 2024-08-15

Author: Konstantin Knizhnik

# Summary

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

# Motivation

This bug in prefetch was fixed, but we want to prevent similar problems in future.

# Solution

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
network traffic and performace. Sending extra tens bytes doesn't significantly affect packet size if it includes page image.
And other cases (relation/database size) are less frequent and actually speed of network round-trip almost not depends
on response size.

Another question: do we need to extend all responses or only `getpage` response? Corruption can happen only with mismatch
of `getpage` responses. And only `getpage` requests are used by prefetch. But for extra safety it is better to check all responses,
and there are no drawbacks except may be some extra coding.
