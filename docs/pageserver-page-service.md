# Page Service

The Page Service listens for GetPage@LSN requests from the Compute Nodes,
and responds with pages from the repository. On each GetPage@LSN request,
it calls into the Repository function

A separate thread is spawned for each incoming connection to the page
service. The page service uses the libpq protocol to communicate with
the client. The client is a Compute Postgres instance.
