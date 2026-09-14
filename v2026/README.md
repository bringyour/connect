# URnetwork Connect

A web-standards VPN marketplace with an emphasis on fast, secure internet everwhere. This project exists to create a trusted best-in-class technology for the "public VPN" market, that:

- Works on consumer devices from the normal app stores
- Allows consumer devices to tap into existing resources to enhance the public VPN. We believe a more ubiquitous and distributed VPN will be better for consumers.
- Emphasizes privacy, security, and availability


## Protocol

[Protocol defintion](protocol): Protobuf messages for the realtime transport protocol

[API definition](api): OpenAPI definition for the API for clients to interact with the marketplace


## Buffer reuse

Anywhere in the code that returns a `[]byte` will allocate it from the shared message pool. The following rules are used:

- When passing `[]byte` into a function that takes ownership of the `[]byte`, the final owner should return the byte to the message pool when finished. Examples of this are passing a `[]byte` to a channel and async io loop functions.
- When passing a `[]byte` to a callback, the callback should assume the `[]byte` is valid only for the duration of the function call. The caller will return the `[]byte` to the message pool after the callbacks are processed.


## Discord

[https://discord.gg/urnetwork](https://discord.gg/urnetwork)


## License

URnetwork is licenced under the [MPL 2.0](LICENSE).


![URnetwork](res/images/connect-project.webp "URnetwork")

[URnetwork](https://ur.io): better internet

