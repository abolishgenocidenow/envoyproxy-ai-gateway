# Runtime credential extension

An embedding application can set `mainlib.Options.CredentialResolver`. A backend
then selects it explicitly with `auth: {externalCredential: "opaque-reference"}`.
The reference comes from accepted configuration. It cannot be combined with
inline credentials or request credential overrides. Without the callback,
loading a referenced backend fails.

The callback runs at every upstream authentication attempt, including retries.
It receives independent copies of request headers, body and Envoy metadata.
Headers and body are client input. The application must validate authenticated
principal and route/connection bindings using its trusted metadata namespace;
this extension does not turn arbitrary Envoy metadata into authorization.

A resolver must recheck authorization and freshness before returning credential
headers, including when using cached provider tokens. Any error denies the
attempt with 401. Errors are normalized rather than exposing credential material;
there is no inline-credential fallback. Empty or invalid header mutations are
rejected before the request map is modified. At most 32 headers, 256 bytes per
name and 16 KiB per value are accepted.

Existing backends without externalCredential keep the existing handler path.
The extension supplies an authentication boundary; it does not implement
credential delivery, workload scope assignment or route-generation activation.
