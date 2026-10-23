//! Windows SSPI: the `Negotiate` (Kerberos or NTLM) and `NTLM` security packages with the current
//! account's credentials or explicit ones, as .NET's `NegotiateAuthentication` uses them for
//! HTTP: integrity required (`ProtectionLevel.Sign`), the tokens bound to the TLS channel, and the
//! SPN built from the host's canonical DNS name.

use std::ptr;

use windows_sys::Win32::{
    Foundation::{
        SEC_E_OK, SEC_I_COMPLETE_AND_CONTINUE, SEC_I_COMPLETE_NEEDED, SEC_I_CONTINUE_NEEDED,
    },
    Networking::WinSock::{
        FreeAddrInfoW, GetAddrInfoW, WSACleanup, WSAStartup, ADDRINFOW, AI_CANONNAME, WSADATA,
    },
    Security::{
        Authentication::Identity::{
            AcquireCredentialsHandleW, CompleteAuthToken, DeleteSecurityContext, FreeContextBuffer,
            FreeCredentialsHandle, InitializeSecurityContextW, SecBuffer, SecBufferDesc,
            ISC_REQ_ALLOCATE_MEMORY, ISC_REQ_CONNECTION, ISC_REQ_INTEGRITY, ISC_RET_INTEGRITY,
            SECBUFFER_CHANNEL_BINDINGS, SECBUFFER_TOKEN, SECBUFFER_VERSION, SECPKG_CRED_OUTBOUND,
            SECURITY_NATIVE_DREP, SEC_CHANNEL_BINDINGS,
        },
        Credentials::SecHandle,
    },
    System::Rpc::{SEC_WINNT_AUTH_IDENTITY_UNICODE, SEC_WINNT_AUTH_IDENTITY_W},
};

use super::{AuthScheme, Explicit, SecurityContext};
use crate::error::Error;

fn wide(text: &str) -> Vec<u16> {
    text.encode_utf16().chain(std::iter::once(0)).collect()
}

fn failure(step: &str, status: i32) -> Error {
    Error::authentication(
        None,
        format!("integrated Windows authentication failed: {step} returned SECURITY_STATUS 0x{status:08X}"),
    )
}

/// The host name the SPN names, as `SocketsHttpHandler` derives it: an IP address as is, any
/// other name as its canonical DNS name (following CNAMEs), or as given when the lookup fails.
/// Blocks on DNS.
pub(crate) fn canonical_host(host: &str) -> String {
    if host.parse::<std::net::IpAddr>().is_ok() {
        return host.to_string();
    }
    let name = wide(host);
    let hints = ADDRINFOW {
        ai_flags: AI_CANONNAME as i32,
        ..ADDRINFOW::default()
    };
    let mut data = WSADATA::default();
    // SAFETY: WSAStartup only writes `data`; it is reference-counted and paired with the
    // WSACleanup below.
    if unsafe { WSAStartup(0x0202, &mut data) } != 0 {
        return host.to_string();
    }
    let mut result: *mut ADDRINFOW = ptr::null_mut();
    // SAFETY: `name` is NUL-terminated and `hints` valid for the call; the result list is freed
    // below.
    let status = unsafe { GetAddrInfoW(name.as_ptr(), ptr::null(), &hints, &mut result) };
    let mut canonical = None;
    if status == 0 && !result.is_null() {
        // SAFETY: GetAddrInfoW succeeded, so `result` points at its first entry, whose
        // `ai_canonname` is null or a NUL-terminated string living as long as the list.
        unsafe {
            let name = (*result).ai_canonname;
            if !name.is_null() {
                let length = (0..).take_while(|&index| *name.add(index) != 0).count();
                canonical = Some(String::from_utf16_lossy(std::slice::from_raw_parts(
                    name, length,
                )));
            }
            FreeAddrInfoW(result);
        }
    }
    // SAFETY: pairs the successful WSAStartup above.
    unsafe { WSACleanup() };
    canonical
        .filter(|canonical| !canonical.is_empty())
        .unwrap_or_else(|| host.to_string())
}

/// A buffer SSPI allocated (`ISC_REQ_ALLOCATE_MEMORY`), freed on every path.
struct ContextBuffer(*mut core::ffi::c_void);

impl Drop for ContextBuffer {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: allocated by SSPI for one call and freed once.
            unsafe { FreeContextBuffer(self.0) };
        }
    }
}

/// The `SEC_CHANNEL_BINDINGS` structure carrying `application_data` (no addresses) right after
/// it, in `u32`s so the structure is aligned.
fn channel_bindings(application_data: &[u8]) -> Vec<u32> {
    const HEADER: usize = std::mem::size_of::<SEC_CHANNEL_BINDINGS>();
    let header = SEC_CHANNEL_BINDINGS {
        cbApplicationDataLength: application_data.len() as u32,
        dwApplicationDataOffset: HEADER as u32,
        ..SEC_CHANNEL_BINDINGS::default()
    };
    let mut buffer = vec![0u32; (HEADER + application_data.len()).div_ceil(4)];
    // SAFETY: the buffer holds at least HEADER + application_data.len() bytes; the structure is
    // plain `u32`s.
    unsafe {
        ptr::write(buffer.as_mut_ptr().cast::<SEC_CHANNEL_BINDINGS>(), header);
        ptr::copy_nonoverlapping(
            application_data.as_ptr(),
            buffer.as_mut_ptr().cast::<u8>().add(HEADER),
            application_data.len(),
        );
    }
    buffer
}

/// One SSPI client context for `HTTP/<host>`.
pub(crate) struct SspiContext {
    credentials: SecHandle,
    context: Option<SecHandle>,
    target: Vec<u16>,
    /// The channel bindings (TLS only), passed with every call.
    bindings: Option<(Vec<u32>, u32)>,
    finished: bool,
}

impl SspiContext {
    /// Acquires outbound credentials for `scheme`'s package: the current account's, or
    /// `explicit` ones. `binding` is the connection's RFC 5929 channel binding.
    pub(crate) fn new(
        scheme: AuthScheme,
        host: &str,
        explicit: Option<&Explicit>,
        binding: Option<&[u8]>,
    ) -> Result<Self, Error> {
        let package = wide(match scheme {
            AuthScheme::Negotiate => "Negotiate",
            AuthScheme::Ntlm => "NTLM",
        });
        // The identity's strings must outlive the call; SSPI copies them.
        let mut user = explicit
            .map(|credentials| wide(&credentials.user))
            .unwrap_or_default();
        let mut domain = explicit
            .map(|credentials| wide(&credentials.domain))
            .unwrap_or_default();
        let mut password = explicit
            .map(|credentials| wide(&credentials.password))
            .unwrap_or_default();
        let identity = SEC_WINNT_AUTH_IDENTITY_W {
            User: user.as_mut_ptr(),
            UserLength: user.len().saturating_sub(1) as u32,
            Domain: domain.as_mut_ptr(),
            DomainLength: domain.len().saturating_sub(1) as u32,
            Password: password.as_mut_ptr(),
            PasswordLength: password.len().saturating_sub(1) as u32,
            Flags: SEC_WINNT_AUTH_IDENTITY_UNICODE,
        };
        let auth_data: *const core::ffi::c_void = if explicit.is_some() {
            ptr::from_ref(&identity).cast()
        } else {
            ptr::null()
        };
        let mut credentials = SecHandle::default();
        let mut expiry = 0i64;
        // SAFETY: every pointer is valid for the call; the output handle is written by SSPI.
        let status = unsafe {
            AcquireCredentialsHandleW(
                ptr::null(),
                package.as_ptr(),
                SECPKG_CRED_OUTBOUND,
                ptr::null(),
                auth_data,
                None,
                ptr::null(),
                &mut credentials,
                &mut expiry,
            )
        };
        // This wide copy of the password is overwritten once SSPI has its own (the configured
        // credentials keep theirs until the client is dropped).
        for unit in &mut password {
            // SAFETY: a valid, aligned element of `password`.
            unsafe { ptr::write_volatile(unit, 0) };
        }
        if status != SEC_E_OK {
            return Err(failure("AcquireCredentialsHandle", status));
        }
        Ok(Self {
            credentials,
            context: None,
            target: wide(&format!("HTTP/{host}")),
            bindings: binding.map(|binding| (channel_bindings(binding), binding.len() as u32)),
            finished: false,
        })
    }
}

impl SecurityContext for SspiContext {
    fn step(&mut self, challenge: Option<&[u8]>) -> Result<Option<Vec<u8>>, Error> {
        if self.finished {
            return Ok(None);
        }
        let mut inputs = Vec::with_capacity(2);
        if let Some(challenge) = challenge {
            inputs.push(SecBuffer {
                cbBuffer: challenge.len() as u32,
                BufferType: SECBUFFER_TOKEN,
                pvBuffer: challenge.as_ptr().cast_mut().cast(),
            });
        }
        if let Some((bindings, length)) = &mut self.bindings {
            inputs.push(SecBuffer {
                cbBuffer: std::mem::size_of::<SEC_CHANNEL_BINDINGS>() as u32 + *length,
                BufferType: SECBUFFER_CHANNEL_BINDINGS,
                pvBuffer: bindings.as_mut_ptr().cast(),
            });
        }
        let input = SecBufferDesc {
            ulVersion: SECBUFFER_VERSION,
            cBuffers: inputs.len() as u32,
            pBuffers: inputs.as_mut_ptr(),
        };
        let mut output_buffer = SecBuffer {
            cbBuffer: 0,
            BufferType: SECBUFFER_TOKEN,
            pvBuffer: ptr::null_mut(),
        };
        let mut output = SecBufferDesc {
            ulVersion: SECBUFFER_VERSION,
            cBuffers: 1,
            pBuffers: &mut output_buffer,
        };
        let mut new_context = self.context.unwrap_or_default();
        let mut attributes = 0u32;
        let mut expiry = 0i64;
        // SAFETY: the handles are this context's; the buffers live across the call; SSPI allocates
        // the output token (ISC_REQ_ALLOCATE_MEMORY), freed by the guard below.
        let status = unsafe {
            InitializeSecurityContextW(
                &self.credentials,
                self.context.as_ref().map_or(ptr::null(), ptr::from_ref),
                self.target.as_ptr(),
                ISC_REQ_ALLOCATE_MEMORY | ISC_REQ_CONNECTION | ISC_REQ_INTEGRITY,
                0,
                SECURITY_NATIVE_DREP,
                if inputs.is_empty() {
                    ptr::null()
                } else {
                    &input
                },
                0,
                &mut new_context,
                &mut output,
                &mut attributes,
                &mut expiry,
            )
        };
        let allocated = ContextBuffer(output_buffer.pvBuffer);
        if status != SEC_E_OK
            && status != SEC_I_CONTINUE_NEEDED
            && status != SEC_I_COMPLETE_NEEDED
            && status != SEC_I_COMPLETE_AND_CONTINUE
        {
            return Err(failure("InitializeSecurityContext", status));
        }
        self.context = Some(new_context);
        if status == SEC_I_COMPLETE_NEEDED || status == SEC_I_COMPLETE_AND_CONTINUE {
            // SAFETY: the context and the output descriptor are the ones just produced.
            let completed = unsafe { CompleteAuthToken(&new_context, &output) };
            if completed != SEC_E_OK {
                return Err(failure("CompleteAuthToken", completed));
            }
        }
        self.finished = status == SEC_E_OK || status == SEC_I_COMPLETE_NEEDED;
        // As .NET's `ProtectionLevel.Sign`: a context without integrity fails.
        if self.finished && attributes & ISC_RET_INTEGRITY == 0 {
            return Err(Error::authentication(
                None,
                "integrated Windows authentication failed: the security context has no integrity protection",
            ));
        }
        let token = if allocated.0.is_null() || output_buffer.cbBuffer == 0 {
            None
        } else {
            // SAFETY: SSPI allocated `cbBuffer` bytes at `pvBuffer`.
            Some(
                unsafe {
                    std::slice::from_raw_parts(
                        allocated.0.cast::<u8>(),
                        output_buffer.cbBuffer as usize,
                    )
                }
                .to_vec(),
            )
        };
        Ok(token)
    }
}

impl Drop for SspiContext {
    fn drop(&mut self) {
        // SAFETY: the handles were produced by SSPI for this value and are released once.
        unsafe {
            if let Some(context) = self.context.take() {
                DeleteSecurityContext(&context);
            }
            FreeCredentialsHandle(&self.credentials);
        }
    }
}

#[cfg(test)]
mod integrated_auth_sspi {
    //! The client contexts against in-process SSPI acceptors (`AcceptSecurityContext`), on
    //! Windows.

    use std::ptr;

    use windows_sys::Win32::{
        Foundation::{
            SEC_E_BAD_BINDINGS, SEC_E_LOGON_DENIED, SEC_E_OK, SEC_I_COMPLETE_AND_CONTINUE,
            SEC_I_COMPLETE_NEEDED,
        },
        Security::{
            Authentication::Identity::{
                AcceptSecurityContext, AcquireCredentialsHandleW, DeleteSecurityContext,
                FreeContextBuffer, FreeCredentialsHandle, SecBuffer, SecBufferDesc,
                ASC_REQ_ALLOCATE_MEMORY, ASC_REQ_CONNECTION, ASC_REQ_INTEGRITY,
                SECBUFFER_CHANNEL_BINDINGS, SECBUFFER_TOKEN, SECBUFFER_VERSION,
                SECPKG_CRED_INBOUND, SECURITY_NATIVE_DREP, SEC_CHANNEL_BINDINGS,
            },
            Credentials::SecHandle,
        },
    };

    use super::{canonical_host, channel_bindings, wide, SspiContext};
    use crate::communication::integrated_auth::{
        ntlm::NtlmContext, AuthScheme, Explicit, SecurityContext,
    };

    /// An NTLM acceptor with the current account's inbound credentials.
    struct Acceptor {
        credentials: SecHandle,
        context: Option<SecHandle>,
        bindings: Option<Vec<u32>>,
        length: u32,
    }

    impl Acceptor {
        fn new(binding: Option<&[u8]>) -> Self {
            let package = wide("NTLM");
            let mut credentials = SecHandle::default();
            let mut expiry = 0i64;
            let status = unsafe {
                AcquireCredentialsHandleW(
                    ptr::null(),
                    package.as_ptr(),
                    SECPKG_CRED_INBOUND,
                    ptr::null(),
                    ptr::null(),
                    None,
                    ptr::null(),
                    &mut credentials,
                    &mut expiry,
                )
            };
            assert_eq!(status, SEC_E_OK, "inbound NTLM credentials");
            Self {
                credentials,
                context: None,
                bindings: binding.map(channel_bindings),
                length: binding.map_or(0, |binding| binding.len() as u32),
            }
        }

        /// Accepts `token`: the status and the reply token.
        fn accept(&mut self, token: &[u8]) -> (i32, Option<Vec<u8>>) {
            let mut inputs = vec![SecBuffer {
                cbBuffer: token.len() as u32,
                BufferType: SECBUFFER_TOKEN,
                pvBuffer: token.as_ptr().cast_mut().cast(),
            }];
            if let Some(bindings) = &mut self.bindings {
                inputs.push(SecBuffer {
                    cbBuffer: std::mem::size_of::<SEC_CHANNEL_BINDINGS>() as u32 + self.length,
                    BufferType: SECBUFFER_CHANNEL_BINDINGS,
                    pvBuffer: bindings.as_mut_ptr().cast(),
                });
            }
            let input = SecBufferDesc {
                ulVersion: SECBUFFER_VERSION,
                cBuffers: inputs.len() as u32,
                pBuffers: inputs.as_mut_ptr(),
            };
            let mut output_buffer = SecBuffer {
                cbBuffer: 0,
                BufferType: SECBUFFER_TOKEN,
                pvBuffer: ptr::null_mut(),
            };
            let mut output = SecBufferDesc {
                ulVersion: SECBUFFER_VERSION,
                cBuffers: 1,
                pBuffers: &mut output_buffer,
            };
            let mut new_context = self.context.unwrap_or_default();
            let mut attributes = 0u32;
            let mut expiry = 0i64;
            let status = unsafe {
                AcceptSecurityContext(
                    &self.credentials,
                    self.context.as_ref().map_or(ptr::null(), ptr::from_ref),
                    &input,
                    ASC_REQ_ALLOCATE_MEMORY | ASC_REQ_CONNECTION | ASC_REQ_INTEGRITY,
                    SECURITY_NATIVE_DREP,
                    &mut new_context,
                    &mut output,
                    &mut attributes,
                    &mut expiry,
                )
            };
            if status >= 0 {
                self.context = Some(new_context);
            }
            let reply =
                (!output_buffer.pvBuffer.is_null() && output_buffer.cbBuffer > 0).then(|| unsafe {
                    std::slice::from_raw_parts(
                        output_buffer.pvBuffer.cast::<u8>(),
                        output_buffer.cbBuffer as usize,
                    )
                    .to_vec()
                });
            if !output_buffer.pvBuffer.is_null() {
                unsafe { FreeContextBuffer(output_buffer.pvBuffer) };
            }
            assert!(
                status != SEC_I_COMPLETE_NEEDED && status != SEC_I_COMPLETE_AND_CONTINUE,
                "NTLM acceptors need no CompleteAuthToken"
            );
            (status, reply)
        }
    }

    impl Drop for Acceptor {
        fn drop(&mut self) {
            unsafe {
                if let Some(context) = self.context.take() {
                    DeleteSecurityContext(&context);
                }
                FreeCredentialsHandle(&self.credentials);
            }
        }
    }

    /// Runs `client` against `acceptor`: the acceptor's final status.
    fn handshake(client: &mut dyn SecurityContext, acceptor: &mut Acceptor) -> i32 {
        let negotiate = client.step(None).unwrap().expect("a negotiate token");
        let (status, challenge) = acceptor.accept(&negotiate);
        assert!(
            status >= 0,
            "the acceptor took the negotiate message: {status:#X}"
        );
        let authenticate = client
            .step(Some(&challenge.expect("a challenge")))
            .unwrap()
            .expect("an authenticate token");
        acceptor.accept(&authenticate).0
    }

    #[test]
    fn integrated_auth_sspi_binds_the_current_account_to_the_channel() {
        let binding = b"tls-server-end-point:0123456789abcdef0123456789abcdef";
        let mut client =
            SspiContext::new(AuthScheme::Ntlm, "localhost", None, Some(&binding[..])).unwrap();
        let mut acceptor = Acceptor::new(Some(&binding[..]));
        // A loopback NTLM exchange is a local call: the acceptor checks the bindings, and the
        // client's context completes with integrity.
        let status = handshake(&mut client, &mut acceptor);
        assert_eq!(status, SEC_E_OK, "{status:#X}");

        // An acceptor on another channel refuses the tokens.
        let mut client =
            SspiContext::new(AuthScheme::Ntlm, "localhost", None, Some(&binding[..])).unwrap();
        let mut acceptor = Acceptor::new(Some(b"tls-server-end-point:another channel"));
        let status = handshake(&mut client, &mut acceptor);
        assert_eq!(status, SEC_E_BAD_BINDINGS, "{status:#X}");
    }

    #[test]
    fn integrated_auth_sspi_accepts_well_formed_managed_ntlm_and_denies_wrong_credentials() {
        // A well-formed message with unknown credentials is denied (SEC_E_LOGON_DENIED); a
        // malformed one would be SEC_E_INVALID_TOKEN.
        let mut client = NtlmContext::new(
            Explicit {
                user: "dxn-signalr-no-such-user".to_string(),
                password: "not the password".to_string(),
                domain: ".".to_string(),
            },
            "HTTP/localhost".to_string(),
            Some(b"tls-server-end-point:binding".to_vec()),
        );
        let mut acceptor = Acceptor::new(None);
        let status = handshake(&mut client, &mut acceptor);
        assert_eq!(status, SEC_E_LOGON_DENIED, "{status:#X}");
    }

    #[test]
    fn integrated_auth_sspi_answers_with_explicit_credentials() {
        // SSPI with explicit credentials produces both tokens (`handshake` requires them), and
        // the acceptor denies the unknown account rather than a malformed exchange.
        let explicit = Explicit {
            user: "dxn-signalr-no-such-user".to_string(),
            password: "not the password".to_string(),
            domain: ".".to_string(),
        };
        let mut client =
            SspiContext::new(AuthScheme::Ntlm, "localhost", Some(&explicit), None).unwrap();
        let mut acceptor = Acceptor::new(None);
        let status = handshake(&mut client, &mut acceptor);
        assert_eq!(status, SEC_E_LOGON_DENIED, "{status:#X}");
    }

    #[test]
    fn integrated_auth_sspi_names_the_canonical_host() {
        assert_eq!(canonical_host("127.0.0.1"), "127.0.0.1");
        assert_eq!(canonical_host("::1"), "::1");
        assert!(!canonical_host("localhost").is_empty());
        assert_eq!(
            canonical_host("no-such-host.invalid"),
            "no-such-host.invalid",
            "a failed lookup keeps the name"
        );
    }
}
