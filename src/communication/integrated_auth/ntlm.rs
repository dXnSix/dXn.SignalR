//! NTLMv2 (MS-NLMP) for explicit credentials where SSPI is not available, as .NET's managed NTLM
//! implementation: the negotiate, challenge and authenticate messages of HTTP NTLM authentication
//! with signing negotiated and a random session key exchanged, the target name and channel
//! bindings in the target info, and the message integrity code when the server sends a
//! timestamp.

// Message parsing must not panic on server input; tests may.
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

use hmac::{Hmac, Mac};
use md4::{Digest, Md4};
use rc4::{KeyInit, Rc4, StreamCipher};

use super::{random, Explicit, SecurityContext};
use crate::error::Error;

type HmacMd5 = Hmac<md5::Md5>;

const SIGNATURE: &[u8; 8] = b"NTLMSSP\0";
const NEGOTIATE_UNICODE: u32 = 0x0000_0001;
const REQUEST_TARGET: u32 = 0x0000_0004;
const NEGOTIATE_SIGN: u32 = 0x0000_0010;
const NEGOTIATE_NTLM: u32 = 0x0000_0200;
const NEGOTIATE_ALWAYS_SIGN: u32 = 0x0000_8000;
const NEGOTIATE_EXTENDED_SESSIONSECURITY: u32 = 0x0008_0000;
const NEGOTIATE_TARGET_INFO: u32 = 0x0080_0000;
const NEGOTIATE_VERSION: u32 = 0x0200_0000;
const NEGOTIATE_128: u32 = 0x2000_0000;
const NEGOTIATE_KEY_EXCH: u32 = 0x4000_0000;
const NEGOTIATE_56: u32 = 0x8000_0000;

/// The flags of the negotiate message: HTTP authentication requires signing, as .NET's
/// `ProtectionLevel.Sign`.
const NEGOTIATE_FLAGS: u32 = NEGOTIATE_UNICODE
    | REQUEST_TARGET
    | NEGOTIATE_SIGN
    | NEGOTIATE_NTLM
    | NEGOTIATE_ALWAYS_SIGN
    | NEGOTIATE_EXTENDED_SESSIONSECURITY
    | NEGOTIATE_TARGET_INFO
    | NEGOTIATE_VERSION
    | NEGOTIATE_128
    | NEGOTIATE_KEY_EXCH
    | NEGOTIATE_56;

/// The flags the server's challenge must keep.
const REQUIRED_FLAGS: u32 = NEGOTIATE_UNICODE
    | NEGOTIATE_NTLM
    | NEGOTIATE_SIGN
    | NEGOTIATE_EXTENDED_SESSIONSECURITY
    | NEGOTIATE_TARGET_INFO;

/// The VERSION structure .NET's managed NTLM sends: 6.1.7600, NTLM revision 15.
const VERSION: [u8; 8] = [6, 1, 0xb0, 0x1d, 0, 0, 0, 0x0f];

/// MS-NLMP AV pair ids.
const AV_EOL: u16 = 0;
const AV_FLAGS: u16 = 6;
const AV_TIMESTAMP: u16 = 7;
const AV_TARGET_NAME: u16 = 9;
const AV_CHANNEL_BINDINGS: u16 = 10;
/// MsvAvFlags: the authenticate message carries a MIC.
const AV_FLAG_MIC: u32 = 0x0000_0002;

/// One NTLM authentication: the negotiate message, then the authenticate message answering the
/// server's challenge.
pub(crate) struct NtlmContext {
    credentials: Explicit,
    /// The SPN (`HTTP/<host>`), sent as MsvAvTargetName.
    target_name: String,
    /// The RFC 5929 channel binding (TLS only), hashed into MsvAvChannelBindings.
    binding: Option<Vec<u8>>,
    negotiate: Option<Vec<u8>>,
    finished: bool,
}

impl NtlmContext {
    pub(crate) fn new(
        credentials: Explicit,
        target_name: String,
        binding: Option<Vec<u8>>,
    ) -> Self {
        Self {
            credentials,
            target_name,
            binding,
            negotiate: None,
            finished: false,
        }
    }
}

impl SecurityContext for NtlmContext {
    fn step(&mut self, challenge: Option<&[u8]>) -> Result<Option<Vec<u8>>, Error> {
        if self.finished {
            return Ok(None);
        }
        match (challenge, &self.negotiate) {
            (None, None) => {
                let message = negotiate_message();
                self.negotiate = Some(message.clone());
                Ok(Some(message))
            }
            (Some(challenge), Some(negotiate)) => {
                let message = authenticate_message(
                    &Answer {
                        credentials: &self.credentials,
                        target_name: &self.target_name,
                        binding: self.binding.as_deref(),
                        negotiate,
                        challenge,
                    },
                    random()?,
                    random()?,
                    now_filetime(),
                )?;
                self.finished = true;
                Ok(Some(message))
            }
            _ => Err(failure("the server sent an NTLM challenge out of sequence")),
        }
    }
}

fn failure(message: &str) -> Error {
    Error::authentication(None, format!("NTLM authentication failed: {message}"))
}

/// The NEGOTIATE_MESSAGE (MS-NLMP 2.2.1.1), without domain or workstation, with the version.
fn negotiate_message() -> Vec<u8> {
    let mut message = Vec::with_capacity(40);
    message.extend_from_slice(SIGNATURE);
    message.extend_from_slice(&1u32.to_le_bytes());
    message.extend_from_slice(&NEGOTIATE_FLAGS.to_le_bytes());
    // Empty domain and workstation fields, pointing at the end of the message.
    for _ in 0..2 {
        message.extend_from_slice(&[0, 0, 0, 0]);
        message.extend_from_slice(&40u32.to_le_bytes());
    }
    message.extend_from_slice(&VERSION);
    message
}

/// The fields of a CHALLENGE_MESSAGE (MS-NLMP 2.2.1.2) the response needs.
struct Challenge {
    flags: u32,
    server_challenge: [u8; 8],
    target_info: Vec<u8>,
}

fn read_u16(bytes: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_le_bytes(
        bytes.get(at..at.checked_add(2)?)?.try_into().ok()?,
    ))
}

fn read_u32(bytes: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        bytes.get(at..at.checked_add(4)?)?.try_into().ok()?,
    ))
}

fn parse_challenge(message: &[u8]) -> Result<Challenge, Error> {
    let malformed = || failure("the server's challenge is malformed");
    if message.get(..8) != Some(SIGNATURE.as_slice()) || read_u32(message, 8) != Some(2) {
        return Err(malformed());
    }
    let flags = read_u32(message, 20).ok_or_else(malformed)?;
    let server_challenge: [u8; 8] = message
        .get(24..32)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(malformed)?;
    let target_info = match (read_u16(message, 40), read_u32(message, 44)) {
        (Some(length), Some(offset)) if flags & NEGOTIATE_TARGET_INFO != 0 => {
            let start = usize::try_from(offset).map_err(|_| malformed())?;
            let end = start
                .checked_add(usize::from(length))
                .ok_or_else(malformed)?;
            message.get(start..end).ok_or_else(malformed)?.to_vec()
        }
        _ => Vec::new(),
    };
    Ok(Challenge {
        flags,
        server_challenge,
        target_info,
    })
}

/// The AV pairs of `target_info` (id, value), up to MsvAvEOL.
fn av_pairs(target_info: &[u8]) -> Vec<(u16, &[u8])> {
    let mut pairs = Vec::new();
    let mut at = 0usize;
    while let (Some(id), Some(length)) = (read_u16(target_info, at), read_u16(target_info, at + 2))
    {
        if id == AV_EOL {
            break;
        }
        let start = at + 4;
        let Some(value) = target_info.get(start..start + usize::from(length)) else {
            break;
        };
        pairs.push((id, value));
        at = start + usize::from(length);
    }
    pairs
}

fn utf16(text: &str) -> Vec<u8> {
    text.encode_utf16().flat_map(u16::to_le_bytes).collect()
}

fn hmac_md5(key: &[u8], parts: &[&[u8]]) -> [u8; 16] {
    let mut mac = <HmacMd5 as Mac>::new_from_slice(key).unwrap_or_else(|_| {
        // HMAC accepts keys of any length.
        unreachable!("HMAC-MD5 takes any key")
    });
    for part in parts {
        mac.update(part);
    }
    mac.finalize().into_bytes().into()
}

/// NTOWFv2 (MS-NLMP 3.3.2): HMAC-MD5 of the user and domain under the MD4 password hash.
fn response_key(credentials: &Explicit) -> [u8; 16] {
    let password_hash: [u8; 16] = Md4::digest(utf16(&credentials.password)).into();
    let identity = utf16(&format!(
        "{}{}",
        credentials.user.to_uppercase(),
        credentials.domain
    ));
    hmac_md5(&password_hash, &[&identity])
}

/// The NTLMv2 client blob ("temp" in MS-NLMP 3.3.2).
fn client_blob(timestamp: u64, client_challenge: [u8; 8], target_info: &[u8]) -> Vec<u8> {
    let mut blob = vec![0x01, 0x01, 0, 0, 0, 0, 0, 0];
    blob.extend_from_slice(&timestamp.to_le_bytes());
    blob.extend_from_slice(&client_challenge);
    blob.extend_from_slice(&[0; 4]);
    blob.extend_from_slice(target_info);
    blob.extend_from_slice(&[0; 4]);
    blob
}

/// The NTLMv2 proof (NTProofStr) of `blob` for `server_challenge`.
fn proof(response_key: &[u8; 16], server_challenge: &[u8; 8], blob: &[u8]) -> [u8; 16] {
    hmac_md5(response_key, &[server_challenge, blob])
}

/// MsvAvChannelBindings: the MD5 of the `gss_channel_bindings_struct` carrying `binding` as its
/// application data (no addresses), or zeros without a binding (MS-NLMP 3.1.5.1.2).
fn channel_bindings_hash(binding: Option<&[u8]>) -> [u8; 16] {
    let Some(binding) = binding else {
        return [0; 16];
    };
    let mut hash = md5::Md5::new();
    // Initiator and acceptor address types and lengths, then the application data's length.
    hash.update([0u8; 16]);
    hash.update(
        u32::try_from(binding.len())
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    hash.update(binding);
    hash.finalize().into()
}

fn push_av_pair(info: &mut Vec<u8>, id: u16, value: &[u8]) {
    info.extend_from_slice(&id.to_le_bytes());
    info.extend_from_slice(&u16::try_from(value.len()).unwrap_or(u16::MAX).to_le_bytes());
    info.extend_from_slice(value);
}

/// The target info the client returns, as .NET's managed NTLM builds it: the server's pairs,
/// then MsvAvFlags (with the MIC bit when the server sent a timestamp), MsvAvChannelBindings and
/// MsvAvTargetName.
fn client_target_info(
    target_info: &[u8],
    with_mic: bool,
    target_name: &str,
    binding: Option<&[u8]>,
) -> Vec<u8> {
    let target_name = utf16(target_name);
    let mut info = Vec::with_capacity(target_info.len() + target_name.len() + 40);
    let mut flags = if with_mic { AV_FLAG_MIC } else { 0 };
    for (id, value) in av_pairs(target_info) {
        match id {
            AV_FLAGS => {
                flags |= value
                    .get(..4)
                    .and_then(|bytes| bytes.try_into().ok())
                    .map_or(0, u32::from_le_bytes);
            }
            AV_TARGET_NAME | AV_CHANNEL_BINDINGS => {}
            _ => push_av_pair(&mut info, id, value),
        }
    }
    if flags != 0 {
        push_av_pair(&mut info, AV_FLAGS, &flags.to_le_bytes());
    }
    push_av_pair(
        &mut info,
        AV_CHANNEL_BINDINGS,
        &channel_bindings_hash(binding),
    );
    push_av_pair(&mut info, AV_TARGET_NAME, &target_name);
    info.extend_from_slice(&[0; 4]);
    info
}

/// What the authenticate message answers.
struct Answer<'a> {
    credentials: &'a Explicit,
    target_name: &'a str,
    binding: Option<&'a [u8]>,
    negotiate: &'a [u8],
    challenge: &'a [u8],
}

/// The AUTHENTICATE_MESSAGE (MS-NLMP 2.2.1.3) answering the challenge, with `client_challenge`
/// and, under key exchange, `exported_key` as the session key.
fn authenticate_message(
    answer: &Answer<'_>,
    client_challenge: [u8; 8],
    exported_key: [u8; 16],
    now: u64,
) -> Result<Vec<u8>, Error> {
    let challenge = parse_challenge(answer.challenge)?;
    if challenge.flags & REQUIRED_FLAGS != REQUIRED_FLAGS {
        return Err(failure(
            "the server does not support NTLMv2 with Unicode and signing",
        ));
    }
    let server_timestamp = av_pairs(&challenge.target_info)
        .into_iter()
        .find(|(id, _)| *id == AV_TIMESTAMP)
        .and_then(|(_, value)| value.get(..8)?.try_into().ok())
        .map(u64::from_le_bytes);
    // With the server's timestamp the client sends a MIC and no LMv2 response (MS-NLMP 3.1.5.1.2).
    let with_mic = server_timestamp.is_some();
    let target_info = client_target_info(
        &challenge.target_info,
        with_mic,
        answer.target_name,
        answer.binding,
    );
    let key = response_key(answer.credentials);
    let blob = client_blob(
        server_timestamp.unwrap_or(now),
        client_challenge,
        &target_info,
    );
    let nt_proof = proof(&key, &challenge.server_challenge, &blob);
    let mut nt_response = nt_proof.to_vec();
    nt_response.extend_from_slice(&blob);
    let lm_response = if with_mic {
        vec![0; 24]
    } else {
        let mut lm = hmac_md5(&key, &[&challenge.server_challenge, &client_challenge]).to_vec();
        lm.extend_from_slice(&client_challenge);
        lm
    };
    // NTLMv2's key exchange key is the session base key; under key exchange the random exported
    // key is sent encrypted with it (MS-NLMP 3.1.5.1.2).
    let session_base_key = hmac_md5(&key, &[&nt_proof]);
    let flags = (challenge.flags & NEGOTIATE_FLAGS) | NEGOTIATE_UNICODE;
    let (session_key, encrypted_key) = if flags & NEGOTIATE_KEY_EXCH != 0 {
        let mut encrypted = exported_key;
        Rc4::new(&session_base_key.into()).apply_keystream(&mut encrypted);
        (exported_key, encrypted.to_vec())
    } else {
        (session_base_key, Vec::new())
    };

    let domain = utf16(&answer.credentials.domain);
    let user = utf16(&answer.credentials.user);
    let workstation: Vec<u8> = Vec::new();
    // Header, version and MIC, then the payload.
    const HEADER: usize = 88;
    let mut message = Vec::with_capacity(
        HEADER + lm_response.len() + nt_response.len() + domain.len() + user.len() + 16,
    );
    message.extend_from_slice(SIGNATURE);
    message.extend_from_slice(&3u32.to_le_bytes());
    let too_long = || failure("the credentials are too long for NTLM");
    let mut offset = HEADER;
    let mut payload = Vec::new();
    for field in [
        &lm_response,
        &nt_response,
        &domain,
        &user,
        &workstation,
        &encrypted_key,
    ] {
        let length = u16::try_from(field.len()).map_err(|_| too_long())?;
        message.extend_from_slice(&length.to_le_bytes());
        message.extend_from_slice(&length.to_le_bytes());
        message.extend_from_slice(&u32::try_from(offset).map_err(|_| too_long())?.to_le_bytes());
        payload.extend_from_slice(field);
        offset += field.len();
    }
    message.extend_from_slice(&flags.to_le_bytes());
    message.extend_from_slice(&VERSION);
    message.extend_from_slice(&[0; 16]); // MIC, filled in below.
    message.extend_from_slice(&payload);
    if with_mic {
        let mic = hmac_md5(
            &session_key,
            &[answer.negotiate, answer.challenge, &message],
        );
        if let Some(slot) = message.get_mut(72..88) {
            slot.copy_from_slice(&mic);
        }
    }
    Ok(message)
}

/// Now as a Windows FILETIME (100 ns since 1601-01-01).
fn now_filetime() -> u64 {
    const EPOCH_DIFFERENCE: u64 = 11_644_473_600;
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| {
            (since.as_secs() + EPOCH_DIFFERENCE) * 10_000_000
                + u64::from(since.subsec_nanos() / 100)
        })
}

#[cfg(test)]
mod integrated_auth_ntlm {
    use super::*;

    /// MS-NLMP 4.2.4: User / Domain / Password, server challenge 0123456789abcdef, client
    /// challenge aaaaaaaaaaaaaaaa, time 0, and its target info (NetBIOS domain then server name).
    fn credentials() -> Explicit {
        Explicit {
            user: "User".to_string(),
            password: "Password".to_string(),
            domain: "Domain".to_string(),
        }
    }

    const SERVER_CHALLENGE: [u8; 8] = [0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef];
    const CLIENT_CHALLENGE: [u8; 8] = [0xaa; 8];
    const EXPORTED_KEY: [u8; 16] = [0x55; 16];

    fn spec_target_info() -> Vec<u8> {
        let mut info = vec![0x02, 0x00, 0x0c, 0x00];
        info.extend(utf16("Domain"));
        info.extend([0x01, 0x00, 0x0c, 0x00]);
        info.extend(utf16("Server"));
        info.extend([0, 0, 0, 0]);
        info
    }

    fn hex(text: &str) -> Vec<u8> {
        text.split_whitespace()
            .map(|byte| u8::from_str_radix(byte, 16).unwrap())
            .collect()
    }

    #[test]
    fn integrated_auth_ntlm_matches_the_ms_nlmp_ntlmv2_test_vectors() {
        let key = response_key(&credentials());
        assert_eq!(
            key.to_vec(),
            hex("0c 86 8a 40 3b fd 7a 93 a3 00 1e f2 2e f0 2e 3f"),
            "NTOWFv2"
        );
        let blob = client_blob(0, CLIENT_CHALLENGE, &spec_target_info());
        let nt_proof = proof(&key, &SERVER_CHALLENGE, &blob);
        assert_eq!(
            nt_proof.to_vec(),
            hex("68 cd 0a b8 51 e5 1c 96 aa bc 92 7b eb ef 6a 1c"),
            "NTProofStr"
        );
        assert_eq!(
            hmac_md5(&key, &[&nt_proof]).to_vec(),
            hex("8d e4 0c ca db c1 4a 82 f1 5c b0 ad 0d e9 5c a3"),
            "SessionBaseKey"
        );
        let mut lm = hmac_md5(&key, &[&SERVER_CHALLENGE, &CLIENT_CHALLENGE]).to_vec();
        lm.extend_from_slice(&CLIENT_CHALLENGE);
        assert_eq!(
            lm,
            hex("86 c3 50 97 ac 9c ec 10 25 54 76 4a 57 cc cc 19 aa aa aa aa aa aa aa aa"),
            "LMv2 response"
        );
    }

    #[test]
    fn integrated_auth_ntlm_encrypts_the_exported_key_as_ms_nlmp() {
        // MS-NLMP 4.2.2.1.3 (NTLMv1 vectors): RC4 under the key exchange key
        // d87262b0cde4b1cb7499becccdf10784 of the random session key 55555555555555555555555555555555.
        let mut encrypted = EXPORTED_KEY;
        let key: [u8; 16] = hex("d8 72 62 b0 cd e4 b1 cb 74 99 be cc cd f1 07 84")
            .try_into()
            .unwrap();
        Rc4::new(&key.into()).apply_keystream(&mut encrypted);
        assert_eq!(
            encrypted.to_vec(),
            hex("51 88 22 b1 b3 f3 50 c8 95 86 82 ec bb 3e 3c b7")
        );
        // MS-NLMP 4.2.4.2.3 (NTLMv2): under the session base key 8de40ccadbc14a82f15cb0ad0de95ca3.
        let mut encrypted = EXPORTED_KEY;
        let key: [u8; 16] = hex("8d e4 0c ca db c1 4a 82 f1 5c b0 ad 0d e9 5c a3")
            .try_into()
            .unwrap();
        Rc4::new(&key.into()).apply_keystream(&mut encrypted);
        assert_eq!(
            encrypted.to_vec(),
            hex("c5 da d2 54 4f c9 79 90 94 ce 1c e9 0b c9 d0 3e")
        );
    }

    /// A CHALLENGE_MESSAGE with `target_info`.
    fn challenge_message(flags: u32, target_info: &[u8]) -> Vec<u8> {
        let mut message = SIGNATURE.to_vec();
        message.extend(2u32.to_le_bytes());
        message.extend([0, 0, 0, 0, 56, 0, 0, 0]); // empty target name at 56
        message.extend(flags.to_le_bytes());
        message.extend(SERVER_CHALLENGE);
        message.extend([0; 8]);
        message.extend((target_info.len() as u16).to_le_bytes());
        message.extend((target_info.len() as u16).to_le_bytes());
        message.extend(56u32.to_le_bytes());
        message.extend([0; 8]); // version
        message.extend(target_info);
        message
    }

    /// A field (length at `at`, offset at `at + 4`) of an authenticate message.
    fn field(message: &[u8], at: usize) -> &[u8] {
        let length = usize::from(read_u16(message, at).unwrap());
        let offset = read_u32(message, at + 4).unwrap() as usize;
        &message[offset..offset + length]
    }

    fn answer(binding: Option<&[u8]>, target_info: &[u8]) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let negotiate = negotiate_message();
        let challenge = challenge_message(NEGOTIATE_FLAGS, target_info);
        let credentials = credentials();
        let authenticate = authenticate_message(
            &Answer {
                credentials: &credentials,
                target_name: "HTTP/server.example.com",
                binding,
                negotiate: &negotiate,
                challenge: &challenge,
            },
            CLIENT_CHALLENGE,
            EXPORTED_KEY,
            0,
        )
        .unwrap();
        (negotiate, challenge, authenticate)
    }

    #[test]
    fn integrated_auth_ntlm_negotiates_signing_key_exchange_and_the_version() {
        let negotiate = negotiate_message();
        assert_eq!(negotiate.len(), 40);
        let flags = read_u32(&negotiate, 12).unwrap();
        for flag in [NEGOTIATE_SIGN, NEGOTIATE_KEY_EXCH, NEGOTIATE_VERSION] {
            assert_ne!(flags & flag, 0, "{flag:#x}");
        }
        assert_eq!(&negotiate[32..40], &VERSION);

        let (_, _, authenticate) = answer(None, &spec_target_info());
        let flags = read_u32(&authenticate, 60).unwrap();
        assert_ne!(flags & NEGOTIATE_SIGN, 0);
        assert_ne!(flags & NEGOTIATE_KEY_EXCH, 0);
        assert_eq!(&authenticate[64..72], &VERSION);
        // The exported key goes out encrypted under the session base key.
        let key = response_key(&credentials());
        let nt_proof = &field(&authenticate, 20)[..16];
        let mut exported: [u8; 16] = field(&authenticate, 52).try_into().unwrap();
        Rc4::new(&hmac_md5(&key, &[nt_proof]).into()).apply_keystream(&mut exported);
        assert_eq!(exported, EXPORTED_KEY);
    }

    #[test]
    fn integrated_auth_ntlm_sends_the_target_name_and_channel_bindings() {
        let binding = b"tls-server-end-point:0123456789abcdef0123456789abcdef";
        for (given, expected) in [
            (None, [0u8; 16]),
            (Some(&binding[..]), {
                // The gss_channel_bindings_struct without addresses, then the application data.
                let mut structure = vec![0u8; 16];
                structure.extend((binding.len() as u32).to_le_bytes());
                structure.extend(binding);
                md5::Md5::digest(&structure).into()
            }),
        ] {
            assert_eq!(channel_bindings_hash(given), expected);
            let (_, _, authenticate) = answer(given, &spec_target_info());
            let response = field(&authenticate, 20);
            let blob = &response[16..];
            let pairs = av_pairs(&blob[28..]);
            assert!(
                pairs.contains(&(AV_CHANNEL_BINDINGS, &expected[..])),
                "{pairs:?}"
            );
            assert!(pairs.contains(&(AV_TARGET_NAME, utf16("HTTP/server.example.com").as_slice())));
            // The server's pairs are kept.
            assert!(pairs.contains(&(2, utf16("Domain").as_slice())));
        }
    }

    #[test]
    fn integrated_auth_ntlm_negotiates_then_answers_the_challenge() {
        let mut context = NtlmContext::new(credentials(), "HTTP/server".to_string(), None);
        let negotiate = context.step(None).unwrap().unwrap();
        assert_eq!(&negotiate[..8], SIGNATURE);
        assert_eq!(read_u32(&negotiate, 8), Some(1));
        let challenge = challenge_message(NEGOTIATE_FLAGS, &spec_target_info());
        let authenticate = context.step(Some(&challenge)).unwrap().unwrap();
        assert_eq!(read_u32(&authenticate, 8), Some(3));
        // The NT response field holds NTProofStr then the blob.
        let response = field(&authenticate, 20);
        let key = response_key(&credentials());
        let blob = &response[16..];
        assert_eq!(&response[..16], &proof(&key, &SERVER_CHALLENGE, blob));
        // The user is UTF-16.
        assert_eq!(field(&authenticate, 36), utf16("User"));
        assert_eq!(context.step(None).unwrap(), None, "finished");
    }

    #[test]
    fn integrated_auth_ntlm_adds_a_mic_when_the_server_sends_a_timestamp() {
        let mut info = vec![0x02, 0x00, 0x0c, 0x00];
        info.extend(utf16("Domain"));
        info.extend([0x07, 0x00, 0x08, 0x00]);
        info.extend(42u64.to_le_bytes());
        info.extend([0, 0, 0, 0]);
        let (negotiate, challenge, authenticate) = answer(None, &info);
        // LMv2 is zero, the blob carries the server's timestamp and MsvAvFlags with the MIC bit.
        assert_eq!(field(&authenticate, 12), &[0; 24]);
        let blob = &field(&authenticate, 20)[16..];
        assert_eq!(&blob[8..16], &42u64.to_le_bytes());
        let returned = &blob[28..blob.len() - 4];
        assert!(av_pairs(returned)
            .iter()
            .any(|(id, value)| *id == AV_FLAGS && *value == AV_FLAG_MIC.to_le_bytes()));
        // The MIC covers the three messages with the MIC field zeroed, under the exported key.
        let mut zeroed = authenticate.clone();
        zeroed[72..88].fill(0);
        assert_eq!(
            &authenticate[72..88],
            &hmac_md5(&EXPORTED_KEY, &[&negotiate, &challenge, &zeroed])
        );
    }

    #[test]
    fn integrated_auth_ntlm_rejects_malformed_or_unexpected_challenges() {
        let mut context = NtlmContext::new(credentials(), "HTTP/server".to_string(), None);
        assert!(context.step(Some(b"NTLMSSP\0")).is_err(), "out of sequence");
        context.step(None).unwrap();
        let mut overflowing = challenge_message(NEGOTIATE_FLAGS, &spec_target_info());
        overflowing[44..48].copy_from_slice(&u32::MAX.to_le_bytes());
        let unsigned = challenge_message(NEGOTIATE_FLAGS & !NEGOTIATE_SIGN, &spec_target_info());
        for malformed in [
            &b"not ntlm"[..],
            &b"NTLMSSP\0\x02\0\0\0"[..],
            &overflowing,
            &unsigned,
        ] {
            let error = NtlmContext {
                negotiate: Some(negotiate_message()),
                ..NtlmContext::new(credentials(), "HTTP/server".to_string(), None)
            }
            .step(Some(malformed))
            .unwrap_err();
            assert_eq!(error.kind(), crate::ErrorKind::Authentication, "{error}");
            assert!(!error.to_string().contains("Password"), "{error}");
        }
    }
}
