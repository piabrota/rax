//! mTLS credentials for reaching a task through Substrate's atenet router.
//!
//! The router's serving CA comes from the live servicedns
//! ClusterTrustBundle; the client certificate is a short-lived one from the
//! podidentity signer, requested with a PodCertificateRequest for the AX
//! server's service account. Credentials are cached in the tunnel directory,
//! in the same format AX uses, for 30 minutes or until a minute before the
//! client certificate expires, whichever is sooner.

use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::tunnel::{current_context, sanitize_context, tunnel_dir};

const SERVICE_DNS_SIGNER_NAME: &str = "servicedns.podcert.ate.dev/identity";
const POD_IDENTITY_SIGNER_NAME: &str = "podidentity.podcert.ate.dev/identity";
const LIVE_BUNDLE_SELECTOR: &str = "podcert.ate.dev/canarying=live";
/// The name the router's certificate is verified against.
pub const ROUTER_SERVER_NAME: &str = "atenet-router.ate-system.svc";
const CACHE_TTL: Duration = Duration::from_secs(30 * 60);
/// How long before its NotAfter a client certificate is treated as expired,
/// so it is not used for a connection that outlives it.
const ROUTER_CERT_EXPIRY: Duration = Duration::from_secs(60);

#[derive(Debug, Serialize, Deserialize)]
struct CachedRouterTls {
    trust_bundle_pem: String,
    cert_chain_pem: String,
    key_pem: String,
    /// RFC 3339.
    expires_at: String,
}

/// A TLS client configuration for the atenet router, authenticating as
/// `service_account` in `namespace`.
pub fn router_tls_config(
    kube_context: &str,
    namespace: &str,
    service_account: &str,
) -> Result<Arc<ClientConfig>, String> {
    let context = current_context(kube_context).unwrap_or_else(|_| kube_context.to_owned());
    if let Some(cfg) = load_cached(&context, namespace, service_account) {
        return Ok(cfg);
    }
    let trust_pem = service_dns_trust_bundle(&context)
        .map_err(|e| format!("loading servicedns trust bundle: {e}"))?;
    let (chain_pem, key_pem) = request_pod_identity_cert(&context, namespace, service_account)
        .map_err(|e| format!("requesting podidentity client certificate: {e}"))?;
    let config = client_config(&trust_pem, &chain_pem, &key_pem)?;
    let not_after = leaf_not_after(&chain_pem)?;
    if SystemTime::now() >= not_after {
        return Err(format!(
            "issued podidentity client certificate already expired at {}",
            chrono::DateTime::<chrono::Utc>::from(not_after)
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ));
    }
    let expires = chrono::DateTime::<chrono::Utc>::from(cache_expiry(SystemTime::now(), not_after));
    let _ = save_cached(
        &context,
        namespace,
        service_account,
        &CachedRouterTls {
            trust_bundle_pem: trust_pem,
            cert_chain_pem: chain_pem,
            key_pem,
            expires_at: expires.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
        },
    );
    Ok(config)
}

/// Builds a client configuration trusting `trust_pem` and presenting the
/// chain and key, offering h2 for gRPC.
pub fn client_config(
    trust_pem: &str,
    chain_pem: &str,
    key_pem: &str,
) -> Result<Arc<ClientConfig>, String> {
    let mut roots = RootCertStore::empty();
    let certs: Vec<_> = CertificateDer::pem_slice_iter(trust_pem.as_bytes())
        .filter_map(|c| c.ok())
        .collect();
    if roots.add_parsable_certificates(certs).0 == 0 {
        return Err("trust bundle contains no valid certificates".into());
    }
    let chain: Vec<_> = CertificateDer::pem_slice_iter(chain_pem.as_bytes())
        .collect::<Result<_, _>>()
        .map_err(|e| format!("parsing certificate chain: {e}"))?;
    let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes())
        .map_err(|e| format!("parsing private key: {e}"))?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_root_certificates(roots)
        .with_client_auth_cert(chain, key)
        .map_err(|e| format!("loading client certificate: {e}"))?;
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(Arc::new(config))
}

/// When credentials written at `now` for a certificate expiring at
/// `not_after` stop being reused: 30 minutes on, but never past a minute
/// before the certificate expires.
fn cache_expiry(now: SystemTime, not_after: SystemTime) -> SystemTime {
    let ttl = now + CACHE_TTL;
    let cert = not_after
        .checked_sub(ROUTER_CERT_EXPIRY)
        .unwrap_or(SystemTime::UNIX_EPOCH);
    ttl.min(cert)
}

/// Whether a cached certificate expiring at `not_after` may still be used.
fn usable(now: SystemTime, not_after: SystemTime) -> bool {
    now + ROUTER_CERT_EXPIRY <= not_after
}

/// The expiry of the leaf (first) certificate in a PEM chain.
fn leaf_not_after(chain_pem: &str) -> Result<SystemTime, String> {
    let leaf = CertificateDer::pem_slice_iter(chain_pem.as_bytes())
        .next()
        .ok_or("certificate chain is empty")?
        .map_err(|e| format!("parsing leaf certificate: {e}"))?;
    let (_, cert) = x509_parser::parse_x509_certificate(&leaf)
        .map_err(|e| format!("parsing leaf certificate: {e}"))?;
    let secs = cert.validity().not_after.timestamp();
    Ok(if secs >= 0 {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs as u64)
    } else {
        SystemTime::UNIX_EPOCH
    })
}

fn cache_path(context: &str, namespace: &str, sa: &str) -> Option<std::path::PathBuf> {
    let key = sanitize_context(&format!("{context}-{namespace}-{sa}"));
    tunnel_dir()
        .ok()
        .map(|d| d.join(format!("{key}-router-tls.json")))
}

fn load_cached(context: &str, namespace: &str, sa: &str) -> Option<Arc<ClientConfig>> {
    load_cached_from(&cache_path(context, namespace, sa)?)
}

fn load_cached_from(path: &std::path::Path) -> Option<Arc<ClientConfig>> {
    let data = std::fs::read(path).ok()?;
    let cached: CachedRouterTls = serde_json::from_slice(&data).ok()?;
    let expires = chrono::DateTime::parse_from_rfc3339(&cached.expires_at).ok()?;
    if SystemTime::now() > SystemTime::from(expires) {
        return None;
    }
    if !usable(
        SystemTime::now(),
        leaf_not_after(&cached.cert_chain_pem).ok()?,
    ) {
        return None;
    }
    client_config(
        &cached.trust_bundle_pem,
        &cached.cert_chain_pem,
        &cached.key_pem,
    )
    .ok()
}

fn save_cached(context: &str, namespace: &str, sa: &str, cached: &CachedRouterTls) -> Option<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let path = cache_path(context, namespace, sa)?;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .ok()?;
    f.write_all(&serde_json::to_vec(cached).ok()?).ok()
}

fn kubectl(context: &str, args: &[&str]) -> Command {
    let mut cmd = Command::new("kubectl");
    if !context.is_empty() {
        cmd.args(["--context", context]);
    }
    cmd.args(args);
    cmd
}

/// The PEM certificates of every live servicedns ClusterTrustBundle.
fn service_dns_trust_bundle(context: &str) -> Result<String, String> {
    let out = kubectl(
        context,
        &[
            "get",
            "clustertrustbundles.certificates.k8s.io",
            "-l",
            LIVE_BUNDLE_SELECTOR,
            "-o",
            "json",
        ],
    )
    .output()
    .map_err(|e| format!("kubectl get clustertrustbundles: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "kubectl get clustertrustbundles: {} ({})",
            out.status,
            combined(&out).trim()
        ));
    }
    let list: Value = serde_json::from_slice(&out.stdout)
        .map_err(|e| format!("decoding ClusterTrustBundle list: {e}"))?;
    let mut pem = String::new();
    for item in list["items"].as_array().into_iter().flatten() {
        if item["spec"]["signerName"] != SERVICE_DNS_SIGNER_NAME {
            continue;
        }
        let bundle = item["spec"]["trustBundle"].as_str().unwrap_or_default();
        if CertificateDer::pem_slice_iter(bundle.as_bytes())
            .filter_map(|c| c.ok())
            .next()
            .is_none()
        {
            return Err(format!(
                "ClusterTrustBundle {:?} contains no valid certificates",
                item["metadata"]["name"].as_str().unwrap_or_default()
            ));
        }
        pem.push_str(bundle);
        if !bundle.ends_with('\n') {
            pem.push('\n');
        }
    }
    if pem.is_empty() {
        return Err(format!(
            "no live ClusterTrustBundle found for signer {SERVICE_DNS_SIGNER_NAME:?}"
        ));
    }
    Ok(pem)
}

fn combined(out: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

fn random_uuid() -> String {
    let mut b: [u8; 16] = rand::random();
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

/// Requests a client certificate from the podidentity signer and returns the
/// issued chain and the key, both PEM.
fn request_pod_identity_cert(
    context: &str,
    namespace: &str,
    service_account: &str,
) -> Result<(String, String), String> {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
        .map_err(|e| format!("generating key: {e}"))?;
    let mut params = rcgen::CertificateParams::default();
    params.distinguished_name = rcgen::DistinguishedName::new();
    let csr = params
        .serialize_request(&key)
        .map_err(|e| format!("creating CSR: {e}"))?;
    let csr_b64 = base64(csr.der());
    let name = format!(
        "ax-ssh-{}",
        rand::random::<[u8; 4]>()
            .iter()
            .map(|x| format!("{x:02x}"))
            .collect::<String>()
    );

    let mut create_err = String::new();
    let mut created = false;
    for api_version in ["certificates.k8s.io/v1", "certificates.k8s.io/v1beta1"] {
        let manifest = json!({
            "apiVersion": api_version,
            "kind": "PodCertificateRequest",
            "metadata": {"name": name, "namespace": namespace},
            "spec": {
                "signerName": POD_IDENTITY_SIGNER_NAME,
                "podName": name,
                "podUID": random_uuid(),
                "serviceAccountName": service_account,
                "serviceAccountUID": random_uuid(),
                "nodeName": "ax-cli",
                "nodeUID": random_uuid(),
                "maxExpirationSeconds": 3600,
                "stubPKCS10Request": csr_b64,
            },
        });
        let mut child = kubectl(context, &["create", "-f", "-"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("kubectl create: {e}"))?;
        let _ = child
            .stdin
            .take()
            .unwrap()
            .write_all(manifest.to_string().as_bytes());
        let out = child
            .wait_with_output()
            .map_err(|e| format!("kubectl create: {e}"))?;
        if out.status.success() {
            created = true;
            break;
        }
        create_err = format!("{} ({})", out.status, combined(&out).trim());
    }
    if !created {
        return Err(format!(
            "creating PodCertificateRequest {namespace}/{name}: {create_err}"
        ));
    }

    let result = poll_issued(context, namespace, &name);
    let _ = kubectl(
        context,
        &[
            "-n",
            namespace,
            "delete",
            "podcertificaterequests.certificates.k8s.io",
            &name,
            "--ignore-not-found",
        ],
    )
    .stdout(Stdio::null())
    .stderr(Stdio::null())
    .status();
    let chain = result?;
    Ok((chain, key.serialize_pem()))
}

fn poll_issued(context: &str, namespace: &str, name: &str) -> Result<String, String> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let out = kubectl(
            context,
            &[
                "-n",
                namespace,
                "get",
                "podcertificaterequests.certificates.k8s.io",
                name,
                "-o",
                "json",
            ],
        )
        .output();
        if let Ok(out) = out
            && out.status.success()
            && let Ok(pcr) = serde_json::from_slice::<Value>(&out.stdout)
        {
            let status = &pcr["status"];
            if let Some(chain) = status["certificateChain"].as_str()
                && !chain.is_empty()
            {
                return Ok(chain.to_owned());
            }
            for c in status["conditions"].as_array().into_iter().flatten() {
                let t = c["type"].as_str().unwrap_or_default();
                if c["status"] == "True" && (t == "Denied" || t == "Failed") {
                    return Err(format!(
                        "PodCertificateRequest {t}: {}: {}",
                        c["reason"].as_str().unwrap_or_default(),
                        c["message"].as_str().unwrap_or_default()
                    ));
                }
            }
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "waiting for PodCertificateRequest {namespace}/{name} to be issued: context deadline exceeded"
            ));
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn base64(data: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let n = chunk.iter().fold(0u32, |n, b| n << 8 | *b as u32) << (8 * (3 - chunk.len()));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(ALPHABET[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_base64() {
        for (input, want) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(base64(input.as_bytes()), want);
        }
    }

    #[test]
    fn uuids_are_v4() {
        let u = random_uuid();
        assert_eq!(u.len(), 36);
        assert_eq!(&u[14..15], "4");
        assert!("89ab".contains(&u[19..20]));
    }

    fn cert_expiring_at(not_after: SystemTime) -> String {
        cert_for(&rcgen::KeyPair::generate().unwrap(), not_after)
    }

    fn cert_for(key: &rcgen::KeyPair, not_after: SystemTime) -> String {
        let mut params = rcgen::CertificateParams::new(vec!["client".into()]).unwrap();
        let not_after = chrono::DateTime::<chrono::Utc>::from(not_after).timestamp();
        params.not_before = rcgen::date_time_ymd(2000, 1, 1);
        params.not_after = time::OffsetDateTime::from_unix_timestamp(not_after).unwrap();
        params.self_signed(key).unwrap().pem()
    }

    #[test]
    fn reads_leaf_expiry() {
        let at = SystemTime::UNIX_EPOCH + Duration::from_secs(2_000_000_000);
        assert_eq!(leaf_not_after(&cert_expiring_at(at)).unwrap(), at);
        assert!(leaf_not_after("").is_err());
        assert!(
            leaf_not_after("-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n")
                .is_err()
        );
    }

    #[test]
    fn caches_until_the_certificate_expires() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let min = Duration::from_secs(60);
        // A long-lived certificate is cached for 30 minutes.
        assert_eq!(cache_expiry(now, now + 60 * min), now + CACHE_TTL);
        // A short-lived one until a minute before it expires.
        assert_eq!(cache_expiry(now, now + 10 * min), now + 9 * min);
        assert!(cache_expiry(now, now + min / 2) < now);
    }

    #[test]
    fn ignores_cached_certificates_near_expiry() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let min = Duration::from_secs(60);
        assert!(usable(now, now + 10 * min));
        assert!(usable(now, now + min));
        // Within the margin, or expired.
        assert!(!usable(now, now + min / 2));
        assert!(!usable(now, now - min));
    }

    #[test]
    fn rejects_expired_cache_entries() {
        let dir = tempfile::tempdir().unwrap();
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca = rcgen::CertificateParams::new(vec![ROUTER_SERVER_NAME.into()])
            .unwrap()
            .self_signed(&ca_key)
            .unwrap();
        let key = rcgen::KeyPair::generate().unwrap();
        for (not_after, want) in [
            (SystemTime::now() + Duration::from_secs(3600), true),
            (SystemTime::now() + Duration::from_secs(30), false),
            (SystemTime::now() - Duration::from_secs(3600), false),
        ] {
            let cached = CachedRouterTls {
                trust_bundle_pem: ca.pem(),
                cert_chain_pem: cert_for(&key, not_after),
                key_pem: key.serialize_pem(),
                // The cache entry itself has not expired; only the
                // certificate may have.
                expires_at: chrono::DateTime::<chrono::Utc>::from(SystemTime::now() + CACHE_TTL)
                    .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
            };
            let path = dir.path().join("cache.json");
            std::fs::write(&path, serde_json::to_vec(&cached).unwrap()).unwrap();
            assert_eq!(load_cached_from(&path).is_some(), want, "{not_after:?}");
        }
    }

    #[test]
    fn builds_client_configs() {
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca = rcgen::CertificateParams::new(vec![ROUTER_SERVER_NAME.into()])
            .unwrap()
            .self_signed(&ca_key)
            .unwrap();
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = rcgen::CertificateParams::new(vec!["client".into()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        let cfg = client_config(&ca.pem(), &cert.pem(), &key.serialize_pem()).unwrap();
        assert_eq!(cfg.alpn_protocols, [b"h2".to_vec()]);
        assert!(client_config("junk", &cert.pem(), &key.serialize_pem()).is_err());
    }
}
