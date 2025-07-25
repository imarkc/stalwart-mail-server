/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    path::PathBuf,
    sync::Arc,
};

use common::{
    Core, Data, Inner, Server,
    config::{
        server::{Listeners, ServerProtocol},
        smtp::resolver::Tlsa,
        spamfilter::IpResolver,
    },
    ipc::{QueueEvent, ReportingEvent},
    manager::boot::{IpcReceivers, build_ipc},
};

use http::HttpSessionManager;
use mail_auth::{MX, Txt, common::resolver::IntoFqdn};
use session::{DummyIo, TestSession};
use smtp::core::{Session, SmtpSessionManager};
use store::{BlobStore, Store, Stores};
use tokio::sync::{mpsc, watch};
use utils::config::Config;

use crate::AssertConfig;

pub mod config;
pub mod inbound;
pub mod lookup;
pub mod management;
pub mod outbound;
pub mod queue;
pub mod reporting;
pub mod session;

pub struct TempDir {
    pub temp_dir: PathBuf,
    pub delete: bool,
}

impl TempDir {
    pub fn new(name: &str, delete: bool) -> TempDir {
        let mut temp_dir = std::env::temp_dir();
        temp_dir.push(name);
        if !temp_dir.exists() {
            let _ = std::fs::create_dir(&temp_dir);
        } else if delete {
            let _ = std::fs::remove_dir_all(&temp_dir);
            let _ = std::fs::create_dir(&temp_dir);
        }
        TempDir { temp_dir, delete }
    }

    pub fn update_config(&self, config: impl AsRef<str>) -> String {
        config
            .as_ref()
            .replace("{TMP}", self.temp_dir.to_str().unwrap())
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        if self.delete {
            let _ = std::fs::remove_dir_all(&self.temp_dir);
        }
    }
}

pub fn add_test_certs(config: &str) -> String {
    let mut cert_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    cert_path.push("resources");
    cert_path.push("smtp");
    cert_path.push("certs");
    let mut cert = cert_path.clone();
    cert.push("tls_cert.pem");
    let mut pk = cert_path.clone();
    pk.push("tls_privatekey.pem");

    config
        .replace("{CERT}", cert.as_path().to_str().unwrap())
        .replace("{PK}", pk.as_path().to_str().unwrap())
}

pub struct QueueReceiver {
    store: Store,
    blob_store: BlobStore,
    pub queue_rx: mpsc::Receiver<QueueEvent>,
}

pub struct ReportReceiver {
    pub report_rx: mpsc::Receiver<ReportingEvent>,
}

pub struct TestSMTP {
    pub server: Server,
    pub temp_dir: Option<TempDir>,
    pub queue_receiver: QueueReceiver,
    pub report_receiver: ReportReceiver,
}

const CONFIG: &str = r#"
[session.connect]
hostname = "'mx.example.org'"
greeting = "'Test SMTP instance'"

[server.listener.smtp-debug]
bind = ['127.0.0.1:9925']
protocol = 'smtp'

[server.listener.lmtp-debug]
bind = ['127.0.0.1:9924']
protocol = 'lmtp'
tls.implicit = true

[server.listener.management-debug]
bind = ['127.0.0.1:9980']
protocol = 'http'
tls.implicit = true

[server.socket]
reuse-addr = true

[server.tls]
enable = true
implicit = false
certificate = 'default'

[certificate.default]
cert = '%{file:{CERT}}%'
private-key = '%{file:{PK}}%'

[storage]
data = "{STORE}"
fts = "{STORE}"
blob = "{STORE}"
lookup = "{STORE}"

[store."rocksdb"]
type = "rocksdb"
path = "{TMP}/queue.db"

#[store."foundationdb"]
#type = "foundationdb"

[store."postgresql"]
type = "postgresql"
host = "localhost"
port = 5432
database = "stalwart"
user = "postgres"
password = "mysecretpassword"

[store."mysql"]
type = "mysql"
host = "localhost"
port = 3307
database = "stalwart"
user = "root"
password = "password"

"#;

impl TestSMTP {
    pub fn from_core(core: Core) -> Self {
        Self::from_core_and_tempdir(core, Default::default(), None)
    }

    pub fn inner_with_rxs(&self) -> (Arc<Inner>, IpcReceivers) {
        let (ipc, ipc_rxs) = build_ipc(false);

        (
            Inner {
                shared_core: self.server.core.as_ref().clone().into_shared(),
                data: Default::default(),
                ipc,
                cache: Default::default(),
            }
            .into(),
            ipc_rxs,
        )
    }

    fn from_core_and_tempdir(core: Core, data: Data, temp_dir: Option<TempDir>) -> Self {
        let store = core.storage.data.clone();
        let blob_store = core.storage.blob.clone();
        let shared_core = core.into_shared();
        let (ipc, mut ipc_rxs) = build_ipc(false);

        TestSMTP {
            queue_receiver: QueueReceiver {
                store,
                blob_store,
                queue_rx: ipc_rxs.queue_rx.take().unwrap(),
            },
            report_receiver: ReportReceiver {
                report_rx: ipc_rxs.report_rx.take().unwrap(),
            },
            server: Server {
                core: shared_core.load_full(),
                inner: Inner {
                    shared_core,
                    data,
                    ipc,
                    cache: Default::default(),
                }
                .into(),
            },
            temp_dir,
        }
    }

    pub async fn new(name: &str, config: impl AsRef<str>) -> TestSMTP {
        Self::with_database(name, config, "rocksdb").await
    }

    pub async fn with_database(
        name: &str,
        config: impl AsRef<str>,
        store_id: impl AsRef<str>,
    ) -> TestSMTP {
        let temp_dir = TempDir::new(name, true);
        let mut config = Config::new(
            temp_dir
                .update_config(add_test_certs(CONFIG) + config.as_ref())
                .replace("{STORE}", store_id.as_ref()),
        )
        .unwrap();
        config.resolve_all_macros().await;
        let stores = Stores::parse_all(&mut config, false).await;
        let core = Core::parse(&mut config, stores, Default::default()).await;
        let data = Data::parse(&mut config);
        core.storage.data.destroy().await;

        Self::from_core_and_tempdir(core, data, Some(temp_dir))
    }

    pub async fn start(&self, protocols: &[ServerProtocol]) -> watch::Sender<bool> {
        // Spawn listeners
        let mut config = Config::new(CONFIG).unwrap();
        let mut servers = Listeners::parse(&mut config);
        servers.parse_tcp_acceptors(&mut config, self.server.inner.clone());

        // Filter out protocols
        servers
            .servers
            .retain(|server| protocols.contains(&server.protocol));

        // Start servers
        servers.bind_and_drop_priv(&mut config);
        config.assert_no_errors();

        servers
            .spawn(|server, acceptor, shutdown_rx| {
                match &server.protocol {
                    ServerProtocol::Smtp | ServerProtocol::Lmtp => server.spawn(
                        SmtpSessionManager::new(self.server.inner.clone()),
                        self.server.inner.clone(),
                        acceptor,
                        shutdown_rx,
                    ),
                    ServerProtocol::Http => server.spawn(
                        HttpSessionManager::new(self.server.inner.clone()),
                        self.server.inner.clone(),
                        acceptor,
                        shutdown_rx,
                    ),
                    ServerProtocol::Imap | ServerProtocol::Pop3 | ServerProtocol::ManageSieve => {
                        unreachable!()
                    }
                };
            })
            .0
    }

    pub fn new_session(&self) -> Session<DummyIo> {
        Session::test(self.server.clone())
    }

    pub fn build_smtp(&self) -> Server {
        self.server.clone()
    }
}

pub trait DnsCache {
    fn txt_add<'x>(
        &self,
        name: impl IntoFqdn<'x>,
        value: impl Into<Txt>,
        valid_until: std::time::Instant,
    );
    fn ipv4_add<'x>(
        &self,
        name: impl IntoFqdn<'x>,
        value: Vec<Ipv4Addr>,
        valid_until: std::time::Instant,
    );
    fn ipv6_add<'x>(
        &self,
        name: impl IntoFqdn<'x>,
        value: Vec<Ipv6Addr>,
        valid_until: std::time::Instant,
    );
    fn dnsbl_add(&self, name: &str, value: Vec<Ipv4Addr>, valid_until: std::time::Instant);
    fn ptr_add(&self, name: IpAddr, value: Vec<String>, valid_until: std::time::Instant);
    fn mx_add<'x>(&self, name: impl IntoFqdn<'x>, value: Vec<MX>, valid_until: std::time::Instant);
    fn tlsa_add<'x>(
        &self,
        name: impl IntoFqdn<'x>,
        value: Arc<Tlsa>,
        valid_until: std::time::Instant,
    );
}

impl DnsCache for Server {
    fn txt_add<'x>(
        &self,
        name: impl IntoFqdn<'x>,
        value: impl Into<Txt>,
        valid_until: std::time::Instant,
    ) {
        self.inner.cache.dns_txt.insert_with_expiry(
            name.into_fqdn().into_owned(),
            value.into(),
            valid_until,
        );
    }

    fn ipv4_add<'x>(
        &self,
        name: impl IntoFqdn<'x>,
        value: Vec<Ipv4Addr>,
        valid_until: std::time::Instant,
    ) {
        self.inner.cache.dns_ipv4.insert_with_expiry(
            name.into_fqdn().into_owned(),
            Arc::new(value),
            valid_until,
        );
    }

    fn dnsbl_add(&self, name: &str, value: Vec<Ipv4Addr>, valid_until: std::time::Instant) {
        self.inner.cache.dns_rbl.insert_with_expiry(
            name.to_string(),
            Some(Arc::new(IpResolver::new(
                value
                    .iter()
                    .copied()
                    .next()
                    .unwrap_or(Ipv4Addr::BROADCAST)
                    .into(),
            ))),
            valid_until,
        );
    }

    fn ipv6_add<'x>(
        &self,
        name: impl IntoFqdn<'x>,
        value: Vec<Ipv6Addr>,
        valid_until: std::time::Instant,
    ) {
        self.inner.cache.dns_ipv6.insert_with_expiry(
            name.into_fqdn().into_owned(),
            Arc::new(value),
            valid_until,
        );
    }

    fn ptr_add(&self, name: IpAddr, value: Vec<String>, valid_until: std::time::Instant) {
        self.inner
            .cache
            .dns_ptr
            .insert_with_expiry(name, Arc::new(value), valid_until);
    }

    fn mx_add<'x>(&self, name: impl IntoFqdn<'x>, value: Vec<MX>, valid_until: std::time::Instant) {
        self.inner.cache.dns_mx.insert_with_expiry(
            name.into_fqdn().into_owned(),
            Arc::new(value),
            valid_until,
        );
    }

    fn tlsa_add<'x>(
        &self,
        name: impl IntoFqdn<'x>,
        value: Arc<Tlsa>,
        valid_until: std::time::Instant,
    ) {
        self.inner.cache.dns_tlsa.insert_with_expiry(
            name.into_fqdn().into_owned(),
            value,
            valid_until,
        );
    }
}
