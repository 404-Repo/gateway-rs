use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayInfo {
    pub node_id: u64,
    pub domain: String,
    pub ip: String,
    pub name: String,
    pub http_port: u16,
    pub available_tasks: usize,
    pub last_task_acquisition: u64,
    pub last_update: u64,
}

#[derive(Debug, Serialize)]
pub struct GatewayInfoRef<'a> {
    pub node_id: u64,
    pub domain: &'a str,
    pub ip: &'a str,
    pub name: &'a str,
    pub http_port: u16,
    pub available_tasks: usize,
    pub last_task_acquisition: u64,
    pub last_update: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayInfoExt {
    pub node_id: u64,
    pub domain: String,
    pub ip: String,
    pub name: String,
    pub http_port: u16,
    pub available_tasks: usize,
    pub cluster_name: String,
    pub last_task_acquisition: u64,
    pub last_update: u64,
}

#[derive(Debug, Serialize)]
pub struct GatewayInfoExtRef<'a> {
    pub node_id: u64,
    pub domain: &'a str,
    pub ip: &'a str,
    pub name: &'a str,
    pub http_port: u16,
    pub available_tasks: usize,
    pub cluster_name: &'a str,
    pub last_task_acquisition: u64,
    pub last_update: u64,
}

impl From<GatewayInfoExt> for GatewayInfo {
    fn from(info: GatewayInfoExt) -> Self {
        GatewayInfo {
            node_id: info.node_id,
            domain: info.domain,
            ip: info.ip,
            name: info.name,
            http_port: info.http_port,
            available_tasks: info.available_tasks,
            last_task_acquisition: info.last_task_acquisition,
            last_update: info.last_update,
        }
    }
}
