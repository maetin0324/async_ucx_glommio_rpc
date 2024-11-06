use std::vec;

use anyhow::Ok;
use async_ucx::ucp::*;
use tracing::debug;
static BUFFER_SIZE: usize = 1024 * 1024;
static OFFSET: usize = 8;
static TOTAL_SIZE: usize = BUFFER_SIZE + OFFSET;
static SEND_SIZE: usize =  128 * 1024 * 1024 * 1024;

pub async fn run_client(addr: &str) -> anyhow::Result<()> {
  let ctx = Context::new()?;
  let worker = ctx.create_worker()?;
  let polling_worker = worker.clone();

  glommio::spawn_local(async move {
    polling_worker.polling().await;
  }).detach();

  let ep = worker.connect_socket(addr.parse().unwrap()).await?;
  let mut buf: Vec<u8> = vec![0; TOTAL_SIZE];
  let mut count: usize = 0;

  debug!("start sending data");
  loop {
    let offset = count * BUFFER_SIZE;
    buf[0..OFFSET].copy_from_slice(&offset.to_le_bytes());
    ep.tag_send(100, &buf).await?;
    count += 1;
    if count % (SEND_SIZE / BUFFER_SIZE) == 0 {
      debug!("send finished, count: {}", count);
      count = 0;
    }
  }
  // ep.tag_send(100, &[0]).await?;
  // debug!("send finished, count: {}", count);

  // Ok(())
}
