#![allow(unused_imports)]
use std::{mem::MaybeUninit, rc::Rc, sync::atomic::AtomicU64};

use anyhow::Ok;
use async_ucx::ucp::*;
use tracing::{debug, info, warn};
use glommio::io::{BufferedFile, DmaBuffer, DmaFile};
use tracing_subscriber::field::debug;

static BUFFER_SIZE: usize = 4 * 1024;
static OFFSET: usize = 8;
static TOTAL_SIZE: usize = BUFFER_SIZE + OFFSET;
static SEND_SIZE: usize = 1 * 1024 * 1024 * 1024;
// static TRAGET_PATH: &str = "test.txt";
// static COMM_COUNT: AtomicU64 = AtomicU64::new(0);

pub async fn run_server() -> anyhow::Result<()>{
  let ctx = Context::new()?;
  let worker = ctx.create_worker()?;


  let polling_worker = worker.clone();
  glommio::spawn_local(async move {
    polling_worker.polling().await;
    warn!("polling abnormally finished");
  }).detach();

  let mut listener = worker.create_listener("0.0.0.0:10000".parse().unwrap())?;
  debug!(addr=listener.socket_addr().unwrap().to_string(), "Listening");

  let mut count = 0;
  loop {
    let conn = listener.next().await;
    conn.remote_addr()?;
    count += 1;
    debug!(count, "Connection accepted");
    let ep = worker.accept(conn).await?;
    debug!("Connection established");
    glommio::spawn_local(async move { 
      if let Err(e) = handler(ep, count).await {
        warn!("handler error: {:?}", e);
      }
    }).detach();
  }
}

async fn handler(ep: Endpoint, _count: u64) -> anyhow::Result<usize> {
  // let file = DmaFile::open(TRAGET_PATH).await
  //   .map_err(|e| anyhow::anyhow!(e.to_string()))?;
  // let file = Rc::new(file);
  // let mut buf = file.alloc_dma_buffer(BUFFER_SIZE);
  // // let mut recv_buf: Vec<MaybeUninit<u8>> = Vec::with_capacity(BUFFER_SIZE);
  // let mut uninit_buf_ref = unsafe {
  //   std::mem::transmute::<&mut [u8], &mut [MaybeUninit<u8>]>(buf.as_mut())
  // };
  // ep.worker().tag_recv(100, &mut uninit_buf_ref).await?;
  let mut buf: Vec<MaybeUninit<u8>> = Vec::with_capacity(TOTAL_SIZE);
  buf.resize(TOTAL_SIZE, MaybeUninit::uninit());
  let mut count = 0;

  debug!("start recv");
  // let recv_size = ep.worker().tag_recv(100, &mut buf).await?;
  // debug!("recv_size: {:?}", recv_size);
  debug!("ep state: {:?}", ep.print_to_stderr());

  let mut now = std::time::Instant::now();
  loop {
    let now2 = std::time::Instant::now();
    let recv_size = ep.worker().tag_recv(100, &mut buf).await?;
    if count == 1 {
      debug!("now2.elapsed(): {:?}", now2.elapsed());
    }
    count += 1;
    if count % (SEND_SIZE / BUFFER_SIZE) == 0 {
      debug!("count: {:?}, time elapsed: {:?}", count, now.elapsed());
      now = std::time::Instant::now();
    }
  }
  info!("Time elapsed: {:?}, count: {:?}", now.elapsed(), count);
  Ok(count)
}

// async fn write_handler(ep: Rc<Endpoint>, file: Rc<DmaFile>) -> anyhow::Result<()> {
//   let mut buf = file.alloc_dma_buffer(BUFFER_SIZE);
//   let mut uninit_buf_ref = unsafe {
//     std::mem::transmute::<&mut [u8], &mut [MaybeUninit<u8>]>(buf.as_mut())
//   };
//   Ok(())
// }