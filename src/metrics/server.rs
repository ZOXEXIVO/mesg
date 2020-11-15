use std::thread;
use std::time::Duration;
use crossbeam_channel::{Sender,unbounded};

pub struct MetricsServer;

impl MetricsServer {
    pub fn start() -> MetricsWriter {
        let (metrics_channel_sender, metrics_receiver) = unbounded();
        let (cancellation_sender, cancellation_receiver) = unbounded();

        let default_sleep_duration = Duration::from_millis(1000);
        
        thread::Builder::new()
            .name("metrics_receiver".into())
            .spawn(move || {
                //info!(thread_logger, "metrics server started");

                loop {
                    let data = metrics_receiver
                        .recv_timeout(Duration::from_secs(1))
                        .unwrap_or(MetricItem {
                            name: "".to_string(),
                            value: -1,
                        });

                    let is_cancelled = cancellation_receiver.try_recv().unwrap_or(false);
                    if is_cancelled {
                        break;
                    }

                    if data.value == -1 {
                        thread::sleep(default_sleep_duration);
                    }

                    //TODO
                }
            })
            .unwrap();

        MetricsWriter::new(
            metrics_channel_sender,
            cancellation_sender,
        )
    }
}

#[derive(Clone)]
pub struct MetricsWriter {
    metrics_channel: Sender<MetricItem>,
    cancellation_channel: Sender<bool>,
}

impl MetricsWriter {
    pub fn new(
        metrics_channel: Sender<MetricItem>,
        cancellation_channel: Sender<bool>,
    ) -> MetricsWriter {
        MetricsWriter {
            metrics_channel,
            cancellation_channel,
        }
    }

    pub fn inc_push_operation(&self) {
        self.write("push", 1)
    }

    pub fn inc_pull_operation(&self) {
        self.write("pull", 1)
    }

    pub fn inc_commit_operation(&self) {
        self.write("commit", 1)
    }
    
    fn write(&self, name: &str, value: i32) {
        let metric = MetricItem {  name: String::from(name), value };

        match self.metrics_channel.send(metric) {
            Ok(()) => {}
            Err(e) => {
                //error!(self.logger, "error while send metric {}", e);
            }
        }
    }
}

#[derive(Clone)]
pub struct MetricItem {
    name: String,
    value: i32,
}
