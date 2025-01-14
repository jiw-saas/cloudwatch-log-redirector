use aws_config::meta::region::RegionProviderChain;
use aws_config::BehaviorVersion;
use aws_sdk_cloudwatchlogs as cloudwatchlogs;
use aws_sdk_cloudwatchlogs::types::InputLogEvent;
use clap::Parser;
use cloudwatchlogs::{Client, Error};
use futures::stream::StreamExt;
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use signal_hook::consts::signal::{SIGABRT, SIGHUP, SIGINT, SIGQUIT, SIGTERM, SIGTRAP};
use signal_hook_tokio::Signals;
use std::env::current_dir;
use std::io::Read;
use std::process::exit;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::BufReader;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;
use tokio::time::{self, Duration};
struct LogBuffer {
    buffer: Vec<(InputLogEvent, u64)>,
    counter: u64,
}

impl LogBuffer {
    // Create a new LogBuffer instance
    fn new() -> Self {
        Self {
            buffer: Vec::new(),
            counter: 0,
        }
    }

    // Add a log event to the buffer
    fn add_event(&mut self, event: InputLogEvent) {
        self.buffer.push((event, self.counter));
        self.counter += 1;
    }

    /// A function that sorts log events by timestamp and returns a subset whose total size is < 1MB.
    /// The returned subset is removed from the original vector.
    fn get_subset_to_publish(&mut self) -> Vec<InputLogEvent> {
        // Sort the log events by timestamp in ascending order
        self.buffer
            .sort_by_key(|event| (event.0.timestamp(), event.1));

        // Define the maximum size in bytes (1MB = 1 * 1024 * 1024 bytes)
        const MAX_SIZE: usize = 1024 * 1024;

        let mut subset = Vec::new();
        let mut total_size = 0;

        // Use the `retain` method to remove events that are part of the subset from the original vector
        self.buffer.retain(|event| {
            let message_size = event.0.message().len();

            // If adding this event would exceed the size limit, return true (keep the event)
            if total_size + message_size >= MAX_SIZE || subset.len() >= 1000 {
                return true;
            }

            // Otherwise, add the event to the subset and accumulate the size
            total_size += message_size;
            subset.push(event.clone());

            // Return false to remove this event from the original vector
            false
        });

        subset.iter().map(|(event, _)| event.clone()).collect()
    }

    // Flush the buffered log events
}

#[derive(Parser)]
#[command(name = "cloudwatch_output_redirector")]
#[command(version = "v0.1.10-jiw")]
#[command(author = "Rusty Conover <rusty@conover.me>")]
#[command(about = "Redirects stdout and stderr to CloudWatch Logs")]
struct CommandLineArgs {
    /// The name of the CloudWatch Logs log group
    log_group_name: String,

    /// The name of the CloudWatch Logs log stream
    log_stream_name: String,

    #[arg(
        short,
        long,
        default_value_t = true,
        help = "Tag the stream names with [STDOUT] and [STDERR]"
    )]
    tag_stream_names: bool,

    #[arg(
        short,
        long,
        default_value_t = false,
        help = "Add messages to the log stream with information about the execution"
    )]
    info: bool,

    #[arg(
        name = "tee",
        long,
        default_value_t = false,
        help = "Tee the output rather than just sending it to CloudWatch Logs"
    )]
    tee: bool,

    /// The command to execute
    command: String,

    /// Arguments for the command
    args: Vec<String>,
}

fn current_time_in_millis() -> i64 {
    // Get the current time since the UNIX_EPOCH
    let now = SystemTime::now();

    // Calculate the duration since UNIX_EPOCH
    now.duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as i64
}

#[derive(Copy, Clone)]
enum Tee {
    DISABLED,
    STDOUT,
    STDERR,
}

struct LogBufferGroup {
    buffer: Arc<Mutex<LogBuffer>>,
    prefix: String,
    active: bool,
    tee: Tee,
}
impl Clone for LogBufferGroup {
    fn clone(&self) -> LogBufferGroup {
        return LogBufferGroup {
            buffer: Arc::clone(&self.buffer),
            prefix: self.prefix.clone(),
            active: self.active,
            tee: self.tee,
        };
    }
}
impl LogBufferGroup {
    async fn add(&self, line: String) {
        if !self.active {
            return;
        }
        let msg = format!("{}{}", self.prefix, line);
        match self.tee {
            Tee::STDOUT => {
                println!("{}", msg);
            }
            Tee::STDERR => {
                eprintln!("{}", msg);
            }
            Tee::DISABLED => {}
        };
        let log_event = InputLogEvent::builder()
            .message(msg)
            .timestamp(current_time_in_millis())
            .build()
            .unwrap();
        let mut buffer = self.buffer.lock().await;
        buffer.add_event(log_event);
    }
    async fn add_stream<T: AsyncRead + Unpin>(&self, reader: BufReader<T>) {
        let mut lines = reader.lines();
        while let Some(line) = lines.next_line().await.unwrap() {
            self.add(line).await
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let args = CommandLineArgs::parse();

    // Create a CloudWatch Logs client
    let region_provider = RegionProviderChain::default_provider().or_else("us-east-1"); // Adjust your region
    let config = Arc::new(
        aws_config::defaults(BehaviorVersion::v2024_03_28())
            .region(region_provider)
            .load()
            .await,
    );

    // Create log group and stream
    {
        let client = Client::new(&Arc::clone(&config));

        if let Err(e) = create_log_group(&client, &args.log_group_name).await {
            if e.to_string().contains("ResourceAlreadyExistsException") {
                tracing::info!("Log group already exists");
            } else {
                tracing::error!("Error creating log group: {}", e);
            }
        }
        if let Err(e) =
            create_log_stream(&client, &args.log_group_name, &args.log_stream_name).await
        {
            if e.to_string().contains("ResourceAlreadyExistsException") {
                tracing::info!("Log stream already exists");
            } else {
                tracing::error!("Error creating log group: {}", e);
            }
        }
    }

    let exit_indicator = Arc::new(Mutex::new(false));
    let exit_flag_handle = Arc::clone(&exit_indicator);
    let log_buffer = Arc::new(Mutex::new(LogBuffer::new()));
    let flush_buffer = Arc::clone(&log_buffer);
    let flush_handle = tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_millis(250));
        let client = Client::new(&config);
        let mut skip_sleep = false;
        loop {
            if !skip_sleep {
                interval.tick().await; // Wait for the next tick
            }

            let messages = {
                // Acquire the lock here but release it quickly
                let mut buffer = flush_buffer.lock().await;
                buffer.get_subset_to_publish()
            };

            if messages.is_empty() {
                tracing::trace!("Nothing to flush");
                let exit = exit_flag_handle.lock().await;
                if *exit {
                    tracing::info!("Exiting flush loop due to flag being set");
                    break;
                }
                skip_sleep = false;
                continue; // Nothing to flush
            }

            if messages.len() > 100 {
                skip_sleep = true;
            }

            tracing::info!("Flushing {} log events", messages.len());
            let result = client
                .put_log_events()
                .log_group_name(&args.log_group_name)
                .log_stream_name(&args.log_stream_name)
                .set_log_events(Some(messages.clone()))
                .send()
                .await;
            match result {
                Ok(put_result) => {
                    match put_result.rejected_log_events_info {
                        Some(rejected) => {
                            tracing::warn!("Some log events were rejected: {:?}", rejected)
                        }
                        None => tracing::info!("Flushed events"),
                    };
                }
                Err(e) => {
                    tracing::error!("Failed to flush log events: {}", e);
                }
            }
        }
    });
    let info_buffer = LogBufferGroup {
        buffer: Arc::clone(&log_buffer),
        prefix: "[INFO] ".to_string(),
        active: args.info,
        tee: match args.tee {
            true => Tee::STDERR,
            false => Tee::DISABLED,
        },
    };
    let status = match start(log_buffer, info_buffer.clone()).await {
        Ok(status) => status,
        Err(e) => {
            info_buffer
                .add(format!("[ERROR] Process exited unexpectedly: {}", e))
                .await;
            -1
        }
    };

    // Set the exit indicator to true
    tracing::info!("Setting exit indicator so final messages are flushed.");
    {
        let mut exit = exit_indicator.lock().await;
        *exit = true;
    }

    // Flush any pending events.
    let flush_result = flush_handle.await;
    if let Err(e) = flush_result {
        tracing::error!("Failed to flush log events: {}", e);
    }

    tracing::info!("Finished");
    exit(status);
}

async fn start(
    log_buffer: Arc<Mutex<LogBuffer>>,
    info_buffer: LogBufferGroup,
) -> Result<i32, std::io::Error> {
    let args = CommandLineArgs::parse();
    info_buffer
        .add(format!(
            "Starting (pwd={:?}): {}{}",
            current_dir().unwrap(),
            args.command,
            args.args.join(" ")
        ))
        .await;
    let mut child = match tokio::process::Command::new(&args.command)
        .args(&args.args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return Err(e),
    };

    let child_pid = child.id();
    info_buffer
        .add(match child_pid {
            Some(pid) => format!("Successfully started. pid={}", pid),
            None => "[WARNING] Successfully started, but cant identify pid!".to_string(),
        })
        .await;
    let signal_buffer = info_buffer.clone();
    let mut signals = Signals::new(&[SIGHUP, SIGINT, SIGQUIT, SIGTERM, SIGTRAP, SIGABRT])?;
    let signals_handle = signals.handle();
    let signal_task = tokio::spawn(async move {
        while let Some(signal) = signals.next().await {
            match child_pid {
                Some(pid) => {
                    signal_buffer
                        .add(format!(
                            "Parent received signal {}, forwarding to child (pid={}).",
                            signal, pid
                        ))
                        .await;

                    match Signal::try_from(signal) {
                        Ok(signal) => {
                            // Forward SIGINT to the child process
                            let _ = kill(Pid::from_raw(pid as i32), signal);
                        }
                        Err(_) => {
                            signal_buffer
                                .add(format!(
                                    "[WARNING] Can not convert signal={} to killsignal",
                                    signal
                                ))
                                .await
                        }
                    }
                }
                None => {
                    signal_buffer
                        .add(format!(
                            "[WARNING] Parent received signal {} but can't forward without pid.",
                            signal
                        ))
                        .await;
                }
            }
        }
    });

    // Stream stdout
    let stdout_log = LogBufferGroup {
        active: true,
        buffer: Arc::clone(&log_buffer),
        prefix: match args.tag_stream_names {
            true => "[STDOUT] ".to_string(),
            false => "".to_string(),
        },
        tee: match args.tee {
            true => Tee::STDOUT,
            false => Tee::DISABLED,
        },
    };
    let stdout_reader = child.stdout.take().map(|stream| BufReader::new(stream));
    let stdout_info = info_buffer.clone();
    let stdout_task = tokio::spawn(async move {
        match stdout_reader {
            Some(reader) => stdout_log.add_stream(reader).await,
            None => {
                stdout_info
                    .add("[WARNING] Can not process stderr.".to_string())
                    .await
            }
        }
    });
    let stderr_log = LogBufferGroup {
        active: true,
        buffer: Arc::clone(&log_buffer),
        prefix: match args.tag_stream_names {
            true => "[STDERR] ".to_string(),
            false => "".to_string(),
        },
        tee: match args.tee {
            true => Tee::STDERR,
            false => Tee::DISABLED,
        },
    };
    let stderr_reader = child.stderr.take().map(|stream| BufReader::new(stream));
    let stderr_info = info_buffer.clone();
    let stderr_task = tokio::spawn(async move {
        match stderr_reader {
            Some(reader) => stderr_log.add_stream(reader).await,
            None => {
                stderr_info
                    .add("[WARNING] Can not process stderr.".to_string())
                    .await
            }
        }
    });
    let exit_indicator = Arc::new(Mutex::new(false));
    let exit_flag_handle = Arc::clone(&exit_indicator);
    let stdin_writer = child.stdin.take().map(|stream| BufWriter::new(stream));
    let stdin_info = info_buffer.clone();
    let mut stdin = std::io::stdin();
    let stdin_task = tokio::spawn(async move {
        match stdin_writer {
            Some(mut writer) => {
                let mut buf = [0; 1024];
                loop {
                    {
                        let exit = exit_flag_handle.lock().await;
                        if *exit {
                            break;
                        }
                    }
                    match stdin.read(&mut buf) {
                        Ok(size) => {
                            if size == 0 {
                                match writer.flush().await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        stdin_info
                                            .add(format!(
                                                "[WARNING] Error while flushing writer: {:?}",
                                                e
                                            ))
                                            .await
                                    }
                                };
                            } else {
                                let last = buf[size - 1]; // Without reading the last buffer item, it will not flush correctly?!
                                let slice = &mut buf[..size];
                                match writer.write(slice).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        stdin_info
                                            .add(format!(
                                                "[WARNING] Error while forwarding stdin: {:?}",
                                                e
                                            ))
                                            .await
                                    }
                                }
                                if last == 10 {
                                    stdin_info.add("Stdin closed.".to_string()).await;
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            stdin_info
                                .add(format!("[WARNING] Erro while reading from stdin: {:?}", e))
                                .await
                        }
                    }
                }
                match writer.flush().await {
                    Ok(_) => {}
                    Err(e) => {
                        stdin_info
                            .add(format!("[WARNING] Error while flushing writer: {:?}", e))
                            .await
                    }
                };
                drop(stdin);
            }
            None => {
                stdin_info
                    .add("[WARNING] Can not process stdin.".to_string())
                    .await
            }
        }
    });

    match stdout_task.await {
        Ok(_) => {}
        Err(e) => {
            info_buffer
                .add(format!("[WARNING] Failed to read stdout: {}", e))
                .await;
        }
    }
    match stderr_task.await {
        Ok(_) => {}
        Err(e) => {
            info_buffer
                .add(format!("[WARNING] Failed to read stderr: {}", e))
                .await;
        }
    }
    let status = match child.wait().await {
        Ok(status) => {
            let code = status.code().map_or(-1, |code| code);
            info_buffer
                .add(format!(
                    "Finished process with (status={:?}, success={})",
                    code,
                    status.success()
                ))
                .await;
            code
        }
        Err(e) => {
            info_buffer
                .add(format!(
                    "[ERROR] Error while waiting for process to finish: {}",
                    e
                ))
                .await;
            -1
        }
    };
    signals_handle.close();
    match signal_task.await {
        Ok(_) => {}
        Err(e) => {
            info_buffer
                .add(format!(
                    "[ERROR] Error while waiting for signals task: {}",
                    e
                ))
                .await;
        }
    };
    tracing::info!("Setting exit indicator so stdin is stopped.");
    {
        let mut exit = exit_indicator.lock().await;
        *exit = true;
    }
    match child.stdin {
        Some(mut stdin) => {
            tracing::info!("Shutting down stdin");
            let _ = stdin.shutdown().await;
        }
        None => {
            tracing::info!("Dont need to shutdown stdin");
        }
    }
    stdin_task.abort();
    match stdin_task.await {
        Ok(_) => {}
        Err(e) => {
            if !e.is_cancelled() {
                info_buffer
                    .add(format!("[WARNING] Fail to process stdin: {:?}", e))
                    .await;
            }
        }
    }
    Ok(status)
}

// Function to create a CloudWatch log group
async fn create_log_group(client: &Client, log_group_name: &str) -> Result<(), Error> {
    client
        .create_log_group()
        .log_group_name(log_group_name)
        .send()
        .await?;
    Ok(())
}

// Function to create a CloudWatch log stream
async fn create_log_stream(
    client: &Client,
    log_group_name: &str,
    log_stream_name: &str,
) -> Result<(), Error> {
    client
        .create_log_stream()
        .log_group_name(log_group_name)
        .log_stream_name(log_stream_name)
        .send()
        .await?;
    Ok(())
}
