use std::{
    collections::BTreeMap,
    fs::File,
    io::{ErrorKind, Read},
};

use ahash::RandomState;
use bstr::ByteSlice;
use hashbrown::HashMap;

const READ_BUF_SIZE: usize = 128 * 1024; // 128 KiB
const VALUE_SEPARATOR: u8 = b';';
const CHANNEL_CAPACITY: usize = 1_000;

#[derive(Debug, Clone, Copy)]
struct Stats {
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
}

impl Stats {
    fn new(first_value: f64) -> Stats {
        Stats {
            count: 1,
            sum: first_value,
            min: first_value,
            max: first_value,
        }
    }

    fn update(&mut self, next_value: f64) {
        self.count += 1;
        self.sum += next_value;
        self.min = self.min.min(next_value);
        self.max = self.max.max(next_value);
    }
}

fn main() {
    let (sender, receiver) = crossbeam_channel::bounded::<Box<[u8]>>(CHANNEL_CAPACITY);

    let n_threads = std::thread::available_parallelism().unwrap().into();
    let mut thread_handles = Vec::with_capacity(n_threads);
    for _ in 0..n_threads {
        let receiver = receiver.clone();
        let handle = std::thread::spawn(move || {
            let mut map = HashMap::<Box<[u8]>, Stats, RandomState>::default();

            for buf in receiver {
                for raw_line in buf.lines_with_terminator() {
                    let line = trim_new_line(raw_line);
                    let (city, temp) =
                        split_once_byte(line, VALUE_SEPARATOR).expect("Separator not found");

                    let temp = fast_float::parse::<f64, _>(temp).unwrap();
                    map.entry_ref(city)
                        .and_modify(|stats| stats.update(temp))
                        .or_insert_with(|| Stats::new(temp));
                }
            }
            map
        });
        thread_handles.push(handle);
    }
    // Drop superfluous receiver
    drop(receiver);

    let input_filename = std::env::args().nth(1).expect("No input filename");
    let mut input_file = File::open(input_filename).unwrap();

    let mut buf = vec![0; READ_BUF_SIZE];
    let mut bytes_not_processed = 0;
    loop {
        let bytes_read = match input_file.read(&mut buf[bytes_not_processed..]) {
            Ok(n) => n,
            Err(err) => {
                if err.kind() == ErrorKind::Interrupted {
                    continue; // Retry
                } else {
                    panic!("I/O error: {err:?}");
                }
            }
        };
        if bytes_read == 0 {
            break; // EOF
        }

        let valid_buf = &mut buf[..(bytes_read + bytes_not_processed)];
        let last_new_line_idx = match find_last_new_line_pos(valid_buf) {
            Some(pos) => pos,
            None => {
                bytes_not_processed += bytes_read;
                assert!(bytes_not_processed <= buf.len());
                if bytes_not_processed == buf.len() {
                    panic!("Found no new line in the whole read buffer");
                }
                continue; // Read again, maybe next read contains a new line
            }
        };
        let buf_boxed = Box::<[u8]>::from(&valid_buf[..(last_new_line_idx + 1)]);
        sender.send(buf_boxed).unwrap();

        valid_buf.copy_within((last_new_line_idx + 1).., 0);
        bytes_not_processed = valid_buf.len() - last_new_line_idx - 1;
    }

    // Handle the case when the file doesn't end with '\n'
    if bytes_not_processed != 0 {
        // Send the last batch
        let buf_boxed = Box::<[u8]>::from(&buf[..bytes_not_processed]);
        sender.send(buf_boxed).unwrap();
        bytes_not_processed = 0;
    }

    drop(sender);
    let mut ordered_map = BTreeMap::new();
    for (idx, handle) in thread_handles.into_iter().enumerate() {
        let map = handle.join().unwrap();
        if idx == 0 {
            ordered_map.extend(map);
        } else {
            for (city, stats) in map.into_iter() {
                ordered_map
                    .entry(city)
                    .and_modify(|s| {
                        s.count += stats.count;
                        s.sum += stats.sum;
                        s.min = s.min.min(stats.min);
                        s.max = s.max.max(stats.max);
                    })
                    .or_insert(stats);
            }
        }
    }
    print!("{{");
    for (idx, (city, stats)) in ordered_map.iter().enumerate() {
        if idx > 0 {
            print!(", ");
        }
        let city = city.as_bstr();
        let avg = stats.sum / stats.count as f64;
        let (min, max) = (stats.min, stats.max);
        print!("{city}={min:.1}/{avg:.1}/{max:.1}");
    }
    println!("}}");
    dbg!(ordered_map.len());
}

fn find_last_new_line_pos(bytes: &[u8]) -> Option<usize> {
    // In this case (position is not far enough),
    // naive version is faster than bstr (memchr)
    bytes.iter().rposition(|&b| b == b'\n')
}

fn split_once_byte(haystack: &[u8], needle: u8) -> Option<(&[u8], &[u8])> {
    let Some(pos) = haystack.iter().position(|&b| b == needle) else {
        return None;
    };
    // // Using memchr / bstr's find_byte is a bit slower
    // let Some(pos) = haystack.find_byte(needle) else {
    //     return None;
    // };
    Some((&haystack[..pos], &haystack[pos + 1..]))
}

fn trim_new_line(s: &[u8]) -> &[u8] {
    let mut trimmed = s;
    if trimmed.last_byte() == Some(b'\n') {
        trimmed = &trimmed[..trimmed.len() - 1];
        if trimmed.last_byte() == Some(b'\r') {
            trimmed = &trimmed[..trimmed.len() - 1];
        }
    }
    trimmed
}
