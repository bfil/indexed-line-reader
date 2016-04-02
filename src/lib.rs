//! # Indexed Line Reader
//! `IndexedLineReader` implements the `Seek` trait to allow seeking to specific lines
//!
//! ## Index Granularity
//! `IndexedLineReader` just stores a byte count every `n` lines to allow fast seeking by line.
//! The granularity can be configured by passing the valut to the `IndexedLineReader` constructor.
//!
//! There is a tradeoff to make between memory occupied by the index and the seek speed,
//! in general lower granularity means more line indexed and higher memory consumption, but fast seek time,
//! while a higher granularity slows down the seek time but less indexes are kept in memory.
//!
//! As an example, if a file has 100,000,000 lines, usually a granularity of 100,000 gives a good tradeoff
//! between performance and memory consumption.
//!
//! ## Example
//!
//! ```no_run
//! extern crate indexed_line_reader;
//! # fn main() {
//!
//! use indexed_line_reader::*;
//! use std::fs::*;
//! use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
//!
//! /* Creates an IndexedLineReader for a file with index granularity of 100 */
//! let file_reader = OpenOptions::new().read(true).open("file.txt").expect("Unable to open file reader");
//! let mut indexed_line_reader = &mut IndexedLineReader::new(BufReader::new(file_reader), 100);
//!
//! /* Seeks to line 100 from the start of the file */
//! indexed_line_reader.seek(SeekFrom::Start(100));
//!
//! /* Seeks forward by 50 lines from the current position on the file */
//! indexed_line_reader.seek(SeekFrom::Current(50));
//!
//! /* Seeks backward by 50 lines from the current position on the file */
//! indexed_line_reader.seek(SeekFrom::Current(-50));
//!
//! /* Seeks to the 100th line from the end on the file */
//! indexed_line_reader.seek(SeekFrom::End(100));
//!
//! # }
//! ```

use std::collections::BTreeMap;
use std::io::{BufRead, Error, Read, Seek, SeekFrom};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinesIndex {
    index: BTreeMap<u64, u64>,
    granularity: u64,
    line_count: u64,
    byte_count: u64
}

impl LinesIndex {
    pub fn new(granularity: u64) -> LinesIndex {
        LinesIndex {
            index: BTreeMap::new(),
            granularity: granularity,
            line_count: 0,
            byte_count: 0
        }
    }

    pub fn insert(&mut self, pos: u64, byte_count: u64) -> Option<u64> {
        self.index.insert(pos, byte_count)
    }

    pub fn clear(&mut self) {
        self.index.clear();
    }

    pub fn line_count(&self) -> u64 {
        self.line_count
    }

    pub fn byte_count(&self) -> u64 {
        self.byte_count
    }

    pub fn byte_count_at_pos(&self, pos: &u64) -> Option<u64> {
        self.index.get(pos).map(|&x| x)
    }

    pub fn last_indexed_pos(&self) -> Option<u64> {
        self.index.keys().map(|&x| x).max()
    }

    pub fn compute<T: BufRead + Seek>(&mut self, mut reader: &mut T) -> Result<u64, Error> {
        let initial_pos = self.last_indexed_pos().unwrap_or(0);
        let mut line_count = initial_pos;
        let mut byte_count = self.byte_count_at_pos(&line_count).unwrap_or(0);
        try!(reader.seek(SeekFrom::Start(byte_count)));
        if byte_count > 0 {
            reader.lines().next();
        }
        for (pos, line) in reader.lines().enumerate() {
            match line {
                Ok(line) => {
                    byte_count += line.as_bytes().len() as u64 + 1;
                    if (pos as u64 + 1) % self.granularity == 0 {
                        self.insert(initial_pos + pos as u64 + 1, byte_count);
                    }
                    line_count += 1;
                },
                Err(err) => return Err(err)
            }
        }
        self.line_count = line_count;
        self.byte_count = byte_count;
        Ok(line_count)
    }

    pub fn get_ref(&self) -> &BTreeMap<u64, u64> {
        &self.index
    }

    pub fn get_mut(&mut self) -> &mut BTreeMap<u64, u64> {
        &mut self.index
    }

    pub fn into_inner(self) -> BTreeMap<u64, u64> {
        self.index
    }
}

#[derive(Debug)]
pub struct IndexedLineReader<T> {
    index: LinesIndex,
    pos: u64,
    line_count: u64,
    reader: T
}

impl<T: BufRead + Seek> IndexedLineReader<T> {
    pub fn new(reader: T, index_granularity: u64) -> IndexedLineReader<T> {
        IndexedLineReader {
            index: LinesIndex::new(index_granularity),
            pos: 0,
            line_count: 0,
            reader: reader
        }
    }

    pub fn get_index(&self) -> &LinesIndex {
        &self.index
    }

    pub fn restore_index(&mut self, index: LinesIndex) {
        self.index = index;
    }

    pub fn compute_index(&mut self) -> Result<u64, Error> {
        self.index.compute(&mut self.reader).and_then(|line_count| {
            self.line_count = line_count;
            Ok(line_count)
        })
    }

    pub fn clear_index(&mut self) {
        self.index.clear()
    }

    pub fn get_current_pos(&self) -> u64 {
        self.pos
    }

    pub fn byte_count(&mut self) -> Result<u64, Error> {
        self.reader.seek(SeekFrom::End(0))
    }

    fn seek_to_index(&mut self, indexed_pos: u64) -> Result<u64, Error> {
        self.pos = indexed_pos;
        let byte_count = self.index.byte_count_at_pos(&indexed_pos).unwrap_or(0);
        self.reader.seek(SeekFrom::Start(byte_count))
    }

    fn seek_to_closest_index(&mut self, pos: SeekFrom) -> Result<u64, Error> {
        match pos {
            SeekFrom::Start(pos) => {
                let extra_lines = pos % self.index.granularity;
                let closest_index = pos - extra_lines;
                self.seek_to_index(closest_index)
            },
            SeekFrom::Current(pos) => {
                let extra_lines = pos as u64 % self.index.granularity;
                let extra_lines_from_current_pos = self.pos % self.index.granularity;
                let previous_closest_index = self.pos - extra_lines_from_current_pos;
                let closest_index = previous_closest_index + pos as u64 - extra_lines;
                self.seek_to_index(closest_index)
            },
            SeekFrom::End(pos) => {
                let pos = self.line_count - pos.abs() as u64;
                self.seek_to_closest_index(SeekFrom::Start(pos))
            }
        }
    }

    fn seek_forward(&mut self, lines: u64) -> Result<u64, Error> {
        let mut lines_left = lines;
        let mut extra_byte_count: u64 = 0;
        for line in (&mut self.reader).lines() {
            match line {
                Ok(line) => {
                    lines_left -= 1;
                    self.pos += 1;
                    extra_byte_count += line.as_bytes().len() as u64 + 1;
                    if lines_left == 0 { break }
                },
                Err(err) => return Err(err)
            }
        }
        Ok(extra_byte_count)
    }
}

impl<T: Read> Read for IndexedLineReader<T> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        self.reader.read(buf)
    }
}

impl<T: BufRead> BufRead for IndexedLineReader<T> {
    fn fill_buf(&mut self) -> Result<&[u8], Error> {
        self.reader.fill_buf()
    }
    fn consume(&mut self, amt: usize) {
        self.reader.consume(amt)
    }
}

impl<T: BufRead + Seek> Seek for IndexedLineReader<T> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, Error> {
        self.compute_index().and_then(|_| {
            match pos {
                SeekFrom::Start(pos) => {
                    let extra_lines = pos as u64 % self.index.granularity;
                    self.seek_to_closest_index(SeekFrom::Start(pos)).and_then(|new_pos| {
                        if extra_lines > 0 {
                            self.seek(SeekFrom::Current(extra_lines as i64))
                        } else {
                            Ok(new_pos)
                        }
                    })
                },
                SeekFrom::Current(pos) => {
                    if pos >= 0 {
                        let extra_lines = pos as u64 % self.index.granularity;
                        let extra_lines_from_current_pos = self.pos % self.index.granularity;
                        self.seek_to_closest_index(SeekFrom::Current(pos)).and_then(|new_pos| {
                            if extra_lines + extra_lines_from_current_pos > 0 {
                                self.seek_forward(extra_lines + extra_lines_from_current_pos)
                            } else {
                                Ok(new_pos)
                            }
                        })
                    } else {
                        let pos = self.pos - pos.abs() as u64;
                        self.seek(SeekFrom::Start(pos))
                    }
                },
                SeekFrom::End(pos) => {
                    let pos = self.line_count - pos.abs() as u64;
                    self.seek(SeekFrom::Start(pos))
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::*;
    use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};

    fn seek_and_assert_line_number(mut reader: &mut IndexedLineReader<BufReader<File>>,
                                    seek_from: SeekFrom, expected_line_number: u64) {
        reader.seek(seek_from).expect(&format!("Unable to seek from {:?}", seek_from));
        let line = (&mut reader).lines().next().unwrap().unwrap();
        let line_number: u64 = line.parse().expect("Unable to deserialize line number");
        assert_eq!(line_number, expected_line_number);
    }

    #[test]
    fn test_seek() {
        let log_name = "indexed-line-reader.log";
        let mut file_writer = OpenOptions::new().create(true).write(true).append(true).open(log_name).expect("Unable to open file writer");

        for i in 0..10000 {
            assert!(write!(file_writer, "{}\n", i).is_ok());
        }

        let file_reader = OpenOptions::new().read(true).open(log_name).expect("Unable to open file reader");
        let mut line_reader = &mut IndexedLineReader::new(BufReader::new(file_reader), 100);

        line_reader.compute_index().expect("Unable to compute index");

        seek_and_assert_line_number(line_reader, SeekFrom::Start(1234), 1234);
        seek_and_assert_line_number(line_reader, SeekFrom::Start(2468), 2468);
        seek_and_assert_line_number(line_reader, SeekFrom::Current(1000), 3468);
        seek_and_assert_line_number(line_reader, SeekFrom::Current(1032), 4500);
        seek_and_assert_line_number(line_reader, SeekFrom::Current(-450), 4050);
        seek_and_assert_line_number(line_reader, SeekFrom::End(1234), 8766);
        seek_and_assert_line_number(line_reader, SeekFrom::End(-1234), 8766);

        remove_file(log_name).expect("Unable to delete log");
    }
}
