use std::io::{Read, Seek, SeekFrom};

use prost::Message;

use crate::proto::{CompressionKind, Footer, Metadata, PostScript, StripeFooter};

const DEFAULT_FOOTER_SIZE: u64 = 16 * 1024;

// see (unstable) Seek::stream_len
fn stream_len(seek: &mut impl Seek) -> std::result::Result<u64, std::io::Error> {
    let old_pos = seek.seek(SeekFrom::Current(0))?;
    let len = seek.seek(SeekFrom::End(0))?;

    // Avoid seeking a third time when we were already at the end of the
    // stream. The branch is usually way cheaper than a seek operation.
    if old_pos != len {
        seek.seek(SeekFrom::Start(old_pos))?;
    }

    Ok(len)
}

pub fn read_metadata<R>(reader: &mut R) -> Result<(PostScript, Footer, Metadata), std::io::Error>
where
    R: Read + Seek,
{
    let file_len = stream_len(reader)?;

    // initial read of the footer
    let footer_len = if file_len < DEFAULT_FOOTER_SIZE {
        file_len
    } else {
        DEFAULT_FOOTER_SIZE
    };

    reader.seek(SeekFrom::End(-(footer_len as i64)))?;
    let mut tail_bytes = vec![0; footer_len as usize];
    reader.read_exact(&mut tail_bytes)?;

    // The final byte of the file contains the serialized length of the Postscript,
    // which must be less than 256 bytes.
    let postscript_len = tail_bytes[tail_bytes.len() - 1] as usize;
    tail_bytes.truncate(tail_bytes.len() - 1);

    // next is the postscript
    let postscript = PostScript::decode(&tail_bytes[tail_bytes.len() - postscript_len..])?;
    tail_bytes.truncate(tail_bytes.len() - postscript_len);

    println!("{postscript:?}");

    // next is the footer
    let footer_length = postscript.footer_length.unwrap() as usize; // todo: throw error

    let footer = &tail_bytes[tail_bytes.len() - footer_length..];
    let footer = deserialize_footer(footer, postscript.compression())?;
    tail_bytes.truncate(tail_bytes.len() - footer_length);

    // finally the metadata
    let metadata_length = postscript.metadata_length.unwrap() as usize; // todo: throw error
    let metadata = &tail_bytes[tail_bytes.len() - metadata_length..];
    let metadata = deserialize_footer_metadata(metadata)?;

    Ok((postscript, footer, metadata))
}

fn decode_header(bytes: &[u8]) -> (bool, usize) {
    // 5 uncompressed = [0x0b, 0x00, 0x00] = [0b1011, 0, 0]
    // let bytes = &[0b1011, 0, 0, 0];
    // 100_000 compressed = [0x40, 0x0d, 0x03] = [0b01000000, 0b00001101, 0b00000011]
    //let bytes = &[0b01000000, 0b00001101, 0b00000011, 0];
    let mut a: [u8; 3] = (&bytes[..3]).try_into().unwrap();
    let mut a = [0, a[0], a[1], a[2]];
    println!(
        "{:#010b} {:#010b} {:#010b} {:#010b}",
        a[0], a[1], a[2], a[3]
    );
    let length = u32::from_le_bytes(a);
    println!("length: {length}");
    let is_original = a[0] & 1 == 1;
    let length = (length >> 1) as usize;
    println!("{:#032b}", length);
    println!("{:#032b}", 1u32);
    dbg!(is_original);
    dbg!(length);
    (is_original, length)
}

macro_rules! deserialize {
    ($bytes:expr, $compression:expr, $op:ident) => {{
        let bytes = $bytes;
        let compression = $compression;
        let (is_original, _length) = decode_header(bytes);
        let bytes = &bytes[3..];

        println!("is_original: {is_original:?}");
        println!("_length: {_length:?}");

        if is_original {
            Ok($op(bytes)?)
        } else {
            match compression {
                CompressionKind::None => Ok($op(bytes)?),
                _ => {
                    let mut gz = flate2::read::DeflateDecoder::new(bytes);
                    let mut bytes = Vec::<u8>::new();
                    gz.read_to_end(&mut bytes).unwrap();
                    Ok($op(&bytes)?)
                }
            }
        }
    }};
}

fn deserialize_footer(
    footer: &[u8],
    compression: CompressionKind,
) -> Result<Footer, std::io::Error> {
    let f = Footer::decode;
    deserialize!(footer, compression, f)
}

fn deserialize_footer_metadata(bytes: &[u8]) -> Result<Metadata, std::io::Error> {
    let f = Metadata::decode;
    deserialize!(bytes, CompressionKind::None, f)
}

pub fn deserialize_stripe_footer(bytes: &[u8]) -> Result<StripeFooter, std::io::Error> {
    let f = StripeFooter::decode;
    deserialize!(bytes, CompressionKind::None, f)
}