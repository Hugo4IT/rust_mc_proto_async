use bytebuffer::ByteBuffer;
use data_buffer::{reader::DataBufferReader, writer::DataBufferWriter};
use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};
use std::{
    error::Error,
    fmt,
    io::{Read, Write},
    net::{TcpStream, ToSocketAddrs},
};
use uuid::Uuid;
use zig_zag::Zigzag;

#[cfg(feature = "tokio")]
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub mod data_buffer;

#[cfg(test)]
mod tests;
pub mod zig_zag;

/// Minecraft protocol error
#[derive(Debug)]
pub enum ProtocolError {
    AddressParseError,
    DataRanOutError,
    StringParseError,
    StreamConnectError,
    VarIntError,
    ReadError,
    WriteError,
    ZlibError,
    UnsignedShortError,
    CloneError,
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "An protocol error occured")
    }
}

impl Error for ProtocolError {}

/// Minecraft packet
#[derive(Debug, Clone)]
pub struct Packet {
    id: u8,
    buffer: ByteBuffer,
}

impl<R: Read> DataBufferReader for R {
    fn read_bytes(&mut self, size: usize) -> Result<Vec<u8>, ProtocolError> {
        let mut buf = vec![0; size];
        match self.read_exact(&mut buf) {
            Ok(_) => Ok(buf),
            Err(_) => Err(ProtocolError::ReadError),
        }
    }
}

impl<W: Write> DataBufferWriter for W {
    fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), ProtocolError> {
        match self.write_all(bytes) {
            Ok(_) => Ok(()),
            Err(_) => Err(ProtocolError::WriteError),
        }
    }
}

impl Packet {
    /// Create new packet from id and buffer
    pub fn new(id: u8, buffer: ByteBuffer) -> Packet {
        Packet { id, buffer }
    }

    /// Create new packet from packet data
    pub fn from_data(data: &[u8]) -> Result<Packet, ProtocolError> {
        let mut buf = ByteBuffer::from_bytes(data);

        let (packet_id, packet_id_size) = buf.read_u8_varint_size()?;
        let packet_data =
            DataBufferReader::read_bytes(&mut buf, data.len() - packet_id_size as usize)?;

        Ok(Packet {
            id: packet_id,
            buffer: ByteBuffer::from_bytes(&packet_data),
        })
    }

    /// Create new packet from id and bytes in buffer
    pub fn from_bytes(id: u8, data: &[u8]) -> Packet {
        Packet {
            id,
            buffer: ByteBuffer::from_bytes(data),
        }
    }

    /// Create new packet with id and empty buffer
    pub fn empty(id: u8) -> Packet {
        Packet {
            id,
            buffer: ByteBuffer::new(),
        }
    }

    /// Build packet with lambda
    pub fn build<F>(id: u8, builder: F) -> Result<Packet, ProtocolError>
    where
        F: FnOnce(&mut Packet) -> Result<(), ProtocolError>,
    {
        let mut packet = Self::empty(id);
        builder(&mut packet)?;
        Ok(packet)
    }

    /// Get packet id
    pub fn id(&self) -> u8 {
        self.id
    }

    /// Set packet id
    pub fn set_id(&mut self, id: u8) {
        self.id = id;
    }

    /// Get mutable reference of buffer
    pub fn buffer(&mut self) -> &mut ByteBuffer {
        &mut self.buffer
    }

    /// Set packet buffer
    pub fn set_buffer(&mut self, buffer: ByteBuffer) {
        self.buffer = buffer;
    }

    /// Get buffer length
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Same as `packet.len() == 0`
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get buffer bytes
    pub fn get_bytes(&self) -> Vec<u8> {
        self.buffer.as_bytes().to_vec()
    }
}

impl DataBufferReader for Packet {
    fn read_bytes(&mut self, size: usize) -> Result<Vec<u8>, ProtocolError> {
        let mut buf = vec![0; size];
        match self.buffer.read_exact(&mut buf) {
            Ok(_) => Ok(buf),
            Err(_) => Err(ProtocolError::ReadError),
        }
    }
}

impl DataBufferWriter for Packet {
    fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), ProtocolError> {
        match self.buffer.write_all(bytes) {
            Ok(_) => Ok(()),
            Err(_) => Err(ProtocolError::WriteError),
        }
    }
}

/// Minecraft connection, wrapper for stream with compression
pub struct MinecraftConnection<T> {
    stream: T,
    compression_threshold: Option<usize>,
    compression_type: u32,
}

impl MinecraftConnection<TcpStream> {
    /// Connect to Minecraft Server with TcpStream
    pub fn connect(addr: &str) -> Result<MinecraftConnection<TcpStream>, ProtocolError> {
        let addr = match addr.to_socket_addrs() {
            Ok(mut i) => match i.next() {
                Some(i) => i,
                None => return Err(ProtocolError::AddressParseError),
            },
            Err(_) => return Err(ProtocolError::AddressParseError),
        };

        let stream: TcpStream = match TcpStream::connect(addr) {
            Ok(i) => i,
            Err(_) => return Err(ProtocolError::StreamConnectError),
        };

        Ok(MinecraftConnection {
            stream,
            compression_threshold: None,
            compression_type: 1,
        })
    }

    /// Close TcpStream
    pub fn close(&mut self) {
        let _ = self.stream.shutdown(std::net::Shutdown::Both);
    }

    /// Try clone MinecraftConnection with compression and stream
    pub fn try_clone(&mut self) -> Result<MinecraftConnection<TcpStream>, ProtocolError> {
        match self.stream.try_clone() {
            Ok(stream) => Ok(MinecraftConnection {
                stream,
                compression_threshold: self.compression_threshold,
                compression_type: self.compression_type,
            }),
            _ => Err(ProtocolError::CloneError),
        }
    }
}

impl<T: Read + Write> DataBufferReader for MinecraftConnection<T> {
    fn read_bytes(&mut self, size: usize) -> Result<Vec<u8>, ProtocolError> {
        let mut buf = vec![0; size];
        match self.stream.read_exact(&mut buf) {
            Ok(_) => Ok(buf),
            Err(_) => Err(ProtocolError::ReadError),
        }
    }
}

impl<T: Read + Write> DataBufferWriter for MinecraftConnection<T> {
    fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), ProtocolError> {
        match self.stream.write_all(bytes) {
            Ok(_) => Ok(()),
            Err(_) => Err(ProtocolError::WriteError),
        }
    }
}

impl<T> MinecraftConnection<T> {
    /// Create new MinecraftConnection from stream
    pub fn new(stream: T) -> MinecraftConnection<T> {
        MinecraftConnection {
            stream,
            compression_threshold: None,
            compression_type: 1,
        }
    }

    /// Set compression threshold
    pub fn set_compression(&mut self, threshold: Option<usize>) {
        self.compression_threshold = threshold;
    }

    /// Get compression threshold
    pub fn compression(&self) -> Option<usize> {
        self.compression_threshold
    }

    /// Set compression type
    ///
    /// `compression_type` is integer from 0 (none) to 9 (slowest)
    /// 1 is fast compression
    /// 6 is normal compression
    pub fn set_compression_type(&mut self, compression_type: u32) {
        self.compression_type = compression_type;
    }

    /// Get compression type
    ///
    /// `compression_type` is integer from 0 (none) to 9 (slowest)
    /// 1 is fast compression
    /// 6 is normal compression
    pub fn compression_type(&self) -> u32 {
        self.compression_type
    }

    /// Get mutable reference of stream
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.stream
    }

    /// Get immutable reference of stream
    pub fn get_ref(&self) -> &T {
        &self.stream
    }
}

impl<T: Read + Write> MinecraftConnection<T> {
    /// Read [`Packet`](Packet) from connection
    pub fn read_packet(&mut self) -> Result<Packet, ProtocolError> {
        read_packet(&mut self.stream, self.compression_threshold)
    }

    /// Write [`Packet`](Packet) to connection
    pub fn write_packet(&mut self, packet: &Packet) -> Result<(), ProtocolError> {
        write_packet(
            &mut self.stream,
            self.compression_threshold,
            self.compression_type,
            packet,
        )
    }
}

#[cfg(feature = "tokio")]
impl<T: AsyncRead + AsyncWrite + Unpin> MinecraftConnection<T> {
    /// Read [`Packet`](Packet) from connection
    pub async fn read_packet_async(&mut self) -> Result<Packet, ProtocolError> {
        read_packet_async(&mut self.stream, self.compression_threshold).await
    }

    /// Write [`Packet`](Packet) to connection
    pub async fn write_packet_async(&mut self, packet: &Packet) -> Result<(), ProtocolError> {
        write_packet_async(
            &mut self.stream,
            self.compression_threshold,
            self.compression_type,
            packet,
        )
        .await
    }
}

impl<T: Clone> MinecraftConnection<T> {
    /// Clone MinecraftConnection with compression and stream
    pub fn clone(&mut self) -> MinecraftConnection<T> {
        MinecraftConnection {
            stream: self.stream.clone(),
            compression_threshold: self.compression_threshold,
            compression_type: self.compression_type,
        }
    }
}

fn compress_zlib(bytes: &[u8], compression: u32) -> Result<Vec<u8>, ProtocolError> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(compression));
    encoder.write_all(bytes).or(Err(ProtocolError::ZlibError))?;
    encoder.finish().or(Err(ProtocolError::ZlibError))
}

fn decompress_zlib(bytes: &[u8]) -> Result<Vec<u8>, ProtocolError> {
    let mut decoder = ZlibDecoder::new(bytes);
    let mut output = Vec::new();
    decoder
        .read_to_end(&mut output)
        .or(Err(ProtocolError::ZlibError))?;
    Ok(output)
}

/// MinecraftConnection shorter alias
pub type MCConn<T> = MinecraftConnection<T>;

/// MinecraftConnection\<TcpStream\> shorter alias
pub type MCConnTcp = MinecraftConnection<TcpStream>;

/// Read [`Packet`](Packet) from stream
///
/// `compression` here is atomic usize
/// usize::MAX means that compression is disabled
///
/// `ordering` is order how to load atomic
pub fn read_packet<T: Read>(
    stream: &mut T,
    compression_threshold: Option<usize>,
) -> Result<Packet, ProtocolError> {
    let mut data: Vec<u8>;

    let packet_length = stream.read_usize_varint_size()?;

    if compression_threshold.is_some() {
        let data_length = stream.read_usize_varint_size()?;

        data = stream.read_bytes(packet_length.0 - data_length.1)?;

        if data_length.0 != 0 {
            data = decompress_zlib(&data)?;
        }
    } else {
        data = stream.read_bytes(packet_length.0)?;
    }

    Packet::from_data(&data)
}

/// Write [`Packet`](Packet) to stream
///
/// `compression` here is atomic usize
/// usize::MAX means that compression is disabled
///
/// `ordering` is order how to load atomic
///
/// `compression_type` is integer from 0 (none) to 9 (longest)
/// 1 is fast compression
/// 6 is normal compression
pub fn write_packet<T: Write>(
    stream: &mut T,
    compression_threshold: Option<usize>,
    compression_type: u32,
    packet: &Packet,
) -> Result<(), ProtocolError> {
    let mut buf = ByteBuffer::new();

    let mut data_buf = ByteBuffer::new();
    data_buf.write_u8_varint(packet.id)?;
    data_buf.write_buffer(&packet.buffer)?;

    if let Some(compression_threshold) = compression_threshold {
        let mut packet_buf = ByteBuffer::new();

        if data_buf.len() >= compression_threshold {
            let compressed_data = compress_zlib(data_buf.as_bytes(), compression_type)?;
            packet_buf.write_usize_varint(data_buf.len())?;
            packet_buf
                .write_all(&compressed_data)
                .or(Err(ProtocolError::WriteError))?;
        } else {
            packet_buf.write_usize_varint(0)?;
            packet_buf.write_buffer(&data_buf)?;
        }

        buf.write_usize_varint(packet_buf.len())?;
        buf.write_buffer(&packet_buf)?;
    } else {
        buf.write_usize_varint(data_buf.len())?;
        buf.write_buffer(&data_buf)?;
    }

    stream.write_buffer(&buf)?;

    Ok(())
}

#[cfg(feature = "tokio")]
async fn read_usize_varint_size_async<T: AsyncRead + Unpin>(
    stream: &mut T,
) -> Result<(usize, usize), ProtocolError> {
    let mut shift: usize = 0;
    let mut decoded: usize = 0;
    let mut size: usize = 0;

    loop {
        let next = stream.read_u8().await.or(Err(ProtocolError::VarIntError))?;
        size += 1;

        if shift >= (std::mem::size_of::<usize>() * 8) {
            return Err(ProtocolError::VarIntError);
        }

        decoded |= ((next & 0b01111111) as usize) << shift;

        if next & 0b10000000 == 0b10000000 {
            shift += 7;
        } else {
            return Ok((decoded, size));
        }
    }
}

#[cfg(feature = "tokio")]
/// Read [`Packet`](Packet) from stream
///
/// `compression` here is atomic usize
/// usize::MAX means that compression is disabled
///
/// `ordering` is order how to load atomic
pub async fn read_packet_async<T: AsyncRead + Unpin>(
    stream: &mut T,
    compression_threshold: Option<usize>,
) -> Result<Packet, ProtocolError> {
    let packet_length = read_usize_varint_size_async(stream).await?;

    let data = if compression_threshold.is_some() {
        let data_length = read_usize_varint_size_async(stream).await?;

        let mut data = vec![0u8; packet_length.0 - data_length.1];

        stream
            .read_exact(&mut data)
            .await
            .or(Err(ProtocolError::DataRanOutError))?;

        if data_length.0 != 0 {
            data = decompress_zlib(&data)?;
        }

        data
    } else {
        let mut data = vec![0u8; packet_length.0];

        stream
            .read_exact(&mut data)
            .await
            .or(Err(ProtocolError::DataRanOutError))?;

        data
    };

    Packet::from_data(&data)
}

#[cfg(feature = "tokio")]
/// Write [`Packet`](Packet) to stream
///
/// `compression` here is atomic usize
/// usize::MAX means that compression is disabled
///
/// `ordering` is order how to load atomic
///
/// `compression_type` is integer from 0 (none) to 9 (longest)
/// 1 is fast compression
/// 6 is normal compression
pub async fn write_packet_async<T: AsyncWrite + Unpin>(
    stream: &mut T,
    compression_threshold: Option<usize>,
    compression_type: u32,
    packet: &Packet,
) -> Result<(), ProtocolError> {
    let mut buf = ByteBuffer::new();

    let mut data_buf = ByteBuffer::new();
    data_buf.write_u8_varint(packet.id)?;
    data_buf.write_buffer(&packet.buffer)?;

    if let Some(compression_threshold) = compression_threshold {
        let mut packet_buf = ByteBuffer::new();

        if data_buf.len() >= compression_threshold {
            let compressed_data = compress_zlib(data_buf.as_bytes(), compression_type)?;
            packet_buf.write_usize_varint(data_buf.len())?;
            packet_buf
                .write_all(&compressed_data)
                .or(Err(ProtocolError::WriteError))?;
        } else {
            packet_buf.write_usize_varint(0)?;
            packet_buf.write_buffer(&data_buf)?;
        }

        buf.write_usize_varint(packet_buf.len())?;
        buf.write_buffer(&packet_buf)?;
    } else {
        buf.write_usize_varint(data_buf.len())?;
        buf.write_buffer(&data_buf)?;
    }

    stream
        .write_all(buf.as_bytes())
        .await
        .or(Err(ProtocolError::WriteError))
}
