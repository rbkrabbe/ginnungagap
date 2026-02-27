pub mod v1 {
    tonic::include_proto!("ginnungagap.v1");
}

pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/descriptor.bin"));
