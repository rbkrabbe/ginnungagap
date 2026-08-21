pub mod v1 {
    // Every generated service method returns `Result<Response<T>, tonic::Status>`,
    // and `Status` is 176 bytes against the lint's 128-byte threshold — so this
    // fires once per method on code tonic writes. The module holds nothing but
    // the generated code, so hand-written code keeps the lint.
    #![allow(clippy::result_large_err)]

    tonic::include_proto!("ginnungagap.v1");
}

pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/descriptor.bin"));
