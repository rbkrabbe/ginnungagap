fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out_dir.join("descriptor.bin"))
        .compile_protos(
            &[
                "../../proto/ginnungagap/v1/types.proto",
                "../../proto/ginnungagap/v1/kv.proto",
                "../../proto/ginnungagap/v1/cluster.proto",
            ],
            &["../../proto"],
        )?;
    Ok(())
}
