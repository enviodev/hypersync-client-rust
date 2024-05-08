fn main() {
    capnpc::CompilerCommand::new()
        .file("hypersync_net_types.capnp")
        .run()
        .expect("compiling schema");
}
