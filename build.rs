fn main() {
    println!("cargo:rerun-if-changed=src/extractor.c");

    cc::Build::new()
        .file("src/c/extractor.c")
        .compile("extractor");
}
