fn main() {
    cc::Build::new()
        .file("src/c/extractor.c")
        .compile("extractor");
}
