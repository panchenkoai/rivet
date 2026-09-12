//! A wrong host in `url:` must be named as such, not surfaced as the driver's text.
//! `.invalid` never resolves (RFC 6761), so these need no stand.

fn init_error(url: &str) -> String {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_rivet"))
        .args(["init", "--source", url, "--tls", "disable"])
        .output()
        .expect("spawn rivet");
    assert!(
        !out.status.success(),
        "init against an unreachable host must fail: {url}"
    );
    String::from_utf8_lossy(&out.stderr).into_owned()
}

#[test]
fn an_unresolvable_host_is_named_on_every_engine() {
    for url in [
        "postgresql://u:p@nosuch-host.invalid:5432/db",
        "mysql://u:p@nosuch-host.invalid:3306/db",
        "sqlserver://u:p@nosuch-host.invalid:1433/db",
        "mongodb://nosuch-host.invalid:27017/db?serverSelectionTimeoutMS=2000",
    ] {
        let said = init_error(url);
        assert!(
            said.contains("cannot resolve host `nosuch-host.invalid`"),
            "{url} must name the host:\n{said}"
        );
        assert!(
            said.contains("check the host name in `url:`"),
            "{url} must point at the fix:\n{said}"
        );
    }
}

#[test]
fn a_closed_port_names_the_endpoint() {
    let said = init_error("postgresql://u:p@127.0.0.1:1/db");
    assert!(
        said.contains("nothing is listening on 127.0.0.1:1"),
        "{said}"
    );
}
