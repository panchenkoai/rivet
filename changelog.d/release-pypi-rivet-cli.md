- **rivet could only be installed from a tarball, Homebrew or cargo, which a managed Airflow worker
  (Cloud Composer, MWAA, Astronomer) does not allow.** **Now** a release can publish the same
  binaries as `rivet-cli` wheels on PyPI (`manylinux_2_28` x86_64 / aarch64 and macOS arm64 /
  x86_64; `rivet` and `rivet-mcp` land in the environment's `bin`, and `rivet_cli.path()` returns
  the installed `rivet`). **What to do:** nothing; once a release is published there,
  `pip install rivet-cli` installs the binary.
