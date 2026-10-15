- **The Linux release binaries did not start on Debian 12, RHEL 8/9 or the official Airflow images.**
  They were built on Ubuntu 24.04 and needed GLIBC_2.38 / 2.39: `version 'GLIBC_2.39' not found`
  (0.31.0 and earlier releases built the same way). **Now** every release also ships
  `rivet-<version>-x86_64-unknown-linux-gnu-glibc2.28.tar.gz` and `…-aarch64-…-glibc2.28.tar.gz`,
  built in manylinux_2_28, which run on any glibc 2.28 or newer; the release checks that they need
  nothing newer and that they start on Rocky Linux 8 and 9, Amazon Linux 2023, Debian 12 and
  Airflow 2.10 / 3.3 before it publishes. **What to do:** on an older distribution or in an Airflow
  image, take the `-glibc2.28` asset.
