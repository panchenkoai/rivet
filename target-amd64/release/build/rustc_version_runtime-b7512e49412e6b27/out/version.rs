
            /// Returns the `rustc` SemVer version and additional metadata
            /// like the git short hash and build date.
            pub fn version_meta() -> VersionMeta {
                VersionMeta {
                    semver: Version {
                        major: 1,
                        minor: 94,
                        patch: 1,
                        pre: Prerelease::new("").unwrap(),
                        build: BuildMetadata::new("").unwrap(),
                    },
                    host: "x86_64-unknown-linux-gnu".to_owned(),
                    short_version_string: "rustc 1.94.1 (e408947bf 2026-03-25)".to_owned(),
                    commit_hash: Some("e408947bfd200af42db322daf0fadfe7e26d3bd1".to_owned()),
                    commit_date: Some("2026-03-25".to_owned()),
                    build_date: None,
                    channel: Channel::Stable,
                    llvm_version: Some(LlvmVersion{ major: 21, minor: 1 }),
                }
            }
            