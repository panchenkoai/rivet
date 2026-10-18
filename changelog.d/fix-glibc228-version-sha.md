- **`rivet --version` printed `(unknown)` instead of the commit on the glibc 2.28 Linux assets.**
  The container that builds them checked the tree out without `.git`, so the build could not read
  the commit (0.32.0). **Now** they print `rivet <version> (<commit>)` like every other asset, and
  the release refuses to publish one that does not. **What to do:** nothing.
