- **A SQLite state could only live beside the config.** `RIVET_STATE_URL` took only a PostgreSQL
  URL, so a deployment whose config directory is read-only, or that keeps several configs' state
  in one place, had no way to point rivet at a SQLite file (0.32.1 and earlier). **Now**
  `RIVET_STATE_URL=sqlite:<path>` names the SQLite file for every command, `init` included; a
  relative path is anchored to the config's directory. A move that would leave the state beside
  the config behind (it holds cursors or runs, the new file none) is refused
  `RIVET_STATE_URL_LEAVES_STATE_BEHIND`, exit 5. **What to do:** to move a state, copy it with
  every run stopped (`sqlite3 .rivet_state.db ".backup <new path>"`), then set the variable.
