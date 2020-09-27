# ctf-flag-submitter

Ctf flag submitter written in rust.

This program supports an **SQLite** or **PostgreSQL** database.

## Build

```bash
git clone https://github.com/joshuachp/ctf-flag-submitter.git
cd ctf-flag-submitter
cargo build --release
```

## Run

You can either specify a Toml configuration file or pass the configuration as
command line arguments.

```bash
flag-submitter -c example.config.toml
# Or
flag-submitter --sqlite-path flag.db --url "http://localhost:8000/index.php"
--token "<TEAM_TOKEN>"
```

There is an example configuration in the file `example.config.toml`.

With the flag `--single-run` or the config value the application can be run just
one time for testing.
