# BitHelper.jl
Some Julia helper functions for basic data I/O and querying in bit.io

To try it out, install from GitHub.

```julia-repl
pkg> add "https://github.com/dliden-bitdotio/BitHelper.jl.git"
```

Then use `bit.import!()` to import data to a bit.io repository; `bit.query!()` to query a repo, and `bit.download_table` to download a full table from a bit.io repo.

`bit.install_key()` can be used to save a bit.io pg_string and password to a LocalPreferences.toml file so they do not need to be manually added to each function call.

## Usage Examples

### Install Key

```julia-repl
julia> bit.install_key!(ENV["bitio_pg_string"]) # if bitio_pg_string is in ENV
[ Info: replacing bit.io pg string
[ Info: bit.io pg_string and password saved to LocalPreferences.toml. Make sure to add this file to your .gitignore!
```

### Query

```julia-repl
julia> bit.query!(raw"select count(*) from "\$username/\$repo"."\$tablename";")
```

### Download Table

```julia-repl
julia> bit.download_table(myusername, myschemaname, myreponame)
DataFrame ...
```

### Import Table

```julia-repl
julia> bit.import!(mydf, myusername, myschema, mytablename, if_exists="truncate")
HTTP.Messages.Response:
...
