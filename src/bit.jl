module bit
using HTTP, LibPQ, JSON, DataFrames, SQLStrings, JSONTables

# load preferences module
@static if VERSION >= v"1.6"
    using Preferences
end

@static if VERSION >= v"1.6"
    preference = @load_preference("preference", "default")
else
    preference = "default"
end

"""
    install_key!(bitio_pg_string)

Save a bit.io pg_string and password to a LocalPreferences.toml file

Make sure to add the LocalPreferences.toml file to .gitignore if in a version-controlled directory!

# Examples
```julia-repl
julia> bit.install_key!(ENV["bitio_pg_string"]) # if bitio_pg_string is in ENV
[ Info: replacing bit.io pg string
[ Info: bit.io pg_string and password saved to LocalPreferences.toml. Make sure to add this file to your .gitignore!
```
"""
function install_key!(bitio_pg_string::AbstractString)
    if @has_preference("bitio_pg_string")
        @info("replacing bit.io pg string")
    end

    # extract password from pg_string
    pw = match(r":(.*):(.*)@", bitio_pg_string)[2]

    # set preferences
    @set_preferences!("bitio_pg_string" => bitio_pg_string,
                      "bitio_key" => pw)

    @info("bit.io pg_string and password saved to LocalPreferences.toml. Make sure to add this file to your .gitignore!")
end

"""
    bit.query!(query, pg_string=missing)

Execute a query on bit.io. This function checks whether the pg\\_string was saved with `install_key!()`.
You can pass the `pg_string` as an optional argument if you do not want to save it to a config file.

# Example

```julia-repl
julia> bit.query!(raw"select count(*) from "\$username/\$repo"."\$tablename";")

```
"""
function query!(query; pg_string=missing)
    if ismissing(pg_string)
        pg_string = @load_preference("bitio_pg_string", missing)
        if ismissing(pg_string)
            throw(ErrorException("Please include a pg_string argument or install your pg_string with the install_key! method"))
        end
    end

    query = sql`$query`
    result = LibPQ.Connection(pg_string) do conn
        execute(conn, query.args[1])
    end
    return result
end


"""
    bit.download_table(username, schema, tablename, pg_string=missing)

Downloads a full table from a bit.io repository. This function checks whether the pg\\_string was saved with `install_key!()`.
You can pass the `pg_string` as an optional argument if you do not want to save it to a config file.

# Example

```julia-repl
julia> bit.download_table(myusername, myschemaname, myreponame)
DataFrame ...
```
"""
function download_table(username, schema, tablename; pg_string=missing)
    if ismissing(pg_string)
        pg_string = @load_preference("bitio_pg_string", missing)
        if ismissing(pg_string)
            throw(ErrorException("Please include a pg_string argument or install your pg_string with the install_key! method"))
        end
    end 
    t = LibPQ.Connection(pg_string) do conn
        execute(conn, """SELECT * FROM "$username/$schema"."$tablename";""")
	end
	return DataFrame(t)
end

"""
    bit.import!(df, username, schema, tablename, bitio_key=missing,
                create_table_if_not_exists=true, if_exists="append")

import data to a bit.io table in the specified schema

This function checks whether the pg\\_string was saved with `install_key!()`.
You can pass the `pg_string` as an optional argument if you do not want to save it to a config file.

the `if_exists` argument specifies what should be done if the table aready exists. Options are:
- "append": append the data as new rows to the existing table
- "truncate": remove all existing data from the table but do not delete the table itself & leave the schema as-is
- "replace': completely delete the existing table and create a new one in its place.


# Example

```julia-repl
bit.import!(mydf, myusername, myschema, mytablename, if_exists="truncate")
HTTP.Messages.Response:
...
```

"""
function import!(df, username, schema, tablename; bitio_key = missing,
    create_table_if_not_exists = true, if_exists = "append")
    if ismissing(bitio_key)
        bitio_key = @load_preference("bitio_key", missing)
        if ismissing(bitio_key)
            throw(ErrorException("Please include your API key or install your pg_string with the install_key! method"))
        end
    end

    exists = query!("""SELECT EXISTS
(
    SELECT 1
    FROM information_schema.tables 
    WHERE table_schema = '$username/$schema'
    AND table_name = '$tablename'
);"""
                    )
    exists = DataFrame(exists)[1,1]

    if exists
        if if_exists == "truncate"
            query!("""TRUNCATE "$username/$schema"."$tablename";""")
        elseif if_exists == "replace"
            query!("""DROP TABLE "$username/$schema"."$tablename";""")
        elseif if_exists != "append"
            throw(ErrorException("if_exists must be one of 'append', 'truncate', or 'replace'"))
        end
    end
    

    url = "https://api.bit.io/api/v1beta/import/json/"
    payload = Dict("create_table_if_not_exists" => create_table_if_not_exists,
        "table_name" => tablename,
        "repo_name" => schema,
        "data" => arraytable(df))
    headers = Dict(
        "Accept" => "application/json",
        "Content-Type" => "application/json",
        "Authorization" => "Bearer $bitio_key"
    )
    HTTP.post(url, headers, json(payload))
end

end #module
