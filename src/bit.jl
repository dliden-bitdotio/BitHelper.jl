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

"""Saves a bit.io pg_string and password to a LocalPreferences.toml file"""
function install_key!(bitio_pg_string::AbstractString)
    if @has_preference("bitio_pg_string")
        print("replacing bit.io pg string")
    end

    # extract password from pg_string
    pw = match(r":(.*):(.*)@", bitio_pg_string)[2]

    # set preferences
    @set_preferences!("bitio_pg_string" => bitio_pg_string,
                      "bitio_key" => pw)

    @info("bit.io pg_string and password saved to LocalPreferences.toml. Make sure to add this file to your .gitignore!")
end

"""Executes a query on bit.io."""
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


"""Downloads a full table given the pg_string, username,
   schema name, and table name"""
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

"""Given a DataFrame, imports the DataFrame to the Database
   table/schema defined by the username, tablename, schema fields"""
function import!(df, username, schema, tablename; bitio_key=missing,
                 create_table_if_not_exists=true, if_exists="append")
    if ismissing(bitio_key)
        bitio_key = @load_preference("bitio_key", missing)
        if ismissing(bitio_key)
            throw(ErrorException("Please include your API key or install your pg_string with the install_key! method"))
        end
    end

    if if_exists=="truncate"
        query!("""TRUNCATE "$username/$schema"."$tablename";""")
    elseif if_exists=="replace"
        query!("""DROP TABLE "$username/$schema"."$tablename";""")
    elseif if_exists != "append"
        throw(ErrorException("if_exists must be one of 'append', 'truncate', or 'replace'"))
    end

    url = "https://api.bit.io/api/v1beta/import/json/"
    payload = Dict("create_table_if_not_exists" => create_table_if_not_exists,
		"table_name" => tablename,
		"repo_name" => schema,
		"data" => arraytable(df))
	headers = Dict(
	    "Accept" => "application/json",
	    "Content-Type"=> "application/json",
	    "Authorization"=> "Bearer $bitio_key"
	)
	HTTP.post(url, headers, json(payload))
end

end #module
