module bit
using HTTP, LibPQ, JSON, DataFrames

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

end #module
