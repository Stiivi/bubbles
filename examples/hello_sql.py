import bubbles

# Follow the comments – there is a line to be uncommented

URL = "https://raw.github.com/Stiivi/cubes/master/examples/hello_world/data.csv"

# Prepare list of stores, we just need one temporary SQL store

stores = {
    "target": bubbles.open_store("sql", "sqlite:///")
}


p = bubbles.Pipeline(stores=stores)
p.source_object("csv_source", resource=URL, encoding="utf8")
p.retype({"Amount (US$, Millions)": "integer"})

# We create a table
# Uncomment this line and see the difference in debug messages
p.create("target", "data")

p.aggregate("Category", "Amount (US$, Millions)")
p.pretty_print()
p.run()

