import bubbles

URL = "https://raw.github.com/Stiivi/cubes/master/examples/hello_world/data.csv"

p = bubbles.Pipeline()
p.source(bubbles.data_object("csv_source", URL, infer_fields=True))
p.aggregate("Category", "Amount (US$, Millions)")
p.pretty_print()

