from bubbles import Pipeline, FieldList, data_object, open_store

# Sample order data with fields:
fields = FieldList(
            ["id", "integer"],
            ["customer_id", "integer"],
            ["year", "integer"],
            ["amount", "integer"]
        )

data = [
    [1, 1, 2009, 10],
    [2, 1, 2010, 20],
    [3, 1, 2011, 20],
    [4, 1, 2012, 50],
    [5, 2, 2010, 50],
    [6, 2, 2012, 40],
    [7, 3, 2011, 100],
    [8, 3, 2012, 150],
    [9, 3, 2013, 120]
]

# Stores for SQL alternative, if enabled (see below)
stores = { "default": open_store("sql","sqlite:///") }

#
# Create the pipeline
#

p = Pipeline(stores=stores)
p.source_object("iterable_data_source", iterable=data, fields=fields)

# Uncomment this to get SQL operations instead of python iterator
p.create("default", "data")

# Find last purchase date
last_purchase = p.fork()
last_purchase.aggregate(["customer_id"],
                        [["year", "max"]],
                        include_count=False)
last_purchase.rename_fields({"year_max": "last_purchase_year"})
p.join_details(last_purchase, "customer_id", "customer_id")

p.pretty_print()

p.run()
