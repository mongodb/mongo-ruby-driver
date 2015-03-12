# Create a single field index
result = client[:restaurants].indexes.create(cuisine: 1)

# Create a compound index
result = client[:restaurants].indexes.create(cuisine: 1, zipcode: -1)