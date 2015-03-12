# Create a single field index
result = client[:restaurants].indexes.create(cuisine: Mongo::Index::ASCENDING)

# Create a compound index
result = client[:restaurants].indexes.create(cuisine: 1, zipcode: Mongo::Index::DESCENDING)