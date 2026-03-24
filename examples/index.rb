# frozen_string_literal: true

# Create a single field index

result = client[:restaurants].indexes.create_one(cuisine: Mongo::Index::ASCENDING)
p result.ok?

# Create a compound index

result = client[:restaurants].indexes.create_one(cuisine: 1, zipcode: Mongo::Index::DESCENDING)
p result.ok?

# Create a single field unique index

result = client[:restaurants].indexes.create_one({ cuisine: Mongo::Index::ASCENDING }, unique: true)
p result.ok?
