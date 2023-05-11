# frozen_string_literal: true
# rubocop:todo all

# Create a single field index

result = client[:restaurants].indexes.create_one(cuisine: Mongo::Index::ASCENDING)

# Create a compound index

result = client[:restaurants].indexes.create_one(cuisine: 1, zipcode: Mongo::Index::DESCENDING)

# Create a single field unique index

result = client[:restaurants].indexes.create_one({ cuisine: Mongo::Index::ASCENDING }, unique: true)
