# frozen_string_literal: true
# rubocop:todo all

# Query for all documents in a collection

cursor = client[:restaurants].find

cursor.each do |doc|
  puts doc
end

# Query for equality on a top level field

cursor = client[:restaurants].find('borough' => 'Manhattan')

cursor.each do |doc|
  puts doc
end

# Query by a field in an embedded document

cursor = client[:restaurants].find('address.zipcode' => '10075')

cursor.each do |doc|
  puts doc
end

# Query by a field in an array

cursor = client[:restaurants].find('grades.grade' => 'B')

cursor.each do |doc|
  puts doc
end

# Query with the greater-than operator

cursor = client[:restaurants].find('grades.score' => { '$gt' => 30 })

cursor.each do |doc|
  puts doc
end

# Query with the less-than operator

cursor = client[:restaurants].find('grades.score' => { '$lt' => 10 })

cursor.each do |doc|
  puts doc
end

# Query with a logical conjuction (AND) of query conditions

cursor = client[:restaurants].find({ 'cuisine' => 'Italian',
                                     'address.zipcode' => '10075'})

cursor.each do |doc|
  puts doc
end

# Query with a logical disjunction (OR) of query conditions

cursor = client[:restaurants].find('$or' => [{ 'cuisine' => 'Italian' },
                                             { 'address.zipcode' => '10075'}
                                            ]
                                  )

cursor.each do |doc|
  puts doc
end

# Sort query results

cursor = client[:restaurants].find.sort('borough' => Mongo::Index::ASCENDING,
                                        'address.zipcode' => Mongo::Index::DESCENDING)

cursor.each do |doc|
  puts doc
end
