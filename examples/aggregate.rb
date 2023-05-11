# frozen_string_literal: true
# rubocop:todo all

# Group documents by field and calculate count.

coll = client[:restaurants]

results = coll.find.aggregate([ { '$group' => { '_id' => '$borough',
                                                'count' => { '$sum' => 1 }
                                              }
                                }
                              ])

results.each do |result|
  puts result
end

# Filter and group documents

results = coll.find.aggregate([ { '$match' => { 'borough' => 'Queens',
                                                'cuisine' => 'Brazilian' } },
                                { '$group' => { '_id' => '$address.zipcode', 
                                                'count' => { '$sum' => 1 } } }
                              ])

results.each do |result|
  puts result
end
