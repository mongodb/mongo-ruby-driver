# frozen_string_literal: true
# rubocop:todo all

# Update top-level fields in a single document

client[:restaurants].find(name: 'Juni').update_one('$set'=> { 'cuisine' => 'American (New)' },
                                                   '$currentDate' => { 'lastModified'  => true })

# Update an embedded document in a single document

client[:restaurants].find(restaurant_id: '41156888').update_one('$set'=> { 'address.street' => 'East 31st Street' })

# Update multiple documents

client[:restaurants].find('address.zipcode' => '10016').update_many('$set'=> { 'borough' => 'Manhattan' },
                                                                    '$currentDate' => { 'lastModified'  => true })

# Replace the contents of a single document

client[:restaurants].find(restaurant_id: '41704620').replace_one(
                                                       'name' => 'Vella 2',
                                                       'address' => {
                                                          'coord' => [-73.9557413, 40.7720266],
                                                          'building' => '1480',
                                                          'street' => '2 Avenue',
                                                          'zipcode' => '10075'
                                                        }
                                                     )
