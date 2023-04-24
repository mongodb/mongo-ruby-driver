# frozen_string_literal: true
# rubocop:todo all

# Insert a document

require 'date'

result = client[:restaurants].insert_one({
    address: {
              street: '2 Avenue',
              zipcode: 10075,
              building: 1480,
              coord: [-73.9557413, 40.7720266]
             },
    borough: 'Manhattan',
    cuisine: 'Italian',
    grades: [
             {
               date: DateTime.strptime('2014-10-01', '%Y-%m-%d'),
               grade: 'A',
               score: 11
             },
             {
               date: DateTime.strptime('2014-01-16', '%Y-%m-%d'),
               grade: 'B',
               score: 17
             }
            ],
    name: 'Vella',
    restaurant_id: '41704620'
  })

result.n #=> returns 1, because 1 document was inserted.
