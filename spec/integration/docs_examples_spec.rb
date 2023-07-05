# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'aggregation examples in Ruby' do
  before(:all) do
    # In sharded clusters we need to ensure the database exists before running
    # the tests in this file.
    begin
      ClientRegistry.instance.global_client('authorized')['_placeholder'].create
    rescue Mongo::Error::OperationFailure => e
      # Collection already exists
      if e.code != 48
        raise
      end
    end
  end

  let(:client) do
    authorized_client
  end

  context 'Aggregation Example 1 - Simple aggregation' do

    let(:example_code) do

      # Start Aggregation Example 1

      client[:sales].aggregate(
          [
              { '$match' => { 'items.fruit' => 'banana' } },
              { '$sort' => { 'date' => 1 } }
          ])

      # End Aggregation Example 1
    end

    it 'successfully executes the aggregation' do
      example_code.to_a
    end
  end

  context 'Aggregation Example 2 - $match, $group, $project, $unwind, $sum, $sort, $dayOfWeek' do

    let(:example_code) do

      # Start Aggregation Example 2

      client[:sales].aggregate(
          [
            { '$unwind' => '$items' },
            { '$match' => { 'items.fruit' => 'banana' } },
            { '$group' => {
              '_id' => { 'day' => { '$dayOfWeek' => '$date' } },
              'count' => { '$sum' => '$items.quantity' } }
            },
            { '$project' => {
                'dayOfWeek' => '$_id.day',
                'numberSold' => '$count',
                '_id' => 0 }
            },
            { '$sort' => { 'numberSold' => 1 } }
          ])

      # End Aggregation Example 2
    end

    it 'successfully executes the aggregation' do
      example_code.to_a
    end
  end

  context 'Aggregation Example 3 - $unwind, $group, $sum, $dayOfWeek, $multiply, $project, $cond' do

    let(:example_code) do

      # Start Aggregation Example 3

      client[:sales].aggregate(
          [
            { '$unwind' => '$items' },
            { '$group' => {
                           '_id' => { 'day' => { '$dayOfWeek' => '$date' } },
                           'items_sold' => { '$sum' => '$items.quantity' },
                           'revenue' => { '$sum' => { '$multiply' => [ '$items.quantity', '$items.price' ] } } }
            },
            { '$project' => { 'day' => '$_id.day',
                              'revenue' => 1,
                              'items_sold' => 1,
                              'discount' => {
                                             '$cond' => { 'if' => { '$lte' => ['$revenue', 250]},
                                                          'then' => 25, 'else' => 0 } } }
            }
          ])

      # End Aggregation Example 3
    end

    it 'successfully executes the aggregation' do
      example_code.to_a
    end
  end

  context 'Aggregation Example 4 - $lookup, $filter, $match' do
    min_server_fcv '3.6'

    let(:example_code) do

      # Start Aggregation Example 4

      client[:sales].aggregate(
          [
            { '$lookup' => {
                            'from' => 'air_airlines',
                            'let' => { 'constituents' => '$airlines' },
                            'pipeline' => [ { '$match' => { '$expr' =>
                                                              { '$in' => ['$name', '$$constituents'] } } }],
                            'as' => 'airlines' }
            },
            { '$project' => { '_id' => 0,
                              'name' => 1,
                              'airlines' => {
                              '$filter' => { 'input' => '$airlines',
                                             'as' => 'airline',
                                             'cond' => { '$eq' => ['$$airline.country', 'Canada'] } } } }
            }
          ])

      # End Aggregation Example 4
    end

    it 'successfully executes the aggregation' do
      example_code.to_a
    end
  end

  context 'runCommand Example 1' do

    let(:example_code) do

      # Start runCommand Example 1

      client.database.command(buildInfo: 1)

      # End runCommand Example 1
    end

    it 'successfully executes the command' do
      example_code
    end
  end

  context 'runCommand Example 2' do

    before do
      client[:restaurants].drop
      client[:restaurants].create
    end

    let(:example_code) do

      # Start runCommand Example 2

      client.database.command(dbStats: 1)

      # End runCommand Example 2
    end

    it 'successfully executes the command' do
      example_code
    end
  end

  context 'Index Example 1 - build simple ascending index' do

    let(:example_code) do

      # Start Index Example 1

      client[:records].indexes.create_one(score: 1)

      # End Index Example 1
    end

    it 'successfully executes the command' do
      example_code
    end
  end

  context 'Index Example 2 - build multikey index with partial filter expression' do

    let(:example_code) do

      # Start Index Example 2

      client[:records].indexes.create_one({ cuisine: 1, name: 1 },
                                          { partialFilterExpression: { rating: { '$gt' => 5 } } })

      # End Index Example 2
    end

    it 'successfully executes the command' do
      example_code
    end
  end
end
