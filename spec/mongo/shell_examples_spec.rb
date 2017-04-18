require 'spec_helper'

describe 'shell examples in Ruby' do

  let(:client) do
    authorized_client
  end

  before do
    client[:inventory].drop
  end

  after do
    client[:inventory].drop
  end

  context 'insert examples' do

    # example 1
    before do
      client[:inventory].insert_one({ item: 'canvas',
                                      qty: 100,
                                      tags: [ 'cotton' ],
                                      size: { h: 28, w: 35.5, uom: 'cm' } })
    end


    context 'example 2' do

      let(:example) do
        client[:inventory].find(item: 'canvas')
      end

      it 'matches the expected output' do
        expect(example.count).to eq(1)
      end
    end

    context 'example 3' do

      let(:example) do
        client[:inventory].insert_many([{ item: 'journal',
                                          qty: 25,
                                          tags: ['blank', 'red'],
                                          size: { h: 14, w: 21, uom: 'cm' }
                                        },
                                        { item: 'mat',
                                          qty: 85,
                                          tags: ['gray'],
                                          size: { h: 27.9, w: 35.5, uom: 'cm' }
                                        },
                                        { item: 'mousepad',
                                          qty: 25,
                                          tags: ['gel', 'blue'],
                                          size: { h: 19, w: 22.85, uom: 'cm'
                                          }
                                        }
                                       ])
      end

      it 'matches the expected output' do
        expect(example.inserted_count).to eq(3)
      end
    end
  end

  context 'query top-level' do

    # example 6
    before do
      client[:inventory].insert_many([{ item: 'journal',
                                        qty: 25,
                                        size: { h: 14, w: 21, uom: 'cm' },
                                        status: 'A' },
                                      { item: 'notebook',
                                        qty: 50,
                                        size: { h: 8.5, w: 11, uom: 'in' },
                                        status: 'A' },
                                      { item: 'paper',
                                        qty: 100,
                                        size: { h: 8.5, w: 11, uom: 'in' },
                                        status: 'D' },
                                      { item: 'planner',
                                        qty: 75,
                                        size: { h: 22.85, w: 30, uom: 'cm' },
                                        status: 'D' },
                                      { item: 'postcard',
                                        qty: 45,
                                        size: { h: 10, w: 15.25, uom: 'cm' },
                                        status: 'A' }
                                     ])

    end

    context 'example 7' do

      let(:example) do
        client[:inventory].find({})
      end

      it 'matches the expected output' do
        expect(example.to_a.size).to eq(5)
      end
    end

    context 'example 8' do

      let(:example) do
        client[:inventory].find
      end

      it 'matches the expected output' do
        expect(example.to_a.size).to eq(5)
      end
    end

    context 'example 9' do

      let(:example) do
        client[:inventory].find(status: 'D')
      end

      it 'matches the expected output' do
        expect(example.to_a.size).to eq(2)
      end
    end

    context 'example 10' do

      let(:example) do
        client[:inventory].find(status: { '$in' => [ 'A', 'D' ]})
      end

      it 'matches the expected output' do
        expect(example.to_a.size).to eq(5)
      end
    end

    context 'example 11' do

      let(:example) do
        client[:inventory].find(status: 'A', qty: { '$lt' => 30 })
      end

      it 'matches the expected output' do
        expect(example.to_a.size).to eq(1)
      end
    end

    context 'example 12' do

      let(:example) do
        client[:inventory].find('$or' => [{ status: 'A' },
                                          { qty: { '$lt' => 30 } }
                                         ]
                               )
      end

      it 'matches the expected output' do
        expect(example.to_a.size).to eq(3)
      end
    end

    context 'example 13' do

      let(:example) do
        client[:inventory].find(status: 'A',
                                '$or' => [{ qty: { '$lt' => 30 } },
                                          { item: { '$regex' => BSON::Regexp::Raw.new('^p') } }
                                ]
        )
      end

      it 'matches the expected output' do
        expect(example.to_a.size).to eq(2)
      end
    end
  end

  context 'query embedded documents' do

    # example 14
    before do
      client[:inventory].insert_many([
                                      { item: 'journal',
                                        qty: 25,
                                        size: { h: 14, w: 21, uom: 'cm' },
                                        status: 'A' },
                                      { item: 'notebook',
                                        qty: 50,
                                        size: { h: 8.5, w: 11, uom: 'in' },
                                        status: 'A' },
                                      { item: 'paper',
                                        qty: 100,
                                        size: { h: 8.5, w: 11, uom: 'in' },
                                        status: 'D' },
                                      { item: 'planner',
                                        qty: 75,
                                        size: { h: 22.85, w: 30, uom: 'cm' },
                                        status: 'D' },
                                      { item: 'postcard',
                                        qty: 45,
                                        size: { h: 10, w: 15.25, uom: 'cm' },
                                        status: 'A' }
                                     ])
    end

    context 'example 15' do

      let(:example) do
        client[:inventory].find(size: { h: 14,
                                        w: 21,
                                        uom: 'cm' })
      end

      it 'matches the expected output' do
        expect(example.to_a.size).to eq(1)
      end
    end

    context 'example 16' do

      let(:example) do
        client[:inventory].find(size: { h: 21,
                                        w: 14,
                                        uom: 'cm' })
      end

      it 'matches the expected output' do
        expect(example.to_a.size).to eq(0)
      end
    end

    context 'example 17' do

      let(:example) do
        client[:inventory].find('size.uom' => 'in')
      end

      it 'matches the expected output' do
        expect(example.to_a.size).to eq(2)
      end
    end

    context 'example 18' do

      let(:example) do
        client[:inventory].find('size.h' => { '$lt' =>  15 })
      end

      it 'matches the expected output' do
        expect(example.to_a.size).to eq(4)
      end
    end

    context 'example 19' do

      let(:example) do
        client[:inventory].find('size.h'   => { '$lt' => 15 },
                                'size.uom' => 'in',
                                'status'   => 'D')
      end

      it 'matches the expected output' do
        expect(example.to_a.size).to eq(1)
      end
    end
  end

  context 'query arrays' do

    # example 20
    before do
      client[:inventory].insert_many([{ item: 'journal',
                                        qty: 25,
                                        tags: ['blank', 'red'],
                                        dim_cm: [ 14, 21 ] },
                                      { item: 'notebook',
                                        qty: 50,
                                        tags: ['red', 'blank'],
                                        dim_cm: [ 14, 21 ] },
                                      { item: 'paper',
                                        qty: 100,
                                        tags: ['red', 'blank', 'plain'],
                                        dim_cm: [ 14, 21 ] },
                                      { item: 'planner',
                                        qty: 75,
                                        tags: ['blank', 'red'],
                                        dim_cm: [ 22.85, 30 ] },
                                      { item: 'postcard',
                                        qty: 45,
                                        tags: ['blue'],
                                        dim_cm: [ 10, 15.25 ] }
                                     ])
    end

    context 'example 21' do

      let(:example) do
        client[:inventory].find(tags: ['red', 'blank'])
      end

      it 'matches the expected output' do
        expect(example.to_a.size).to eq(1)
      end
    end

    context 'example 22' do

      let(:example) do
        client[:inventory].find(tags: { '$all' => ['red', 'blank'] })
      end

      it 'matches the expected output' do
        expect(example.to_a.size).to eq(4)
      end
    end

    context 'example 23' do

      let(:example) do
        client[:inventory].find(tags: 'red')
      end

      it 'matches the expected output' do
        expect(example.count).to eq(4)
      end
    end

    context 'example 24' do

      let(:example) do
        client[:inventory].find(dim_cm: { '$gt' => 25 })
      end

      it 'matches the expected output' do
        expect(example.count).to eq(1)
      end
    end

    context 'example 25' do

      let(:example) do
        client[:inventory].find(dim_cm: { '$gt' => 15,
                                          '$lt' => 20 })
      end

      it 'matches the expected output' do
        expect(example.count).to eq(4)
      end
    end

    context 'example 26' do

      let(:example) do
        client[:inventory].find(dim_cm: { '$elemMatch' => { '$gt' => 22,
                                                            '$lt' => 30 } })
      end

      it 'matches the expected output' do
        expect(example.count).to eq(1)
      end
    end

    context 'example 27' do

      let(:example) do
        client[:inventory].find('dim_cm.1' => { '$gt' => 25 })
      end

      it 'matches the expected output' do
        expect(example.count).to eq(1)
      end
    end

    context 'example 28' do

      let(:example_28) do
        client[:inventory].find(tags: { '$size' => 3 })
      end

      it 'matches the expected output' do
        expect(example_28.count).to eq(1)
      end
    end
  end

  context 'query array of embedded documents' do

    # example 29
    before do
      client[:inventory].insert_many([{ item: 'journal',
                                        instock: [ { warehouse: 'A', qty: 5 },
                                                   { warehouse: 'C', qty: 15 }] },
                                      { item: 'notebook',
                                        instock: [ { warehouse: 'C', qty: 5 }] },
                                      { item: 'paper',
                                        instock: [ { warehouse: 'A', qty: 60 },
                                                   { warehouse: 'B', qty: 15 }] },
                                      { item: 'planner',
                                        instock: [ { warehouse: 'A', qty: 40 },
                                                   { warehouse: 'B', qty: 5 }] },
                                      { item: 'postcard',
                                        instock: [ { warehouse: 'B', qty: 15 },
                                                   { warehouse: 'C', qty: 35 }] }
                                     ])
    end


    context 'example 30' do

      let(:example) do
        client[:inventory].find(instock: { warehouse: 'A', qty: 5 })
      end

      it 'matches the expected output' do
        expect(example.to_a.size).to eq(1)
      end
    end

    context 'example 31' do

      let(:example) do
        client[:inventory].find(instock: { qty: 5, warehouse: 'A' } )
      end

      it 'matches the expected output' do
        expect(example.to_a.size).to eq(0)
      end
    end

    context 'example 32' do

      let(:example) do
        client[:inventory].find('instock.0.qty' => { '$lte' => 20 })
      end

      it 'matches the expected output' do
        expect(example.to_a.size).to eq(3)
      end
    end

    context 'example 33' do

      let(:example_33) do
        client[:inventory].find('instock.qty' => { '$lte' => 20 })
      end

      it 'matches the expected output' do
        expect(example_33.to_a.size).to eq(5)
      end
    end

    context 'example 34' do

      let(:example) do
        client[:inventory].find(instock: { '$elemMatch' => { qty: 5,
                                                             warehouse: 'A' } })
      end

      it 'matches the expected output' do
        expect(example.to_a.size).to eq(1)
      end
    end

    context 'example 35' do

      let(:example) do
        client[:inventory].find(instock: { '$elemMatch' => { qty: { '$gt'  => 10,
                                                                    '$lte' => 20 } } })
      end

      it 'matches the expected output' do
        expect(example.to_a.size).to eq(3)
      end
    end

    context 'example 36' do

      let(:example) do
        client[:inventory].find('instock.qty' => { '$gt' => 10, '$lte' => 20 })
      end

      it 'matches the expected output' do
        expect(example.to_a.size).to eq(4)
      end
    end

    context 'example 37' do

      let(:example) do
        client[:inventory].find('instock.qty' => 5,
                                'instock.warehouse' => 'A')
      end

      it 'matches the expected output' do
        expect(example.to_a.size).to eq(2)
      end
    end
  end

  context 'query null' do

    # example 38
    before do
      client[:inventory].insert_many([{ _id: 1, item: nil },
                                      { _id: 2 }])
    end

    context 'example 39' do

      let(:example) do
        client[:inventory].find(item: nil)
      end

      it 'matches the expected output' do
        expect(example.to_a.size).to eq(2)
      end
    end

    context 'example 40' do

      let(:example) do
        client[:inventory].find(item: { '$type' => 10 })
      end

      it 'matches the expected output' do
        expect(example.to_a.size).to eq(1)
      end
    end

    context 'example 41' do

      let(:example) do
        client[:inventory].find(item: { '$exists' => false })
      end

      it 'matches the expected output' do
        expect(example.to_a.size).to eq(1)
      end
    end
  end

  context 'projection' do

    # example 42
    before do
      client[:inventory].insert_many([{ item: 'journal',
                                        status: 'A',
                                        size: { h: 14, w: 21, uom: 'cm' },
                                        instock: [ { warehouse: 'A', qty: 5 } ] },
                                      { item: 'notebook',
                                        status: 'A',
                                        size: { h: 8.5, w: 11, uom: 'in' },
                                        instock: [ { warehouse: 'C', qty: 5 } ] },
                                      { item: 'paper',
                                        status: 'D',
                                        size: { h: 8.5, w: 11, uom: 'in' },
                                        instock: [ { warehouse: 'A', qty: 60 } ] },
                                      { item: 'planner',
                                        status: 'D',
                                        size: { h: 22.85, w: 30, uom: 'cm' },
                                        instock: [ { warehouse: 'A', qty: 40 } ] },
                                      { item: 'postcard',
                                        status: 'A',
                                        size: { h: 10, w: 15.25, uom: 'cm' },
                                        instock: [ { warehouse: 'B', qty: 15 },
                                                   { warehouse: 'C', qty: 35 } ] }])
    end


    context 'example 43' do

      let(:example) do
        client[:inventory].find(status: 'A')
      end

      it 'matches the expected output' do
        expect(example.to_a.size).to eq(3)
      end
    end

    context 'example 44' do

      let!(:example) do
        client[:inventory].find({ status: 'A' },
                               projection: { item: 1, status: 1 })
      end

      it 'matches the expected output' do
        expect(example.to_a[1]['_id']).not_to be_nil
        expect(example.to_a[1]['item']).not_to be_nil
        expect(example.to_a[1]['status']).not_to be_nil
        expect(example.to_a[1]['size']).to be_nil
        expect(example.to_a[1]['instock']).to be_nil
      end
    end

    context 'example 45' do

      let!(:example) do
        client[:inventory].find({ status: 'A' },
                                projection: { item: 1, status: 1, _id: 0 })
      end

      it 'matches the expected output' do
        expect(example.to_a[1]['_id']).to be_nil
        expect(example.to_a[1]['item']).not_to be_nil
        expect(example.to_a[1]['status']).not_to be_nil
        expect(example.to_a[1]['size']).to be_nil
        expect(example.to_a[1]['instock']).to be_nil
      end
    end

    context 'example 46' do

      let!(:example) do
        client[:inventory].find({ status: 'A' },
                                projection: { status: 0, instock: 0 })
      end

      it 'matches the expected output' do
        expect(example.to_a[1]['_id']).not_to be_nil
        expect(example.to_a[1]['item']).not_to be_nil
        expect(example.to_a[1]['status']).to be_nil
        expect(example.to_a[1]['size']).not_to be_nil
        expect(example.to_a[1]['instock']).to be_nil
      end
    end

    context 'example 47' do

      let!(:example) do
        client[:inventory].find({ status: 'A' },
                                projection: { 'item' => 1, 'status' => 1, 'size.uom' => 1 })
      end

      it 'matches the expected output' do
        expect(example.to_a[1]['_id']).not_to be_nil
        expect(example.to_a[1]['item']).not_to be_nil
        expect(example.to_a[1]['status']).not_to be_nil
        expect(example.to_a[1]['size']).not_to be_nil
        expect(example.to_a[1]['instock']).to be_nil
        expect(example.to_a[1]['size']).not_to be_nil
        expect(example.to_a[1]['size']['uom']).not_to be_nil
        expect(example.to_a[1]['size']['h']).to be_nil
        expect(example.to_a[1]['size']['w']).to be_nil
      end
    end

    context 'example 48' do

      let!(:example) do
        client[:inventory].find({ status: 'A' },
                                projection: { 'size.uom' => 0 })
      end

      it 'matches the expected output' do
        expect(example.to_a[1]['_id']).not_to be_nil
        expect(example.to_a[1]['item']).not_to be_nil
        expect(example.to_a[1]['status']).not_to be_nil
        expect(example.to_a[1]['size']).not_to be_nil
        expect(example.to_a[1]['instock']).not_to be_nil
        expect(example.to_a[1]['size']).not_to be_nil
        expect(example.to_a[1]['size']['uom']).to be_nil
        expect(example.to_a[1]['size']['h']).not_to be_nil
        expect(example.to_a[1]['size']['w']).not_to be_nil
      end
    end

    context 'example 49' do

      let!(:example) do
        client[:inventory].find({ status: 'A' },
                                projection: {'item' => 1, 'status' => 1, 'instock.qty' => 1 })
      end

      let(:instock_list) do
        example.to_a[1]['instock']
      end

      it 'matches the expected output' do
        expect(example.to_a[1]['_id']).not_to be_nil
        expect(example.to_a[1]['item']).not_to be_nil
        expect(example.to_a[1]['status']).not_to be_nil
        expect(example.to_a[1]['size']).to be_nil
        expect(example.to_a[1]['instock']).not_to be_nil
        expect(instock_list.collect { |doc| doc['warehouse'] }.compact).to be_empty
        expect(instock_list.collect { |doc| doc['qty'] }).to eq([5])
      end
    end

    context 'example 50' do

      let!(:example) do
        client[:inventory].find({ status: 'A' },
                                projection: {'item' => 1,
                                             'status' => 1,
                                             'instock' => { '$slice' => -1 } })
      end

      let(:instock_list) do
        example.to_a[1]['instock']
      end

      it 'matches the expected output' do
        expect(example.to_a[1]['_id']).not_to be_nil
        expect(example.to_a[1]['item']).not_to be_nil
        expect(example.to_a[1]['status']).not_to be_nil
        expect(example.to_a[1]['size']).to be_nil
        expect(example.to_a[1]['instock']).not_to be_nil
        expect(instock_list.size).to eq(1)
      end
    end
  end

  context 'update' do

    # example 51
    before do
      client[:inventory].insert_many([
                                      { item: 'canvas',
                                        qty: 100,
                                        size: { h: 28, w: 35.5, uom: 'cm' },
                                        status: 'A' },
                                      { item: 'journal',
                                        qty: 25,
                                        size: { h: 14, w: 21, uom: 'cm' },
                                        status: 'A' },
                                      { item: 'mat',
                                        qty: 85,
                                        size: { h: 27.9, w: 35.5, uom: 'cm' },
                                        status: 'A' },
                                      { item: 'mousepad',
                                        qty: 25,
                                        size: { h: 19, w: 22.85, uom: 'cm' },
                                        status: 'P' },
                                      { item: 'notebook',
                                        qty: 50,
                                        size: { h: 8.5, w: 11, uom: 'in' },
                                        status: 'P' },
                                      { item: 'paper',
                                        qty: 100,
                                        size: { h: 8.5, w: 11, uom: 'in' },
                                        status: 'D' },
                                      { item: 'planner',
                                        qty: 75,
                                        size: { h: 22.85, w: 30, uom: 'cm' },
                                        status: 'D' },
                                      { item: 'postcard',
                                        qty: 45,
                                        size: { h: 10, w: 15.25, uom: 'cm' },
                                        status: 'A' },
                                      { item: 'sketchbook',
                                        qty: 80,
                                        size: { h: 14, w: 21, uom: 'cm' },
                                        status: 'A' },
                                      { item: 'sketch pad',
                                        qty: 95,
                                        size: { h: 22.85, w: 30.5, uom: 'cm' },
                                        status: 'A' }
                                    ])
    end

    context 'example 52', if: write_command_enabled? do

      let!(:example) do
        client[:inventory].update_one({ item: 'paper'},
                                      { '$set' => { 'size.uom' => 'cm', 'status' => 'P' },
                                        '$currentDate' => { 'lastModified' => true } })
      end

      it 'matches the expected output' do
        expect(client[:inventory].find(item: 'paper').all? { |doc| doc['size']['uom'] == 'cm'}).to be(true)
        expect(client[:inventory].find(item: 'paper').all? { |doc| doc['status'] == 'P'}).to be(true)
        expect(client[:inventory].find(item: 'paper').all? { |doc| doc['lastModified'] }).to be(true)
      end
    end

    context 'example 53', if: write_command_enabled? do

      let!(:example) do
        client[:inventory].update_many({ qty: { '$lt' => 50 } },
                                      { '$set' => { 'size.uom' => 'in', 'status' => 'P' },
                                        '$currentDate' => { 'lastModified' => true } })
      end

      let(:from_db) do
        client[:inventory].find(qty: { '$lt' => 50 })
      end

      it 'matches the expected output' do
        expect(from_db.all? { |doc| doc['size']['uom'] == 'in'}).to be(true)
        expect(from_db.all? { |doc| doc['status'] == 'P'}).to be(true)
        expect(from_db.all? { |doc| doc['lastModified'] }).to be(true)
      end
    end

    context 'example 54' do

      let!(:example) do
        client[:inventory].replace_one({ item: 'paper' },
                                       { item: 'paper',
                                         instock: [ { warehouse: 'A', qty: 60 },
                                                    { warehouse: 'B', qty: 40 } ] })
      end

      let(:from_db) do
        client[:inventory].find({ item: 'paper' }, projection: { _id: 0 })
      end

      it 'matches the expected output' do
        expect(from_db.first.keys.size).to eq(2)
        expect(from_db.first.key?('item')).to be(true)
        expect(from_db.first.key?('instock')).to be(true)
        expect(from_db.first['instock'].size).to eq(2)
      end
    end
  end

  context 'delete' do

    # example 55
    before do
      client[:inventory].insert_many([
                                      { item: 'journal',
                                        qty: 25,
                                        size: { h: 14, w: 21, uom: 'cm' },
                                        status: 'A' },
                                      { item: 'notebook',
                                        qty: 50,
                                        size: { h: 8.5, w: 11, uom: 'in' },
                                        status: 'P' },
                                      { item: 'paper',
                                        qty: 100,
                                        size: { h: 8.5, w: 11, uom: 'in' },
                                        status: 'D' },
                                      { item: 'planner',
                                        qty: 75,
                                        size: { h: 22.85, w: 30, uom: 'cm' },
                                        status: 'D' },
                                      { item: 'postcard',
                                        qty: 45,
                                        size: { h: 10, w: 15.25, uom: 'cm' },
                                        status: 'A' },
                                     ])
    end

    context 'example 56' do

      let(:example) do
        client[:inventory].delete_many({})
      end

      it 'matches the expected output' do
        expect(example.deleted_count).to eq(5)
        expect(client[:inventory].find.to_a.size).to eq(0)
      end
    end

    context 'example 57' do

      let(:example) do
        client[:inventory].delete_many(status: 'A')
      end

      it 'matches the expected output' do
        expect(example.deleted_count).to eq(2)
        expect(client[:inventory].find.to_a.size).to eq(3)
      end
    end

    context 'example 58' do

      let(:example) do
        client[:inventory].delete_one(status: 'D')
      end

      it 'matches the expected output' do
        expect(example.deleted_count).to eq(1)
        expect(client[:inventory].find.to_a.size).to eq(4)
      end
    end
  end
end
