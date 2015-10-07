require 'spec_helper'

describe Mongo::Collection::View::Builder::Modifiers do

  describe '.map_driver_options' do

    shared_examples_for 'transformable driver options' do

      it 'maps hint' do
        expect(transformed[:hint]).to eq("_id" => 1)
      end

      it 'maps comment' do
        expect(transformed[:comment]).to eq('testing')
      end

      it 'maps max scan' do
        expect(transformed[:max_scan]).to eq(200)
      end

      it 'maps max time ms' do
        expect(transformed[:max_time_ms]).to eq(500)
      end

      it 'maps max' do
        expect(transformed[:max_value]).to eq("name" => 'joe')
      end

      it 'maps min' do
        expect(transformed[:min_value]).to eq("name" => 'albert')
      end

      it 'maps return key' do
        expect(transformed[:return_key]).to be true
      end

      it 'maps show record id' do
        expect(transformed[:show_disk_loc]).to be true
      end

      it 'maps snapshot' do
        expect(transformed[:snapshot]).to be true
      end

      it 'maps explain' do
        expect(transformed[:explain]).to be true
      end

      it 'returns a BSON document' do
        expect(transformed).to be_a(BSON::Document)
      end
    end

    context 'when the keys are strings' do

      let(:modifiers) do
        {
          '$orderby' => { name: 1 },
          '$hint' => { _id: 1 },
          '$comment' => 'testing',
          '$snapshot' => true,
          '$maxScan' => 200,
          '$max' => { name: 'joe' },
          '$min' => { name: 'albert' },
          '$maxTimeMS' => 500,
          '$returnKey' => true,
          '$showDiskLoc' => true,
          '$explain' => true
        }
      end

      let(:transformed) do
        described_class.map_driver_options(modifiers)
      end

      it_behaves_like 'transformable driver options'
    end

    context 'when the keys are symbols' do

      let(:modifiers) do
        {
          :$orderby => { name: 1 },
          :$hint => { _id: 1 },
          :$comment => 'testing',
          :$snapshot => true,
          :$maxScan => 200,
          :$max => { name: 'joe' },
          :$min => { name: 'albert' },
          :$maxTimeMS => 500,
          :$returnKey => true,
          :$showDiskLoc => true,
          :$explain => true
        }
      end

      let(:transformed) do
        described_class.map_driver_options(modifiers)
      end

      it_behaves_like 'transformable driver options'
    end
  end

  describe '.map_server_modifiers' do

    shared_examples_for 'transformable server modifiers' do

      it 'maps hint' do
        expect(transformed[:$hint]).to eq("_id" => 1)
      end

      it 'maps comment' do
        expect(transformed[:$comment]).to eq('testing')
      end

      it 'maps max scan' do
        expect(transformed[:$maxScan]).to eq(200)
      end

      it 'maps max time ms' do
        expect(transformed[:$maxTimeMS]).to eq(500)
      end

      it 'maps max' do
        expect(transformed[:$max]).to eq("name" => 'joe')
      end

      it 'maps min' do
        expect(transformed[:$min]).to eq("name" => 'albert')
      end

      it 'maps return key' do
        expect(transformed[:$returnKey]).to be true
      end

      it 'maps show record id' do
        expect(transformed[:$showDiskLoc]).to be true
      end

      it 'maps snapshot' do
        expect(transformed[:$snapshot]).to be true
      end

      it 'maps explain' do
        expect(transformed[:$explain]).to be true
      end

      it 'returns a BSON document' do
        expect(transformed).to be_a(BSON::Document)
      end

      it 'does not include non modifiers' do
        expect(transformed[:limit]).to be_nil
      end
    end

    context 'when the keys are strings' do

      let(:options) do
        {
          'sort' => { name: 1 },
          'hint' => { _id: 1 },
          'comment' => 'testing',
          'snapshot' => true,
          'max_scan' => 200,
          'max_value' => { name: 'joe' },
          'min_value' => { name: 'albert' },
          'max_time_ms' => 500,
          'return_key' => true,
          'show_disk_loc' => true,
          'explain' => true,
          'limit' => 10
        }
      end

      let(:transformed) do
        described_class.map_server_modifiers(options)
      end

      it_behaves_like 'transformable server modifiers'
    end

    context 'when the keys are symbols' do

      let(:options) do
        {
          :sort => { name: 1 },
          :hint => { _id: 1 },
          :comment => 'testing',
          :snapshot => true,
          :max_scan => 200,
          :max_value => { name: 'joe' },
          :min_value => { name: 'albert' },
          :max_time_ms => 500,
          :return_key => true,
          :show_disk_loc => true,
          :explain => true,
          :limit => 10
        }
      end

      let(:transformed) do
        described_class.map_server_modifiers(options)
      end

      it_behaves_like 'transformable server modifiers'
    end
  end
end
