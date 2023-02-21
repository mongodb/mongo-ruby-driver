# frozen_string_literal: true
# encoding: utf-8

shared_examples 'app metadata document' do
  let(:app_metadata) do
    described_class.new({})
  end

  it 'includes Ruby driver identification' do
    expect(document[:client][:driver][:name]).to eq('mongo-ruby-driver')
    expect(document[:client][:driver][:version]).to eq(Mongo::VERSION)
  end

  context 'linux' do
    before(:all) do
      unless SpecConfig.instance.linux?
        skip "Linux required, we have #{RbConfig::CONFIG['host_os']}"
      end
    end

    it 'includes operating system information' do
      expect(document[:client][:os][:type]).to eq('linux')
      if BSON::Environment.jruby? || RUBY_VERSION >= '3.0'
        expect(document[:client][:os][:name]).to eq('linux')
      else
        # Ruby 2.7.2 and earlier use linux-gnu.
        # Ruby 2.7.3 uses linux.
        expect(%w(linux linux-gnu)).to include(document[:client][:os][:name])
      end
      expect(document[:client][:os][:architecture]).to eq('x86_64')
    end
  end

  context 'macos' do
    before(:all) do
      unless SpecConfig.instance.macos?
        skip "MacOS required, we have #{RbConfig::CONFIG['host_os']}"
      end
    end

    it 'includes operating system information' do
      expect(document[:client][:os][:type]).to eq('darwin')
      if BSON::Environment.jruby?
        expect(document[:client][:os][:name]).to eq('darwin')
      else
        expect(document[:client][:os][:name]).to match(/darwin\d+/)
      end
      expect(document[:client][:os][:architecture]).to eq('x86_64')
    end
  end

  context 'mri' do
    require_mri

    it 'includes Ruby version' do
      expect(document[:client][:platform]).to start_with("Ruby #{RUBY_VERSION}")
    end

    context 'when custom platform is specified' do
      let(:app_metadata) do
        described_class.new(platform: 'foowidgets')
      end

      it 'starts with custom platform' do
        expect(document[:client][:platform]).to start_with("foowidgets, Ruby #{RUBY_VERSION}")
      end
    end
  end

  context 'jruby' do
    require_jruby

    it 'includes JRuby and Ruby compatibility versions' do
      expect(document[:client][:platform]).to start_with("JRuby #{JRUBY_VERSION}, like Ruby #{RUBY_VERSION}")
    end

    context 'when custom platform is specified' do
      let(:app_metadata) do
        described_class.new(platform: 'foowidgets')
      end

      it 'starts with custom platform' do
        expect(document[:client][:platform]).to start_with("foowidgets, JRuby #{JRUBY_VERSION}")
      end
    end
  end

  context 'when wrapping libraries are specified' do
    let(:app_metadata) do
      described_class.new(wrapping_libraries: wrapping_libraries)
    end

    context 'one' do
      let(:wrapping_libraries) { [wrapping_library] }

      context 'no fields' do
        let(:wrapping_library) do
          {}
        end

        it 'adds empty strings' do
          expect(document[:client][:driver][:name]).to eq('mongo-ruby-driver|')
          expect(document[:client][:driver][:version]).to eq("#{Mongo::VERSION}|")
          expect(document[:client][:platform]).to match(/\AJ?Ruby[^|]+\|\z/)
        end
      end

      context 'some fields' do
        let(:wrapping_library) do
          {name: 'Mongoid'}
        end

        it 'adds the fields' do
          expect(document[:client][:driver][:name]).to eq('mongo-ruby-driver|Mongoid')
          expect(document[:client][:driver][:version]).to eq("#{Mongo::VERSION}|")
          expect(document[:client][:platform]).to match(/\AJ?Ruby[^|]+\|\z/)
        end
      end

      context 'all fields' do
        let(:wrapping_library) do
          {name: 'Mongoid', version: '7.1.2', platform: 'OS9000'}
        end

        it 'adds the fields' do
          expect(document[:client][:driver][:name]).to eq('mongo-ruby-driver|Mongoid')
          expect(document[:client][:driver][:version]).to eq("#{Mongo::VERSION}|7.1.2")
          expect(document[:client][:platform]).to match(/\AJ?Ruby[^|]+\|OS9000\z/)
        end
      end
    end

    context 'two' do
      context 'some fields' do
        let(:wrapping_libraries) do
          [
            {name: 'Mongoid', version: '42'},
            # All libraries should be specifying their versions, in theory,
            # but test not specifying a version.
            {version: '4.0', platform: 'OS9000'},
          ]
        end

        it 'adds the fields' do
          expect(document[:client][:driver][:name]).to eq('mongo-ruby-driver|Mongoid|')
          expect(document[:client][:driver][:version]).to eq("#{Mongo::VERSION}|42|4.0")
          expect(document[:client][:platform]).to match(/\AJ?Ruby[^|]+\|\|OS9000\z/)
        end
      end

      context 'a realistic Mongoid & Rails wrapping' do
        let(:wrapping_libraries) do
          [
            {name: 'Mongoid', version: '7.1.2'},
            {name: 'Rails', version: '6.0.3'},
          ]
        end

        it 'adds the fields' do
          expect(document[:client][:driver][:name]).to eq('mongo-ruby-driver|Mongoid|Rails')
          expect(document[:client][:driver][:version]).to eq("#{Mongo::VERSION}|7.1.2|6.0.3")
          expect(document[:client][:platform]).to match(/\AJ?Ruby[^|]+\|\|\z/)
        end
      end
    end
  end
end
