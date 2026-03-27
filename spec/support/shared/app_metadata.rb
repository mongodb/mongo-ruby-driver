# frozen_string_literal: true

def target_arch
  @target_arch ||= begin
    uname = `uname -a`.strip
    case uname
    when /aarch/ then 'aarch64'
    when /x86/   then 'x86_64'
    when /arm/   then 'arm64'
    else raise "unrecognized architecture: #{uname.inspect}"
    end
  end
end

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
      skip "Linux required, we have #{RbConfig::CONFIG['host_os']}" unless SpecConfig.instance.linux?
    end

    it 'includes operating system information' do
      expect(document[:client][:os][:type]).to eq('linux')
      if BSON::Environment.jruby? || RUBY_VERSION >= '3.0'
        expect(document[:client][:os][:name]).to eq('linux')
      else
        # Ruby 2.7.2 and earlier use linux-gnu.
        # Ruby 2.7.3 uses linux.
        %w[linux linux-gnu].should include(document[:client][:os][:name])
      end
      expect(document[:client][:os][:architecture]).to eq(target_arch)
    end
  end

  context 'macos' do
    before(:all) do
      skip "MacOS required, we have #{RbConfig::CONFIG['host_os']}" unless SpecConfig.instance.macos?
    end

    it 'includes operating system information' do
      expect(document[:client][:os][:type]).to eq('darwin')
      if BSON::Environment.jruby?
        expect(document[:client][:os][:name]).to eq('darwin')
      else
        document[:client][:os][:name].should =~ /darwin\d+/
      end
      expect(document[:client][:os][:architecture]).to eq(target_arch)
    end
  end

  context 'mri' do
    require_mri

    it 'includes Ruby version' do
      document[:client][:platform].should start_with("Ruby #{RUBY_VERSION}")
    end

    context 'when custom platform is specified' do
      let(:app_metadata) do
        described_class.new(platform: 'foowidgets')
      end

      it 'starts with custom platform' do
        document[:client][:platform].should start_with("foowidgets, Ruby #{RUBY_VERSION}")
      end
    end
  end

  context 'jruby' do
    require_jruby

    it 'includes JRuby and Ruby compatibility versions' do
      document[:client][:platform].should start_with("JRuby #{JRUBY_VERSION}, like Ruby #{RUBY_VERSION}")
    end

    context 'when custom platform is specified' do
      let(:app_metadata) do
        described_class.new(platform: 'foowidgets')
      end

      it 'starts with custom platform' do
        document[:client][:platform].should start_with("foowidgets, JRuby #{JRUBY_VERSION}")
      end
    end
  end

  context 'when wrapping libraries are specified' do
    let(:app_metadata) do
      described_class.new(wrapping_libraries: wrapping_libraries)
    end

    context 'one' do
      let(:wrapping_libraries) { [ wrapping_library ] }

      context 'no fields' do
        let(:wrapping_library) do
          {}
        end

        it 'adds empty strings' do
          expect(document[:client][:driver][:name]).to eq('mongo-ruby-driver|')
          expect(document[:client][:driver][:version]).to eq("#{Mongo::VERSION}|")
          document[:client][:platform].should =~ /\AJ?Ruby[^|]+\|\z/
        end
      end

      context 'some fields' do
        let(:wrapping_library) do
          { name: 'Mongoid' }
        end

        it 'adds the fields' do
          expect(document[:client][:driver][:name]).to eq('mongo-ruby-driver|Mongoid')
          expect(document[:client][:driver][:version]).to eq("#{Mongo::VERSION}|")
          document[:client][:platform].should =~ /\AJ?Ruby[^|]+\|\z/
        end
      end

      context 'all fields' do
        let(:wrapping_library) do
          { name: 'Mongoid', version: '7.1.2', platform: 'OS9000' }
        end

        it 'adds the fields' do
          expect(document[:client][:driver][:name]).to eq('mongo-ruby-driver|Mongoid')
          expect(document[:client][:driver][:version]).to eq("#{Mongo::VERSION}|7.1.2")
          document[:client][:platform].should =~ /\AJ?Ruby[^|]+\|OS9000\z/
        end
      end
    end

    context 'two' do
      context 'some fields' do
        let(:wrapping_libraries) do
          [
            { name: 'Mongoid', version: '42' },
            # All libraries should be specifying their versions, in theory,
            # but test not specifying a version.
            { version: '4.0', platform: 'OS9000' },
          ]
        end

        it 'adds the fields' do
          expect(document[:client][:driver][:name]).to eq('mongo-ruby-driver|Mongoid|')
          expect(document[:client][:driver][:version]).to eq("#{Mongo::VERSION}|42|4.0")
          document[:client][:platform].should =~ /\AJ?Ruby[^|]+\|\|OS9000\z/
        end
      end

      context 'a realistic Mongoid & Rails wrapping' do
        let(:wrapping_libraries) do
          [
            { name: 'Mongoid', version: '7.1.2' },
            { name: 'Rails', version: '6.0.3' },
          ]
        end

        it 'adds the fields' do
          expect(document[:client][:driver][:name]).to eq('mongo-ruby-driver|Mongoid|Rails')
          expect(document[:client][:driver][:version]).to eq("#{Mongo::VERSION}|7.1.2|6.0.3")
          document[:client][:platform].should =~ /\AJ?Ruby[^|]+\|\|\z/
        end
      end
    end
  end
end
