# frozen_string_literal: true
# rubocop:todo all

def target_arch
  @target_arch ||= begin
    uname = `uname -a`.strip
    case uname
    when /aarch/ then "aarch64"
    when /x86/   then "x86_64"
    when /arm/   then "arm64"
    else raise "unrecognized architecture: #{uname.inspect}"
    end
  end
end

shared_examples 'app metadata document' do
  let(:app_metadata) do
    described_class.new({})
  end

  it 'includes Ruby driver identification' do
    document[:client][:driver][:name].should == 'mongo-ruby-driver'
    document[:client][:driver][:version].should == Mongo::VERSION
  end

  context 'linux' do
    before(:all) do
      unless SpecConfig.instance.linux?
        skip "Linux required, we have #{RbConfig::CONFIG['host_os']}"
      end
    end

    it 'includes operating system information' do
      document[:client][:os][:type].should == 'linux'
      if BSON::Environment.jruby? || RUBY_VERSION >= '3.0'
        document[:client][:os][:name].should == 'linux'
      else
        # Ruby 2.7.2 and earlier use linux-gnu.
        # Ruby 2.7.3 uses linux.
        %w(linux linux-gnu).should include(document[:client][:os][:name])
      end
      document[:client][:os][:architecture].should == target_arch
    end
  end

  context 'macos' do
    before(:all) do
      unless SpecConfig.instance.macos?
        skip "MacOS required, we have #{RbConfig::CONFIG['host_os']}"
      end
    end

    it 'includes operating system information' do
      document[:client][:os][:type].should == 'darwin'
      if BSON::Environment.jruby?
        document[:client][:os][:name].should == 'darwin'
      else
        document[:client][:os][:name].should =~ /darwin\d+/
      end
      document[:client][:os][:architecture].should == target_arch
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
      let(:wrapping_libraries) { [wrapping_library] }

      context 'no fields' do
        let(:wrapping_library) do
          {}
        end

        it 'adds empty strings' do
          document[:client][:driver][:name].should == 'mongo-ruby-driver|'
          document[:client][:driver][:version].should == "#{Mongo::VERSION}|"
          document[:client][:platform].should =~ /\AJ?Ruby[^|]+\|\z/
        end
      end

      context 'some fields' do
        let(:wrapping_library) do
          {name: 'Mongoid'}
        end

        it 'adds the fields' do
          document[:client][:driver][:name].should == 'mongo-ruby-driver|Mongoid'
          document[:client][:driver][:version].should == "#{Mongo::VERSION}|"
          document[:client][:platform].should =~ /\AJ?Ruby[^|]+\|\z/
        end
      end

      context 'all fields' do
        let(:wrapping_library) do
          {name: 'Mongoid', version: '7.1.2', platform: 'OS9000'}
        end

        it 'adds the fields' do
          document[:client][:driver][:name].should == 'mongo-ruby-driver|Mongoid'
          document[:client][:driver][:version].should == "#{Mongo::VERSION}|7.1.2"
          document[:client][:platform].should =~ /\AJ?Ruby[^|]+\|OS9000\z/
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
          document[:client][:driver][:name].should == 'mongo-ruby-driver|Mongoid|'
          document[:client][:driver][:version].should == "#{Mongo::VERSION}|42|4.0"
          document[:client][:platform].should =~ /\AJ?Ruby[^|]+\|\|OS9000\z/
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
          document[:client][:driver][:name].should == 'mongo-ruby-driver|Mongoid|Rails'
          document[:client][:driver][:version].should == "#{Mongo::VERSION}|7.1.2|6.0.3"
          document[:client][:platform].should =~ /\AJ?Ruby[^|]+\|\|\z/
        end
      end
    end
  end
end
