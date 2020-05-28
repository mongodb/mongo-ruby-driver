shared_examples 'app metadata document' do
  let(:app_metadata) do
    described_class.new({})
  end

  it 'includes Ruby driver identification' do
    document[:client][:driver][:name].should == 'mongo-ruby-driver'
    document[:client][:driver][:version].should == Mongo::VERSION
  end

  it 'includes operating system information' do
    document[:client][:os][:type].should == 'linux'
    if BSON::Environment.jruby?
      document[:client][:os][:name].should == 'linux'
    else
      document[:client][:os][:name].should == 'linux-gnu'
    end
    document[:client][:os][:architecture].should == 'x86_64'
  end

  context 'mri' do
    only_mri

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
end
