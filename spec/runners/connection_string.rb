
def define_connection_string_spec_tests(test_paths, spec_cls = Mongo::ConnectionString::Spec, &block)

  clean_slate_for_all

  test_paths.each do |path|

    spec = spec_cls.new(path)

    context(spec.description) do

      #include Mongo::ConnectionString

      spec.tests.each_with_index do |test, index|
        context "when a #{test.description} is provided" do
          if test.description.downcase.include?("gssapi")
            require_mongo_kerberos
          end

          context 'when the uri is invalid', unless: test.valid? do

            it 'raises an error' do
              expect{
                test.uri
              }.to raise_exception(Mongo::Error::InvalidURI)
            end
          end

          context 'when the uri should warn', if: test.warn? do

            before do
              expect(Mongo::Logger.logger).to receive(:warn)
            end

            it 'warns' do
              expect(test.client).to be_a(Mongo::Client)
            end
          end

          context 'when the uri is valid', if: test.valid? do

            it 'does not raise an exception' do
              expect(test.uri).to be_a(Mongo::URI)
            end

            it 'creates a client with the correct hosts' do
              expect(test.client).to have_hosts(test, test.hosts)
            end

            it 'creates a client with the correct authentication properties' do
              expect(test.client).to match_auth(test)
            end

            it 'creates a client with the correct options' do
              expect(test.client).to match_options(test)
            end
          end
        end
      end
    end
  end
end
