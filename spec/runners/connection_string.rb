
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

            if test.read_concern_expectation
              # Tests do not specify a read concern in the input and expect
              # the read concern to be {); our non-specified read concern is nil.
              # (But if a test used nil for the expectation, we wouldn't assert
              # read concern at all.)
              if test.read_concern_expectation == {}
                it 'creates a client with no read concern' do
                  actual = Utils.camelize_hash(test.client.options[:read_concern])
                  expect(actual).to be nil
                end
              else

                it 'creates a client with the correct read concern' do
                  actual = Utils.camelize_hash(test.client.options[:read_concern])
                  expect(actual).to eq(test.read_concern_expectation)
                end
              end
            end

            if test.write_concern_expectation
              let(:actual_write_concern) do
                Utils.camelize_hash(test.client.options[:write_concern])
              end

              let(:expected_write_concern) do
                test.write_concern_expectation.dup.tap do |expected|
                  # Spec tests have expectations on the "driver API" which is
                  # different from what is being sent to the server. In Ruby
                  # the "driver API" matches what we send to the server, thus
                  # these expectations are rather awkward to work with.
                  # Convert them all to expected server fields.
                  j = expected.delete('journal')
                  unless j.nil?
                    expected['j'] = j
                  end
                  wtimeout = expected.delete('wtimeoutMS')
                  unless wtimeout.nil?
                    expected['wtimeout'] = wtimeout
                  end
                end
              end

              if test.write_concern_expectation == {}

                it 'creates a client with no write concern' do
                  expect(actual_write_concern).to be nil
                end
              else
                it 'creates a client with the correct write concern' do
                  expect(actual_write_concern).to eq(expected_write_concern)
                end
              end
            end
          end
        end
      end
    end
  end
end
