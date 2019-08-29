MECHANISMS = {
  'MONGODB-CR' => :mongodb_cr,
  'MONGODB-X509' => :mongodb_x509,
  'PLAIN' => :plain,
  'SCRAM-SHA-1' => :scram,
  'SCRAM-SHA-256' => :scram256,
  'GSSAPI' => :gssapi
}

RSpec::Matchers.define :match_credential do |test|
  def match_user?(client, credential)
    client.options[:user] == credential['username']
  end

  def match_password?(client, credential)
    client.options[:password] == credential['password']
  end

  def match_auth_source?(client, credential)
    expected_auth_source = credential['source'] == '$external' ? :external : credential['source']
    client.options[:auth_source] == expected_auth_source
  end

  def match_auth_mech?(client, credential)
    if credential['mechanism'].nil?
      expected_mechanism = nil
    else
      expected_mechanism = MECHANISMS[credential['mechanism']]
    end

    client.options[:auth_mech] == expected_mechanism
  end

  def match_auth_mech_properties?(client, credential)
    if credential['mechanism_properties'].nil?
      return client.options[:auth_mech_properties].nil?
    end

    same_keys =
      client.options[:auth_mech_properties] == credential['mechanism_propertis'].keys.map(&:downcase)
    
    same_values =
      credential['mechanism_properties'].all? do |prop, prop_val|
        client.options[:auth_mech_properties][prop.downcase] == prop_val
      end

    same_keys && same_values
  end

  def blank_credentials?(client)
    %i(auth_mech auth_mech_properties auth_source password user).all? do |key|
      client.options[key].nil?
    end
  end

  match do |client|
    return blank_credentials?(client) if test.credential.nil?

    match_user?(client, test.credential) &&
      match_password?(client, test.credential) &&
      match_auth_source?(client, test.credential) &&
      match_auth_mech?(client, test.credential) &&
      match_auth_mech_properties?(client, test.credential)
  end

  failure_message do |client|
    "Expected that client initialized with URI #{test.uri_string} would match credentials: #{test.credential}"
  end
end

module Mongo
  module Auth
    class Spec

      attr_reader :description
      attr_reader :tests

      def initialize(file)
        file = File.new(file)
        @spec = YAML.load(ERB.new(file.read).result)
        file.close
        @description = File.basename(file)

      end

      def tests
        @tests ||= @spec['tests'].collect do |spec|
          Test.new(spec)
        end
      end
    end

    class Test
      attr_reader :description
      attr_reader :uri_string

      def initialize(spec)
        @spec = spec
        @description = @spec['description']
        @uri_string = @spec['uri']
      end

      def valid?
        @spec['valid']
      end

      def credential
        @spec['credential']
      end

      def client
        @client ||= ClientRegistry.instance.new_local_client(@spec['uri'], monitoring_io: false)
      end
    end
  end
end
