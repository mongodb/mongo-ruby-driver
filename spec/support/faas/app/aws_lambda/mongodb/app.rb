# frozen_string_literal: true

require 'mongo'
require 'faas_test/runner'

# Helpful resources:
# https://dev.to/aws-builders/building-aws-ruby-lambdas-that-require-gems-with-native-extension-17h

# Parameters
# ----------
# event: Hash, required
#     API Gateway Lambda Proxy Input Format
#     Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format
#
# context: object, required
#     Lambda Context runtime methods and attributes
#     Context doc: https://docs.aws.amazon.com/lambda/latest/dg/ruby-context.html
def lambda_handler(event:, context:)
  client = Mongo::Client.new(ENV['MONGODB_URI'])
  runner = FaaSTest::Runner.new(client)

  results = runner.run

  {
    statusCode: 200,
    body: results.to_json
  }
end
