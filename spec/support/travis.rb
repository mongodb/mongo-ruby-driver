SSL_OPTIONS = {}

# start up on the travis ci environment.
if (ENV['CI'] == 'travis')
  if ENV['SSL'] == 'enabled'
    SSL_OPTIONS[:ssl] = true
    SSL_OPTIONS[:ssl_cert] = File.read('/tmp/mongodb.pem')
  end
  starting = true
  client = Mongo::Client.new(['127.0.0.1:27017'])
  while starting
    begin
      client.command(Mongo::Server::Monitor::STATUS)
      break
    rescue Mongo::Error::OperationFailure => e
      sleep(2)
      client.cluster.scan!
    end
  end
end
