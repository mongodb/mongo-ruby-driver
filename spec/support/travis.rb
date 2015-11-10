# start up on the travis ci environment.
if (ENV['CI'] == 'travis')
  starting = true
  client = Mongo::Client.new(['127.0.0.1:27017'])
  while starting
    begin
      client.command(Mongo::Server::Monitor::Connection::ISMASTER)
      break
    rescue Mongo::Error::OperationFailure => e
      sleep(2)
      client.cluster.scan!
    end
  end
end
