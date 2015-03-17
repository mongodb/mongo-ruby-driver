# Copyright (C) 2014-2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  module MongoOrchestration
    module Operation

      class ClientHosts

        def initialize(client, mo, config)
          @client = client
          @mo = mo
          @primary = config['primary']
          @secondaries = config['secondaries']
        end

        def run
          check_primary
          check_secondaries
        end

        private

        def client_primary
          @client.cluster.servers.find do |server|
            server.primary?
          end
        end

        def client_secondaries
          @client.cluster.servers.select do |server|
            server if server.secondary?
          end
        end

        def check_primary
          compare([ @primary ], [ client_primary ])
        end

        def check_secondaries
          compare(@secondaries, client_secondaries)
        end

        def client_pairs(servers)
          servers.collect do |s|
            [ s.address.host, s.address.port.to_s ]
          end
        end

        def mo_pairs(servers)
          servers.collect do |server|
            @mo.get_host_port(server)
          end
        end

        def compare(mo_servers, client_servers)
          mo_pairs(mo_servers) == 
            client_pairs(client_servers)
        end
      end
    end
  end
end
