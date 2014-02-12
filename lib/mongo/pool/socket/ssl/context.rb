# Copyright (C) 2009-2014 MongoDB, Inc.
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
  class Pool
    module Socket
      class SSL

        # Factory module for creating SSL context objects.
        #
        # @since 3.0.0
        module Context

          class << self

            # Create the new SSL context based off the provided options.
            #
            # @example Create the SSL context.
            #   Context.create(ssl_cert: '/path/to/file')
            #
            # @param [ Hash ] options The SSL options.
            #
            # @return [ OpenSSL::SSL::SSLContext ] The created context.
            #
            # @since 3.0.0
            def create(opts = {})
              context = OpenSSL::SSL::SSLContext.new

              # Client SSL certificate.
              if opts[:ssl_cert]
                context.cert = OpenSSL::X509::Certificate.new(File.open(opts[:ssl_cert]))
              end

              # Client private key file (optional if included in cert).
              if opts[:ssl_key]
                context.key = OpenSSL::PKey::RSA.new(File.open(opts[:ssl_key]))
              end

              # Peer certificate validation.
              if opts[:ssl_verify] || opts[:ssl_ca_cert]
                context.ca_file     = opts[:ssl_ca_cert]
                context.verify_mode = OpenSSL::SSL::VERIFY_PEER
              end

              context
            end
          end
        end
      end
    end
  end
end
