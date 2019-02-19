# Copyright (C) 2014-2019 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'resolv'

module Mongo
  module SRV

    # The SRV::Resolver class encapsulates the necessary behavior for querying SRV records.
    #
    # @api private
    class Resolver
      include Loggable

      RECORD_PREFIX = '_mongodb._tcp.'.freeze

      def initialize(raise_on_invalid = false)
        @raise_on_invalid = raise_on_invalid
        @resolver = Resolv::DNS.new
      end

      def get_records(hostname)
        query_name = RECORD_PREFIX + hostname
        records = @resolver.getresources(query_name, Resolv::DNS::Resource::IN::SRV).reduce(SRV::Records.new(hostname)) do |records, record|
          begin
            records.add_record(record)
          rescue Error::MismatchedDomain => e
            if @raise_on_invalid
              raise
            else
              log_warn(e.message)
            end
          end

          records
        end

        if records.empty?
          if @raise_on_invalid
            raise Error::NoSRVRecords.new(URI::SRVProtocol::NO_SRV_RECORDS % hostname)
          else
            log_warn(URI::SRVProtocol::NO_SRV_RECORDS % hostname)
          end
        end

        records
      end

      def get_txt_opts(host)
        @resolver.getresources(host, Resolv::DNS::Resource::IN::TXT)
      end
    end
  end
end
