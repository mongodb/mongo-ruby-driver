# Copyright (C) 2014-2019 MongoDB, Inc.
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
  module SRV

    class Monitor
      include Loggable

      MIN_RESCAN_FREQUENCY = 60

      attr_reader :options

      def initialize(cluster, resolver, srv_records, options = nil)
        @options = options || {}
        @cluster = cluster
        @resolver = resolver
        @records = srv_records
        @no_records_found = false
      end

      def start_monitor!
        @thread = Thread.new do
          loop do
            sleep(rescan_frequency)
            scan!
          end
        end

        ObjectSpace.define_finalizer(self, self.class.finalize(@thread))
      end

      def scan!
        @old_hosts = @records.hosts

        begin
          @records = @resolver.get_records(@records.hostname)
        rescue Resolv::ResolvTimeout => e
          log_warn("Timed out trying to resolve hostname #{@records.hostname}")
          return
        rescue Resolv::ResolvError => e
          log_warn("Unable to resolve hostname #{@records.hostname}")
          return
        end

        if @records.empty?
          @no_records_found = true
          return
        end

        @no_records_found = false

        (@old_hosts - @records.hosts).each do |host|
          @cluster.remove(host)
        end

        (@records.hosts - @old_hosts).each do |host|
          @cluster.add(host)
        end
      end

      def self.finalize(thread)
        Proc.new do
          thread.kill
        end
      end

      private

      def rescan_frequency
        if @no_records_found
          Server:: Monitor::HEARTBEAT_FREQUENCY
        elsif @records.min_ttl.nil?
          MIN_RESCAN_FREQUENCY
        else
          [@records.min_ttl, MIN_RESCAN_FREQUENCY].max
        end
      end
    end
  end
end
