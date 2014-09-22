# Copyright (C) 2014 MongoDB, Inc.
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

require 'mongo'

begin
  if RUBY_PLATFORM =~ /java/
    require 'mongo_kerberos/sasl_java'
  else
    require 'mongo_kerberos/sasl_c'
    require "csasl/csasl"
  end
end

module Mongo
  module Authentication

    private

    # Handles issuing authentication commands for the GSSAPI auth mechanism.
    #
    # @param auth [Hash] The authentication credentials to be used.
    # @param opts [Hash] Hash of optional settings and configuration values.
    #
    # @private
    def issue_gssapi(auth, opts={})
      Mongo::Sasl::GSSAPI.authenticate(auth[:username], self, opts[:socket], auth[:extra] || {})
    end
  end
end
