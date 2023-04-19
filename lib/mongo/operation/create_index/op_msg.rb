# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2018-2020 MongoDB Inc.
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
  module Operation
    class CreateIndex

      # A MongoDB createindex operation sent as an op message.
      #
      # @api private
      #
      # @since 2.5.2
      class OpMsg < OpMsgBase
        include ExecutableTransactionLabel

        private

        def selector(connection)
          {
            createIndexes: coll_name,
            indexes: indexes,
            comment: spec[:comment],
          }.compact.tap do |selector|
            if commit_quorum = spec[:commit_quorum]
              # While server versions 3.4 and newer generally perform option
              # validation, there was a bug on server versions 4.2.0 - 4.2.5 where
              # the server would accept the commitQuorum option and use it internally
              # (see SERVER-47193). As a result, the drivers specifications require
              # drivers to perform validation and raise an error when the commitQuorum
              # option is passed to servers that don't support it.
              unless connection.features.commit_quorum_enabled?
                raise Error::UnsupportedOption.commit_quorum_error
              end
              selector[:commitQuorum] = commit_quorum
            end
          end
        end
      end
    end
  end
end
