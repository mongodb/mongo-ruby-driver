# frozen_string_literal: true
# encoding: utf-8

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

      # A MongoDB createindex operation sent as a command message.
      #
      # @api private
      #
      # @since 2.5.2
      class Command
        include Specifiable
        include Executable
        include Limited
        include WriteConcernSupported

        private

        def selector(connection)
          indexes.each do |index|
            if index[:collation] && !connection.features.collation_enabled?
              raise Error::UnsupportedCollation
            end
          end

          {
            createIndexes: coll_name,
            indexes: indexes,
          }.tap do |selector|
            if commit_quorum = spec[:commit_quorum]
              unless connection.features.commit_quorum_enabled?
                raise Error::UnsupportedOption.commit_quorum_error
              end
              selector[:commitQuorum] = commit_quorum
            end
          end
        end

        def message(connection)
          Protocol::Query.new(db_name, Database::COMMAND, command(connection), options(connection))
        end
      end
    end
  end
end
