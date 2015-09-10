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

require 'mongo/operation/write/update/result'

module Mongo
  module Operation
    module Write

      # A MongoDB update operation.
      #
      # @note If the server version is >= 2.5.5, a write command operation
      #   will be created and sent instead.
      #
      # @example Create the update operation.
      #   Write::Update.new({
      #     :update =>
      #       {
      #         :q => { :foo => 1 },
      #         :u => { :$set => { :bar => 1 }},
      #         :multi  => true,
      #         :upsert => false
      #       },
      #     :db_name => 'test',
      #     :coll_name => 'test_coll',
      #     :write_concern => write_concern
      #   })
      #
      # Initialization:
      #   param [ Hash ] spec The specifications for the update.
      #
      #   option spec :update [ Hash ] The update document.
      #   option spec :db_name [ String ] The name of the database on which
      #     the query should be run.
      #   option spec :coll_name [ String ] The name of the collection on which
      #     the query should be run.
      #   option spec :write_concern [ Mongo::WriteConcern ] The write concern.
      #   option spec :options [ Hash ] Options for the command, if it ends up being a
      #     write command.
      #
      # @since 2.0.0
      class Update
        include GLE
        include WriteCommandEnabled
        include Specifiable

        private

        def write_command_op
          s = spec.merge(:updates => [ update ])
          s.delete(:update)
          Command::Update.new(s)
        end

        def message
          flags = []
          flags << :multi_update if update[Operation::MULTI]
          flags << :upsert if update[Operation::UPSERT]
          Protocol::Update.new(
            db_name,
            coll_name,
            update[Operation::Q],
            update[Operation::U],
            flags.empty? ? {} : { flags: flags }
          )
        end
      end
    end
  end
end
