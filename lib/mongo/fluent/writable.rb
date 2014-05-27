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

  # These methods are writes.
  module Writable

    # Removes all the documents matching the query spec.
    # If a limit other than 1 has been specified, an error is raised.
    #
    # @return [ RemoveResult ] Document specifying success or failure of
    #   the operation.
    #
    # @since 3.0.0
    def remove
      validate_write!
      validate_no_limit!

      spec = { :deletes       => [{ :q     => selector,
                                    :limit => limit }],
               :db_name       => collection.database.name,
               :coll_name     => collection.name,
               :write_concern => write_concern,
      }
      Operation::Write::Delete.new(spec).execute(collection.client)
    end

    # Removes a single document matching the query spec.
    # If a limit has been specified, it is ignored.
    #
    # @return [ RemoveResult ] Document specifying success or failure of
    #   the operation.
    #
    # @since 3.0.0
    def remove_one
      validate_write!

      spec = { :deletes       => [{ :q     => selector,
                                    :limit => 1 }],
               :db_name       => collection.database.name,
               :coll_name     => collection.name,
               :write_concern => write_concern,
      }
      Operation::Write::Delete.new(spec).execute(collection.client)
    end

    # Replaces a single document matching the query spec with the
    # provided replacement.
    # If a limit has been specified, it is ignored.
    #
    # @return [ ReplaceResult ] Document specifying success or failure of
    #   the operation.
    #
    # @since 3.0.0
    def replace_one(replacement)
      validate_write!
      validate_replacement!(replacement)

      spec = { :updates       => [{ :q      => selector,
                                    :u      => replacement,
                                    :multi  => false,
                                    :upsert => upsert }],
               :db_name       => collection.database.name,
               :coll_name     => collection.name,
               :write_concern => write_concern,
      }
      Mongo::Operation::Write::Update.new(spec).execute(collection.client)
    end

    # Updates all the documents matching the query spec by applying
    # the specified update.
    # If a limit other than 1 has been specified, an error is raised.
    #
    # @return [ UpdateResult ] Document specifying success or failure of
    #   the operation.
    #
    # @since 3.0.0
    def update(update)
      validate_no_limit!
      validate_write!
      validate_update!(update)

      spec = { :updates       => [{ :q      => selector,
                                    :u      => update,
                                    :multi  => true,
                                    :upsert => upsert }],
               :db_name       => collection.database.name,
               :coll_name     => collection.name,
               :write_concern => write_concern,
      }
      Mongo::Operation::Write::Update.new(spec).execute(collection.client)
    end

    # Updates a single document matching the query spec by applying the
    # specified update.
    # If a limit has been specified, it is ignored.
    #
    # @return [ UpdateResult ] Document specifying success or failure of
    #   the operation.
    #
    # @since 3.0.0
    def update_one(update)
      validate_write!
      validate_update!(update)

      spec = { :updates       => [{ :q      => selector,
                                    :u      => update,
                                    :multi  => false,
                                    :upsert => upsert }],
               :db_name       => collection.database.name,
               :coll_name     => collection.name,
               :write_concern => write_concern,
      }
      Mongo::Operation::Write::Update.new(spec).execute(collection.client)
    end

    private

    # Verifies that skip and sort have not been specified as they're invalid with
    #   these methods.
    #
    # @raise [ Exception ] If skip or sort have been specified earlier in the chain.
    #
    # @since 3.0.0
    def validate_write!
      # @todo: update with real error
      raise Exception, 'Sort cannot be combined with this method' if sort
      raise Exception, 'Skip cannot be combined with this method' if skip
    end

    # Verifies that limit has not been specified as it's invalid with these methods.
    #
    # @raise [ Exception ] If limit has been specified earlier in the chain.
    #
    # @since 3.0.0
    def validate_no_limit!
      # @todo: update with real error
      raise Exception, 'Limit other than 1 has been specified' if limit && limit > 1
    end
  end
end
