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

  module Queryable

    # Get the distinct values for a specified field across a single
    # collection.
    # Note that if a @selector is defined, it will be used in the analysis.
    # If a limit has been specified, an error is raised.
    #
    # @param key [ Symbol, String ] The field to collect distinct values from.
    #
    # @return [ Hash ] A doc with an array of the distinct values and query plan.
    def distinct(key)
      raise Exception, 'Skip cannot be combined with this method' if skip
      raise Exception, 'Limit other than 1 has been specified' if limit && limit > 1
      @collection.distinct(self, key)
    end

    # Get the explain plan for the query.
    #
    # @return [ Hash ] A single document with the explain plan.
    def explain
      explain_limit = limit || 0
      opts = @opts.merge(:limit => -explain_limit.abs, :explain => true)
      @collection.explain(CollectionView.new(@collection, @selector, opts))
    end

    # Fetches a single document matching the query spec.
    #
    # @return [ Hash ] The first document matching the query spec.
    #
    # @since 3.0.0
    def fetch_one
      limit(1).to_a.first
    end
  end
end
