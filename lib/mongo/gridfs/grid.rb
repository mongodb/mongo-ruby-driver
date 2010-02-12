# --
# Copyright (C) 2008-2009 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ++

# GridFS is a specification for storing large objects in MongoDB.
# See the documentation for GridFS::GridStore
#
# @see GridFS::GridStore
#
# @core gridfs
module Mongo
  class Grid
    DEFAULT_ROOT_COLLECTION = 'fs'

    def initialize(db, root_collection=DEFAULT_ROOT_COLLECTION, opts={})
      check_params(db)
      @db     = db
      @files  = @db["#{root_collection}.files"]
      @chunks = @db["#{root_collection}.chunks"]
    end

    def open(filename, mode, opts={})
      file   = GridIO.new(@files, @chunks, filename, mode, opts)
      result = nil
      begin
        if block_given?
          result = yield file
        end
      ensure
        file.close
      end
      result
    end

    private

    def check_params(db)
      if !db.is_a?(Mongo::DB)
        raise MongoArgumentError, "db must be an instance of Mongo::DB."
      end
    end
  end
end
