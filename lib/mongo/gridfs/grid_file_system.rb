# --
# Copyright (C) 2008-2010 10gen Inc.
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

module Mongo

  # WARNING: This class is part of a new, experimental GridFS API. Subject to change.
  class GridFileSystem < Grid

    def initialize(db, fs_name=Grid::DEFAULT_FS_NAME)
      raise MongoArgumentError, "db must be a Mongo::DB." unless db.is_a?(Mongo::DB)

      @db      = db
      @files   = @db["#{fs_name}.files"]
      @chunks  = @db["#{fs_name}.chunks"]
      @fs_name = fs_name

      @files.create_index([['filename', 1], ['uploadDate', -1]])
      @default_query_opts = {:sort => [['filename', 1], ['uploadDate', -1]], :limit => 1}
    end

    def open(filename, mode, opts={})
      opts.merge!(default_grid_io_opts(filename))
      file   = GridIO.new(@files, @chunks, filename, mode, opts)
      return file unless block_given?
      result = nil
      begin
        result = yield file
      ensure
        file.close
      end
      result
    end

    def delete(filename)
      files = @files.find({'filename' => filename}, :fields => ['_id'])
      files.each do |file|
        @files.remove({'_id' => file['_id']})
        @chunks.remove({'files_id' => file['_id']})
      end
    end
    alias_method :unlink, :delete

    def remove_previous_versions
      ids = @files.find({'filename' => filename}, :sort => [['filename', 1]])
    end

    private

    def default_grid_io_opts(filename=nil)
      {:fs_name => @fs_name, :query => {'filename' => filename}, :query_opts => @default_query_opts}
    end
  end
end
