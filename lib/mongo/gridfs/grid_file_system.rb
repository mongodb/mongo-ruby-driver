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

    def initialize(db, fs_name=DEFAULT_FS_NAME)
      super

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

    def put(data, filename, opts={})
      opts.merge!(default_grid_io_opts(filename))
      file = GridIO.new(@files, @chunks, filename, 'w', opts)
      file.write(data)
      file.close
      file.files_id
    end

    def get(filename, opts={})
      opts.merge!(default_grid_io_opts(filename))
      GridIO.new(@files, @chunks, filename, 'r', opts)
    end

    def delete(filename, opts={})
      ids = @files.find({'filename' => filename}, ['_id'])
      ids.each do |id|
        @files.remove({'_id' => id})
        @chunks.remove('files_id' => id)
      end
    end

    private

    def default_grid_io_opts(filename=nil)
      {:fs_name => @fs_name, :query => {'filename' => filename}, :query_opts => @default_query_opts}
    end
  end
end
