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

module Mongo

  # WARNING: This class is part of a new, experimental GridFS API. Subject to change.
  class GridFileSystem < Grid

    def initialize(db, bucket_name=DEFAULT_BUCKET_NAME)
      super

      @files.create_index([['filename', 1], ['uploadDate', -1]])
    end

    def open(filename, mode, opts={})
      file   = GridIO.new(@files, @chunks, filename, mode, true, opts)
      return file unless block_given?
      result = nil
      begin
        result = yield file
      ensure
        file.close
      end
      result
    end

    def put(data, filename)
    end

    def get(id)
    end

    # Deletes all files matching the given criteria.
    def delete(criteria)
    end

  end
end
