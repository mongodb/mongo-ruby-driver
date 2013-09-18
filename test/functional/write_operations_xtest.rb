# Copyright (C) 2013 MongoDB, Inc.
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

require 'rbconfig'
require 'test_helper'
require 'benchmark'
require 'json'

MAX_BSON_SIZE = 16 * 1024 * 1024
TWEET_JSON = %q(
    { "text" : "Apple's New Multitouch IPod Nano - Associated Content http://bit.ly/aoICLJ", "in_reply_to_status_id" : null, "retweet_count" : null, "contributors" : null, "created_at" : "Thu Sep 02 18:11:31 +0000 2010", "geo" : null, "source" : "<a href=\"http://twitterfeed.com\" rel=\"nofollow\">twitterfeed</a>", "coordinates" : null, "in_reply_to_screen_name" : null, "truncated" : false, "entities" : { "user_mentions" : [], "urls" : [ { "indices" : [ 54, 74 ], "url" : "http://bit.ly/aoICLJ", "expanded_url" : null } ], "hashtags" : [] }, "retweeted" : false, "place" : null, "user" : { "friends_count" : 385, "profile_sidebar_fill_color" : "DDEEF6", "location" : "Los Angeles, CA", "verified" : false, "follow_request_sent" : null, "favourites_count" : 0, "profile_sidebar_border_color" : "C0DEED", "profile_image_url" : "http://a2.twimg.com/profile_images/1069735022/ipod_004_normal.png", "geo_enabled" : false, "created_at" : "Sun Jul 11 10:44:52 +0000 2010", "description" : "All About iPod and more...", "time_zone" : null, "url" : null, "screen_name" : "iPodMusicPlayer", "notifications" : null, "profile_background_color" : "C0DEED", "listed_count" : 10, "lang" : "en", "profile_background_image_url" : "http://a3.twimg.com/profile_background_images/121997947/iPod.jpg", "statuses_count" : 1504, "following" : null, "profile_text_color" : "333333", "protected" : false, "show_all_inline_media" : false, "profile_background_tile" : false, "name" : "iPod Music Player", "contributors_enabled" : false, "profile_link_color" : "0084B4", "followers_count" : 386, "id" : 165366606, "profile_use_background_image" : true, "utc_offset" : null }, "favorited" : false, "in_reply_to_user_id" : null, "id" : 22819405000 }
)
TWEET_COUNT = 51428 # in twitter.bson
TWEET_COUNT_TOO_BIG = 11749
TWEET = JSON.parse(TWEET_JSON)

def twitter_bulk_data
  twitter_file_name = "#{File.dirname(__FILE__)}/twitter.bson"
  twitter_file_size = File.size?(twitter_file_name)
  twitter_file = File.open(twitter_file_name)
  tweet = []
  while !twitter_file.eof? do
    tweet << BSON.read_bson_document(twitter_file)
    if tweet.size % 100 == 0
      STDOUT.print 100*twitter_file.pos/twitter_file_size
      print "\r"
    end
    #if tweet.size == 16000; then puts; p tweet.size; break; end
  end
  puts
  twitter_file.close
  tweet
end

def tweet_too_big_gen
  a = [ TWEET ]
  doc = {:tweet => a }
  bytes = BSON::BSON_CODER.serialize(doc, false, true, MAX_BSON_SIZE)
  (MAX_BSON_SIZE/bytes.size).times { a << TWEET.dup }
  begin
    a << TWEET.dup
    bytes = BSON::BSON_CODER.serialize(doc, false, true, MAX_BSON_SIZE) unless a.size < TWEET_COUNT_TOO_BIG
  rescue BSON::InvalidDocument => e
    #p e
    break
  end until bytes.size > MAX_BSON_SIZE
  doc
end

module Mongo
  class Collection

    def insert_buffer(collection_name, continue_on_error)
      message = BSON::ByteBuffer.new("", @connection.max_message_size)
      message.put_int(continue_on_error ? 1 : 0)
      BSON::BSON_RUBY.serialize_cstr(message, "#{@db.name}.#{collection_name}")
      message
    end

    def insert_batch(message, documents, write_concern, continue_on_error, errors, collection_name=@name)
      begin
        send_insert_message(message, documents, collection_name, write_concern)
      rescue OperationFailure => ex
        raise ex unless continue_on_error
        errors << ex
      end
    end

    def send_insert_message(message, documents, collection_name, write_concern)
      instrument(:insert, :database => @db.name, :collection => collection_name, :documents => documents) do
        if Mongo::WriteConcern.gle?(write_concern)
          @connection.send_message_with_gle(Mongo::Constants::OP_INSERT, message, @db.name, nil, write_concern)
        else
          @connection.send_message(Mongo::Constants::OP_INSERT, message)
        end
      end
    end

    # Sends a Mongo::Constants::OP_INSERT message to the database.
    # Takes an array of +documents+, an optional +collection_name+, and a
    # +check_keys+ setting.
    def insert_documents(documents, collection_name=@name, check_keys=true, write_concern={}, flags={})
      continue_on_error = !!flags[:continue_on_error]
      collect_on_error = !!flags[:collect_on_error]
      error_docs = [] # docs with errors on serialization
      errors = [] # for all errors on insertion
      batch_start = 0

      message = insert_buffer(collection_name, continue_on_error)

      documents.each_with_index do |doc, index|
        begin
          serialized_doc = BSON::BSON_CODER.serialize(doc, check_keys, true, @connection.max_bson_size)
        rescue BSON::InvalidDocument, BSON::InvalidKeyName, BSON::InvalidStringEncoding => ex
          raise ex unless collect_on_error
          error_docs << doc
          next
        end

        # Check if the current msg has room for this doc. If not, send current msg and create a new one.
        # GLE is a sep msg with its own header so shouldn't be included in padding with header size.
        total_message_size = Networking::STANDARD_HEADER_SIZE + message.size + serialized_doc.size
        if total_message_size > @connection.max_message_size
          docs_to_insert = documents[batch_start..index] - error_docs
          insert_batch(message, docs_to_insert, write_concern, continue_on_error, errors, collection_name)
          batch_start = index
          message = insert_buffer(collection_name, continue_on_error)
          redo
        else
          message.put_binary(serialized_doc.to_s)
        end
      end

      docs_to_insert = documents[batch_start..-1] - error_docs
      inserted_docs = documents - error_docs
      inserted_ids = inserted_docs.collect {|o| o[:_id] || o['_id']}

      # Avoid insertion if all docs failed serialization and collect_on_error
      if error_docs.empty? || !docs_to_insert.empty?
        insert_batch(message, docs_to_insert, write_concern, continue_on_error, errors, collection_name)
        # insert_batch collects errors if w > 0 and continue_on_error is true,
        # so raise the error here, as this is the last or only msg sent
        raise errors.last unless errors.empty?
      end

      collect_on_error ? [inserted_ids, error_docs] : inserted_ids
    end

  end
end

class TestCollection < Test::Unit::TestCase
  @@client ||= standard_connection(:op_timeout => 10)
  @@db = @@client.db(MONGO_TEST_DB)
  @@test = @@db.collection("test")
  @@tweet = BSON.serialize(TWEET)
  #puts "single tweet size: #{@@tweet.size}"
  @@tweet_batch_huge = TWEET_COUNT.times.collect{ TWEET.dup } #twitter_bulk_data
  #puts "user bulk test tweet count: #{@@tweet_batch_huge.size}, estimated size: #{@@tweet.size * @@tweet_batch_huge.size}"
  @@tweet_too_big = tweet_too_big_gen
  #puts "@@tweet_too_big[:tweet].size: #{@@tweet_too_big[:tweet].size}"
  @@tweet_batch_big = @@tweet_too_big[:tweet].drop(1).collect{|doc| doc.dup}
  @@write_ops = [:insert, :update, :delete]
  @@write_batch_size = nil

  def set_max_wire_version(n)
    @@db.connection.instance_variable_set(:@max_wire_version, n) # SERVER-9038 imcomplete so != @@db.connection.check_is_master(@@db.connection.host_port)['maxWireVersion']
  end

  def setup
    @@test.remove
    set_max_wire_version(0)
  end

  def benchmark
    test_name = caller(1, 1)[0][/`(.*)'/, 1]
    GC.start
    GC.disable
    GC.stat
    bm = Benchmark.measure do
      yield
    end
    GC.stat
    GC.enable
    @@test.remove
    puts "#{test_name}: #{'%.2f' % bm.utime}"
  end

  def test_benchmark_op_insert_huge_single_w_0
    benchmark do
      @@tweet_batch_huge.each do |tweet|
        @@test.insert_documents([tweet], 'test', false, {:w => 0} ) # @@test.insert(tweet, :w => 0)
      end
    end
  end

  def test_benchmark_op_insert_huge_single_w_1
    benchmark do
      @@tweet_batch_huge.each do |tweet|
        @@test.insert_documents([tweet], 'test', false, {:w => 1}) # @@test.insert(tweet)
      end
    end
  end

  def test_benchmark_op_insert_batch_huge
    benchmark do
      @response = @@test.insert_documents(@@tweet_batch_huge, 'test', false, {:w => 1}) # @@test.insert(@@tweet_batch_huge)
    end
  end

  def test_benchmark_op_insert_batch_big
    benchmark do
      @response = @@test.insert_documents(@@tweet_batch_big, 'test', false, {:w => 1}) # @@test.insert(@@tweet_batch_big)
    end
    benchmark do
      @response = @@test.insert_documents(@@tweet_batch_big, 'test', false, {:w => 1}) # @@test.insert(@@tweet_batch_big)
    end
  end

  def test_op_insert_too_big
    assert_raise BSON::InvalidDocument do
      @@test.insert([@@tweet_too_big])
    end
  end

  if @@version >= "2.5.2"
    def test_benchmark_write_command_insert_batch_huge
      set_max_wire_version(2)
      benchmark do
        @response = @@test.insert(@@tweet_batch_huge)
      end
    end

    def test_benchmark_write_command_insert_batch_big
      set_max_wire_version(2)
      benchmark do
        @response = @@test.insert(@@tweet_batch_big)
      end
      benchmark do
        @response = @@test.insert(@@tweet_batch_big)
      end
    end

    def test_write_command_insert_batch_too_big
      set_max_wire_version(2)
      assert_raise BSON::InvalidDocument do
        @@test.insert(@@tweet_too_big)
      end
    end
  end

end
