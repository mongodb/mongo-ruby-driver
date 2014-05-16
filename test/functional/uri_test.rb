# Copyright (C) 2009-2013 MongoDB, Inc.
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

require 'test_helper'

class URITest < Test::Unit::TestCase
  include Mongo

  def test_uri_without_port
    parser = Mongo::URIParser.new('mongodb://localhost')
    assert_equal 1, parser.nodes.length
    assert_equal 'localhost', parser.nodes[0][0]
    assert_equal 27017, parser.nodes[0][1]
  end

  def test_basic_uri
    parser = Mongo::URIParser.new('mongodb://localhost:27018')
    assert_equal 1, parser.nodes.length
    assert_equal 'localhost', parser.nodes[0][0]
    assert_equal 27018, parser.nodes[0][1]
  end

  def test_ipv6_format
    parser = Mongo::URIParser.new('mongodb://[::1]:27018')
    assert_equal 1, parser.nodes.length
    assert_equal '::1', parser.nodes[0][0]
    assert_equal 27018, parser.nodes[0][1]

    parser = Mongo::URIParser.new('mongodb://[::1]')
    assert_equal 1, parser.nodes.length
    assert_equal '::1', parser.nodes[0][0]
  end

  def test_ipv6_format_multi
    parser = Mongo::URIParser.new('mongodb://[::1]:27017,[::1]:27018')
    assert_equal 2, parser.nodes.length
    assert_equal '::1', parser.nodes[0][0]
    assert_equal 27017, parser.nodes[0][1]
    assert_equal '::1', parser.nodes[1][0]
    assert_equal 27018, parser.nodes[1][1]

    parser = Mongo::URIParser.new('mongodb://[::1]:27017,localhost:27018')
    assert_equal 2, parser.nodes.length
    assert_equal '::1', parser.nodes[0][0]
    assert_equal 27017, parser.nodes[0][1]
    assert_equal 'localhost', parser.nodes[1][0]
    assert_equal 27018, parser.nodes[1][1]

    parser = Mongo::URIParser.new('mongodb://localhost:27017,[::1]:27018')
    assert_equal 2, parser.nodes.length
    assert_equal 'localhost', parser.nodes[0][0]
    assert_equal 27017, parser.nodes[0][1]
    assert_equal '::1', parser.nodes[1][0]
    assert_equal 27018, parser.nodes[1][1]
  end

  def test_multiple_uris
    parser = Mongo::URIParser.new('mongodb://a.example.com:27018,b.example.com')
    assert_equal 2, parser.nodes.length
    assert_equal ['a.example.com', 27018], parser.nodes[0]
    assert_equal ['b.example.com', 27017], parser.nodes[1]
  end

  def test_username_without_password
    parser = Mongo::URIParser.new('mongodb://bob:@localhost?authMechanism=GSSAPI')
    assert_equal "bob", parser.auths.first[:username]
    assert_equal nil, parser.auths.first[:password]

    parser = Mongo::URIParser.new('mongodb://bob@localhost?authMechanism=GSSAPI')
    assert_equal nil, parser.auths.first[:password]

    assert_raise_error MongoArgumentError do
      Mongo::URIParser.new('mongodb://bob:@localhost')
    end

    assert_raise_error MongoArgumentError do
      Mongo::URIParser.new('mongodb://bob@localhost')
    end
  end

  def test_complex_passwords
    parser = Mongo::URIParser.new('mongodb://bob:secret.word@a.example.com:27018/test')
    assert_equal "bob", parser.auths.first[:username]
    assert_equal "secret.word", parser.auths.first[:password]

    parser = Mongo::URIParser.new('mongodb://bob:s-_3#%R.t@a.example.com:27018/test')
    assert_equal "bob", parser.auths.first[:username]
    assert_equal "s-_3#%R.t", parser.auths.first[:password]

    assert_raise_error MongoArgumentError do
      Mongo::URIParser.new('mongodb://doctor:bad:wolf@gallifrey.com:27018/test')
    end

    assert_raise_error MongoArgumentError do
      Mongo::URIParser.new('mongodb://doctor:bow@tie@gallifrey.com:27018/test')
    end
  end

  def test_complex_usernames
    parser = Mongo::URIParser.new('mongodb://s-_3#%R.t:secret.word@a.example.com:27018/test')
    assert_equal "s-_3#%R.t", parser.auths.first[:username]

    assert_raise_error MongoArgumentError do
      Mongo::URIParser.new('mongodb://doc:tor:badwolf@gallifrey.com:27018/test')
    end

    assert_raise_error MongoArgumentError do
      Mongo::URIParser.new('mongodb://d@ctor:bowtie@gallifrey.com:27018/test')
    end
  end

  def test_username_with_encoded_symbol
    parser = Mongo::URIParser.new('mongodb://f%40o:bar@localhost/admin')
    username = parser.auths.first[:username]
    assert_equal 'f@o', username

    parser = Mongo::URIParser.new('mongodb://f%3Ao:bar@localhost/admin')
    username = parser.auths.first[:username]
    assert_equal 'f:o', username
  end

  def test_password_with_encoded_symbol
    parser = Mongo::URIParser.new('mongodb://foo:b%40r@localhost/admin')
    password = parser.auths.first[:password]
    assert_equal 'b@r', password

    parser = Mongo::URIParser.new('mongodb://foo:b%3Ar@localhost/admin')
    password = parser.auths.first[:password]
    assert_equal 'b:r', password
  end

  def test_opts_with_semincolon_separator
    parser = Mongo::URIParser.new('mongodb://localhost:27018?connect=direct;slaveok=true;safe=true')
    assert_equal 'direct', parser.connect
    assert parser.direct?
    assert parser.slaveok
    assert parser.safe
  end

  def test_opts_with_amp_separator
    parser = Mongo::URIParser.new('mongodb://localhost:27018?connect=direct&slaveok=true&safe=true')
    assert_equal 'direct', parser.connect
    assert parser.direct?
    assert parser.slaveok
    assert parser.safe
  end

  def test_opts_with_uri_encoded_stuff
    parser = Mongo::URIParser.new('mongodb://localhost:27018?connect=%64%69%72%65%63%74&slaveok=%74%72%75%65&safe=true')
    assert_equal 'direct', parser.connect
    assert parser.direct?
    assert parser.slaveok
    assert parser.safe
  end

  def test_opts_made_invalid_by_mixed_separators
    assert_raise_error MongoArgumentError, "must not mix URL separators ; and &" do
      Mongo::URIParser.new('mongodb://localhost:27018?replicaset=foo;bar&slaveok=true&safe=true')
    end
  end

  def test_opts_safe
    parser = Mongo::URIParser.new('mongodb://localhost:27018?safe=true;w=2;journal=true;fsync=true;wtimeoutMS=200')
    assert parser.safe
    assert_equal 2, parser.w
    assert parser.fsync
    assert parser.journal
    assert_equal 200, parser.wtimeoutms
  end

  def test_opts_ssl
    parser = Mongo::URIParser.new('mongodb://localhost:27018?ssl=true;w=2;journal=true;fsync=true;wtimeoutMS=200')
    assert parser.ssl
  end

  def test_opts_nonsafe_timeout
    parser = Mongo::URIParser.new('mongodb://localhost:27018?connectTimeoutMS=5500&socketTimeoutMS=500')
    assert_equal 5.5, parser.connecttimeoutms
    assert_equal 0.5, parser.sockettimeoutms
  end

  def test_opts_replica_set
    parser = Mongo::URIParser.new('mongodb://localhost:27018?connect=replicaset;replicaset=foo')
    assert_equal 'foo', parser.replicaset
    assert_equal 'replicaset', parser.connect
    assert parser.replicaset?
  end

  def test_opts_conflicting_replica_set
    assert_raise_error MongoArgumentError, "connect=direct conflicts with setting a replicaset name" do
      Mongo::URIParser.new('mongodb://localhost:27018?connect=direct;replicaset=foo')
    end
  end

  def test_case_insensitivity
    parser = Mongo::URIParser.new('mongodb://localhost:27018?wtimeoutms=500&JOURNAL=true&SaFe=true')
    assert_equal 500, parser.wtimeoutms
    assert_equal true, parser.journal
    assert_equal true, parser.safe
  end

  def test_read_preference_option_primary
    parser = Mongo::URIParser.new("mongodb://localhost:27018?readPreference=primary")
    assert_equal :primary, parser.readpreference
  end

  def test_read_preference_option_primary_preferred
    parser = Mongo::URIParser.new("mongodb://localhost:27018?readPreference=primaryPreferred")
    assert_equal :primary_preferred, parser.readpreference
  end

  def test_read_preference_option_secondary
    parser = Mongo::URIParser.new("mongodb://localhost:27018?readPreference=secondary")
    assert_equal :secondary, parser.readpreference
  end

  def test_read_preference_option_secondary_preferred
    parser = Mongo::URIParser.new("mongodb://localhost:27018?readPreference=secondaryPreferred")
    assert_equal :secondary_preferred, parser.readpreference
  end

  def test_read_preference_option_nearest
    parser = Mongo::URIParser.new("mongodb://localhost:27018?readPreference=nearest")
    assert_equal :nearest, parser.readpreference
  end

  def test_read_preference_option_with_invalid
    assert_raise_error MongoArgumentError  do
      Mongo::URIParser.new("mongodb://localhost:27018?readPreference=invalid")
    end
  end

  def test_read_preference_connection_options
    parser = Mongo::URIParser.new("mongodb://localhost:27018?replicaset=test&readPreference=nearest")
    assert_equal :nearest, parser.connection_options[:read]
  end

  def test_read_preference_connection_options_with_no_replica_set
    parser = Mongo::URIParser.new("mongodb://localhost:27018?readPreference=nearest")
    assert_equal :nearest, parser.connection_options[:read]
  end

  def test_read_preference_connection_options_prefers_preference_over_slaveok
    parser = Mongo::URIParser.new("mongodb://localhost:27018?replicaset=test&readPreference=nearest&slaveok=true")
    assert_equal :nearest, parser.connection_options[:read]
  end

  def test_connection_when_sharded_with_no_options
    parser = Mongo::URIParser.new("mongodb://localhost:27017,localhost:27018")
    client = parser.connection({}, false, true)
    assert_equal [[ "localhost", 27017 ], [ "localhost", 27018 ]], client.seeds
    assert_true client.mongos?
  end

  def test_connection_when_sharded_with_options
    parser = Mongo::URIParser.new("mongodb://localhost:27017,localhost:27018")
    client = parser.connection({ :refresh_interval => 10 }, false, true)
    assert_equal [[ "localhost", 27017 ], [ "localhost", 27018 ]], client.seeds
    assert_equal 10, client.refresh_interval
    assert_true client.mongos?
  end

  def test_connection_when_sharded_with_uri_options
    parser = Mongo::URIParser.new("mongodb://localhost:27017,localhost:27018?readPreference=nearest")
    client = parser.connection({}, false, true)
    assert_equal [[ "localhost", 27017 ], [ "localhost", 27018 ]], client.seeds
    assert_equal :nearest, client.read
    assert_true client.mongos?
  end

  def test_auth_source
    parser = Mongo::URIParser.new("mongodb://user:pass@localhost?authSource=foobar")
    assert_equal 'foobar', parser.authsource
  end

  def test_auth_mechanism
    parser = Mongo::URIParser.new("mongodb://user@localhost?authMechanism=MONGODB-X509")
    assert_equal 'MONGODB-X509', parser.authmechanism

    assert_raise_error MongoArgumentError  do
      Mongo::URIParser.new("mongodb://user@localhost?authMechanism=INVALID")
    end
  end

  def test_sasl_plain
    parser = Mongo::URIParser.new("mongodb://user:pass@localhost?authMechanism=PLAIN")
    assert_equal 'PLAIN', parser.auths.first[:mechanism]
    assert_equal 'user', parser.auths.first[:username]
    assert_equal 'pass', parser.auths.first[:password]
    assert_equal 'admin', parser.auths.first[:source]

    parser = Mongo::URIParser.new("mongodb://foo%2Fbar%40example.net:pass@localhost/some_db?authMechanism=PLAIN")
    assert_equal 'PLAIN', parser.auths.first[:mechanism]
    assert_equal 'foo/bar@example.net', parser.auths.first[:username]
    assert_equal 'pass', parser.auths.first[:password]
    assert_equal 'some_db', parser.auths.first[:source]

    assert_raise_error MongoArgumentError  do
      Mongo::URIParser.new("mongodb://user@localhost/some_db?authMechanism=PLAIN")
    end
  end

  def test_gssapi
    uri = "mongodb://foo%2Fbar%40example.net@localhost?authMechanism=GSSAPI;"
    parser = Mongo::URIParser.new(uri)
    assert_equal 'GSSAPI', parser.auths.first[:mechanism]
    assert_equal 'foo/bar@example.net', parser.auths.first[:username]


    uri = "mongodb://foo%2Fbar%40example.net@localhost?authMechanism=GSSAPI;" +
            "gssapiServiceName=mongodb;canonicalizeHostName=true"
    parser = Mongo::URIParser.new(uri)
    assert_equal 'GSSAPI', parser.auths.first[:mechanism]
    assert_equal 'foo/bar@example.net', parser.auths.first[:username]
    assert_equal 'mongodb', parser.auths.first[:extra][:gssapi_service_name]
    assert_equal true, parser.auths.first[:extra][:canonicalize_host_name]
  end

  def test_opts_case_sensitivity
    # options gssapiservicename, authsource, replicaset, w should be case sensitive
    uri = "mongodb://localhost?gssapiServiceName=MongoDB;" +
            "authSource=FooBar;" +
            "replicaSet=Foo;" +
            "w=Majority"
    parser = Mongo::URIParser.new(uri)
    assert_equal 'MongoDB', parser.gssapiservicename
    assert_equal 'FooBar',  parser.authsource
    assert_equal 'Foo',     parser.replicaset
    assert_equal :Majority, parser.w
  end
end
