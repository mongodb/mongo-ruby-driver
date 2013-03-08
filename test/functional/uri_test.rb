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

  def test_multiple_uris
    parser = Mongo::URIParser.new('mongodb://a.example.com:27018,b.example.com')
    assert_equal 2, parser.nodes.length
    assert_equal ['a.example.com', 27018], parser.nodes[0]
    assert_equal ['b.example.com', 27017], parser.nodes[1]
  end

  def test_complex_passwords
    parser = Mongo::URIParser.new('mongodb://bob:secret.word@a.example.com:27018/test')
    assert_equal "bob", parser.auths[0][:username]
    assert_equal "secret.word", parser.auths[0][:password]

    parser = Mongo::URIParser.new('mongodb://bob:s-_3#%R.t@a.example.com:27018/test')
    assert_equal "bob", parser.auths[0][:username]
    assert_equal "s-_3#%R.t", parser.auths[0][:password]
  end

  def test_complex_usernames
    parser = Mongo::URIParser.new('mongodb://b:ob:secret.word@a.example.com:27018/test')
    assert_equal "b:ob", parser.auths[0][:username]
  end

  def test_passwords_contain_no_commas
    assert_raise MongoArgumentError do
      Mongo::URIParser.new('mongodb://bob:a,b@a.example.com:27018/test')
    end
  end

  def test_multiple_uris_with_auths
    parser = Mongo::URIParser.new('mongodb://bob:secret@a.example.com:27018,b.example.com/test')
    assert_equal 2, parser.nodes.length
    assert_equal ['a.example.com', 27018], parser.nodes[0]
    assert_equal ['b.example.com', 27017], parser.nodes[1]
    assert_equal 2, parser.auths.length
    assert_equal "bob", parser.auths[0][:username]
    assert_equal "secret", parser.auths[0][:password]
    assert_equal "test", parser.auths[0][:db_name]
    assert_equal "bob", parser.auths[1][:username]
    assert_equal "secret", parser.auths[1][:password]
    assert_equal "test", parser.auths[1][:db_name]
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
end
