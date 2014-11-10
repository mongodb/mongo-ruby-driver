# Copyright (C) 2014 MongoDB, Inc.
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

module SCRAMTests

  def setup_conversation
    SecureRandom.expects(:base64).returns('NDA2NzU3MDY3MDYwMTgy')
    @password = Digest::MD5.hexdigest("user:mongo:pencil")
    @scram = Mongo::Authentication::SCRAM.new({ :username => 'user' }, @password)
  end

  def test_scram_authenticate
    if @version.to_s > '2.7'
      @client.clear_auths
      assert @db.authenticate(TEST_USER, TEST_USER_PWD, nil, 'admin', 'SCRAM-SHA-1')
    end
  end

  def test_scram_conversation_start
    setup_conversation
    command = @scram.start
    assert_equal 1, command['saslStart']
    assert_equal 'SCRAM-SHA-1', command['mechanism']
    assert_equal 'n,,n=user,r=NDA2NzU3MDY3MDYwMTgy', command['payload'].to_s
  end

  def test_scram_conversation_continue
    setup_conversation
    payload = BSON::Binary.new(
      'r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8,s=AVvQXzAbxweH2RYDICaplw==,i=10000'
    )
    reply = { 'conversationId' => 1, 'done' => false, 'payload' => payload, 'ok' => 1.0 }
    command = @scram.continue(reply)
    assert_equal 1, command['saslContinue']
    assert_equal 1, command['conversationId']
    assert_equal(
      'c=biws,r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8,p=qYUYNy6SQ9Jucq9rFA9nVgXQdbM=',
      command['payload'].to_s
    )
  end

  def test_scram_conversation_continue_with_invalid_nonce
    setup_conversation
    payload = BSON::Binary.new(
      'r=NDA2NzU4MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8,s=AVvQXzAbxweH2RYDICaplw==,i=10000'
    )
    reply = { 'conversationId' => 1, 'done' => false, 'payload' => payload, 'ok' => 1.0 }
    assert_raise_error Mongo::InvalidNonce do
      @scram.continue(reply)
    end
  end

  def test_scram_conversation_finalize
    setup_conversation
    continue_payload = BSON::Binary.new(
      'r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8,s=AVvQXzAbxweH2RYDICaplw==,i=10000'
    )
    continue_reply = { 'conversationId' => 1, 'done' => false, 'payload' => continue_payload, 'ok' => 1.0 }
    @scram.continue(continue_reply)
    payload = BSON::Binary.new('v=gwo9E8+uifshm7ixj441GvIfuUY=')
    reply = { 'conversationId' => 1, 'done' => false, 'payload' => payload, 'ok' => 1.0 }
    command = @scram.finalize(reply)
    assert_equal 1, command['saslContinue']
    assert_equal 1, command['conversationId']
    assert_equal '', command['payload'].to_s
  end

  def test_scram_conversation_finalize_with_invalid_server_signature
    setup_conversation
    continue_payload = BSON::Binary.new(
      'r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8,s=AVvQXzAbxweH2RYDICaplw==,i=10000'
    )
    continue_reply = { 'conversationId' => 1, 'done' => false, 'payload' => continue_payload, 'ok' => 1.0 }
    @scram.continue(continue_reply)
    payload = BSON::Binary.new('v=LQ+8yhQeVL2a3Dh+TDJ7xHz4Srk=')
    reply = { 'conversationId' => 1, 'done' => false, 'payload' => payload, 'ok' => 1.0 }
    assert_raise_error Mongo::InvalidSignature do
      @scram.finalize(reply)
    end
  end
end
