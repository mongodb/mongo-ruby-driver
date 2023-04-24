# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

require_relative './shared/supports_retries'
require_relative './shared/only_supports_legacy_retries'
require_relative './shared/does_not_support_retries'

describe 'Retryable Writes' do
  require_fail_command
  require_wired_tiger
  require_no_multi_mongos
  require_warning_clean

  let(:client) do
    authorized_client.with(
      socket_timeout: socket_timeout,
      retry_writes: retry_writes,
      max_write_retries: max_write_retries,
    )
  end

  let(:socket_timeout) { nil }
  let(:retry_writes) { nil }
  let(:max_write_retries) { nil }

  let(:collection) { client['test'] }

  before do
    collection.drop
  end

  context 'collection#insert_one' do
    let(:command_name) { 'insert' }

    let(:perform_operation) do
      collection.insert_one(_id: 1)
    end

    let(:actual_result) do
      collection.count(_id: 1)
    end

    let(:expected_successful_result) do
      1
    end

    let(:expected_failed_result) do
      0
    end

    it_behaves_like 'it supports retries'
  end

  context 'collection#update_one' do
    before do
      collection.insert_one(_id: 1)
    end

    let(:command_name) { 'update' }

    let(:perform_operation) do
      collection.update_one({ _id: 1 }, { '$set' => { a: 1 } })
    end

    let(:actual_result) do
      collection.count(a: 1)
    end

    let(:expected_successful_result) do
      1
    end

    let(:expected_failed_result) do
      0
    end

    it_behaves_like 'it supports retries'
  end

  context 'collection#replace_one' do
    before do
      collection.insert_one(_id: 1, text: 'hello world')
    end

    let(:command_name) { 'update' }

    let(:perform_operation) do
      collection.replace_one({ text: 'hello world' }, { text: 'goodbye' })
    end

    let(:actual_result) do
      collection.count(text: 'goodbye')
    end

    let(:expected_successful_result) do
      1
    end

    let(:expected_failed_result) do
      0
    end

    it_behaves_like 'it supports retries'
  end

  context 'collection#delete_one' do
    before do
      collection.insert_one(_id: 1)
    end

    let(:command_name) { 'delete' }

    let(:perform_operation) do
      collection.delete_one(_id: 1)
    end

    let(:actual_result) do
      collection.count(_id: 1)
    end

    let(:expected_successful_result) do
      0
    end

    let(:expected_failed_result) do
      1
    end

    it_behaves_like 'it supports retries'
  end

  context 'collection#find_one_and_update' do
    before do
      collection.insert_one(_id: 1)
    end

    let(:command_name) { 'findAndModify' }

    let(:perform_operation) do
      collection.find_one_and_update({ _id: 1 }, { '$set' => { text: 'hello world' } })
    end

    let(:actual_result) do
      collection.count(text: 'hello world')
    end

    let(:expected_successful_result) do
      1
    end

    let(:expected_failed_result) do
      0
    end

    it_behaves_like 'it supports retries'
  end

  context 'collection#find_one_and_replace' do
    before do
      collection.insert_one(_id: 1, text: 'hello world')
    end

    let(:command_name) { 'findAndModify' }

    let(:perform_operation) do
      collection.find_one_and_replace({ text: 'hello world' }, { text: 'goodbye' })
    end

    let(:actual_result) do
      collection.count(text: 'goodbye')
    end

    let(:expected_successful_result) do
      1
    end

    let(:expected_failed_result) do
      0
    end

    it_behaves_like 'it supports retries'
  end

  context 'collection#find_one_and_delete' do
    before do
      collection.insert_one(_id: 1)
    end

    let(:command_name) { 'findAndModify' }

    let(:perform_operation) do
      collection.find_one_and_delete(_id: 1)
    end

    let(:actual_result) do
      collection.count(_id: 1)
    end

    let(:expected_successful_result) do
      0
    end

    let(:expected_failed_result) do
      1
    end

    it_behaves_like 'it supports retries'
  end

  context 'collection#update_many' do
    let(:command_name) { 'update' }

    before do
      collection.insert_one(_id: 1, text: 'hello world')
      collection.insert_one(_id: 2, text: 'hello world')
    end

    let(:perform_operation) do
      collection.update_many({ text: 'hello world' }, { '$set' => { text: 'goodbye' } })
    end

    let(:actual_result) do
      collection.count(text: 'goodbye')
    end

    let(:expected_successful_result) do
      2
    end

    let(:expected_failed_result) do
      0
    end

    it_behaves_like 'it only supports legacy retries'
  end

  context 'collection#delete_many' do
    let(:command_name) { 'delete' }

    before do
      collection.insert_one(_id: 1, text: 'hello world')
      collection.insert_one(_id: 2, text: 'hello world')
    end

    let(:perform_operation) do
      collection.delete_many(text: 'hello world')
    end

    let(:actual_result) do
      collection.count(text: 'hello world')
    end

    let(:expected_successful_result) do
      0
    end

    let(:expected_failed_result) do
      2
    end

    it_behaves_like 'it only supports legacy retries'
  end

  context 'collection#bulk_write' do
    context 'with insert_one' do
      let(:command_name) { 'insert' }

      let(:perform_operation) do
        collection.bulk_write([{ insert_one: { _id: 1 } }])
      end

      let(:actual_result) do
        collection.count(_id: 1)
      end

      let(:expected_successful_result) do
        1
      end

      let(:expected_failed_result) do
        0
      end

      it_behaves_like 'it supports retries'
    end

    context 'with delete_one' do
      let(:command_name) { 'delete' }

      before do
        collection.insert_one(_id: 1)
      end

      let(:perform_operation) do
        collection.bulk_write([{ delete_one: { filter: { _id: 1 } } }])
      end

      let(:actual_result) do
        collection.count(_id: 1)
      end

      let(:expected_successful_result) do
        0
      end

      let(:expected_failed_result) do
        1
      end

      it_behaves_like 'it supports retries'
    end

    context 'with update_one' do
      let(:command_name) { 'update' }

      before do
        collection.insert_one(_id: 1, text: 'hello world')
      end

      let(:perform_operation) do
        collection.bulk_write([{ update_one: { filter: { text: 'hello world' }, update: { '$set' => { text: 'goodbye' } } } }])
      end

      let(:actual_result) do
        collection.count(text: 'goodbye')
      end

      let(:expected_successful_result) do
        1
      end

      let(:expected_failed_result) do
        0
      end

      it_behaves_like 'it supports retries'
    end

    context 'with delete_many' do
      let(:command_name) { 'delete' }

      before do
        collection.insert_one(_id: 1, text: 'hello world')
        collection.insert_one(_id: 2, text: 'hello world')
      end

      let(:perform_operation) do
        collection.bulk_write([{ delete_many: { filter: { text: 'hello world' } } }])
      end

      let(:actual_result) do
        collection.count(text: 'hello world')
      end

      let(:expected_successful_result) do
        0
      end

      let(:expected_failed_result) do
        2
      end

      it_behaves_like 'it only supports legacy retries'
    end

    context 'with update_many' do
      let(:command_name) { 'update' }

      before do
        collection.insert_one(_id: 1, text: 'hello world')
        collection.insert_one(_id: 2, text: 'hello world')
      end

      let(:perform_operation) do
        collection.bulk_write([{ update_many: { filter: { text: 'hello world' }, update: { '$set' => { text: 'goodbye' } } } }])
      end

      let(:actual_result) do
        collection.count(text: 'goodbye')
      end

      let(:expected_successful_result) do
        2
      end

      let(:expected_failed_result) do
        0
      end

      it_behaves_like 'it only supports legacy retries'
    end
  end

  context 'database#command' do
    let(:command_name) { 'ping' }

    let(:perform_operation) do
      collection.database.command(ping: 1)
    end

    it_behaves_like 'it does not support retries'
  end
end
