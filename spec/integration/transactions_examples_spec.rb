require 'spec_helper'

describe 'Transactions examples' do
  require_transaction_support

  let(:hr) do
    authorized_client.use(:hr).database
  end

  let(:reporting) do
    authorized_client.use(:reporting).database
  end

  before(:each) do
    hr[:employees].insert_one(employee: 3, status: 'Active')
    reporting[:events].insert_one(employee: 3, status: { new: 'Active', old: nil})
  end

  after(:each) do
    hr.drop
    reporting.drop
  end

  context 'individual examples' do

    let(:session) do
      authorized_client.start_session
    end

    # Start Transactions Intro Example 1

    def update_employee_info(session)
      employees_coll = session.client.use(:hr)[:employees]
      events_coll = session.client.use(:reporting)[:events]

      session.start_transaction(read_concern: { level: :snapshot },
                                write_concern: { w: :majority })
      employees_coll.update_one({ employee: 3 }, { '$set' => { status: 'Inactive'} },
                                session: session)
      events_coll.insert_one({ employee: 3, status: { new: 'Inactive', old: 'Active' } },
                             session: session)

      begin
        session.commit_transaction
        puts 'Transaction committed.'
      rescue Mongo::Error => e
        if e.label?('UnknownTransactionCommitResult')
          puts "UnknownTransactionCommitResult, retrying commit operation..."
          retry
        else
          puts 'Error during commit ...'
          raise
        end
      end
    end

    # End Transactions Intro Example  1

    context 'Transactions Intro Example 1' do

      let(:run_transaction) do
        update_employee_info(session)
      end

      it 'makes the changes to the database' do
        run_transaction
        employee = hr[:employees].find({ employee: 3 }, limit: 1).first
        expect(employee).to_not be_nil
        expect(employee['status']).to eq('Inactive')
      end
    end

    context 'Transactions Retry Example 1' do

      # Start Transactions Retry Example 1

      def run_transaction_with_retry(session)
        begin
          yield session # performs transaction
        rescue Mongo::Error => e

          puts 'Transaction aborted. Caught exception during transaction.'
          raise unless e.label?('TransientTransactionError')

          puts "TransientTransactionError, retrying transaction ..."
          retry
        end
      end

      # End Transactions Retry Example 1

      let(:run_transaction) do
        run_transaction_with_retry(session) { |s| update_employee_info(s) }
      end

      it 'makes the changes to the database' do
        run_transaction
        employee = hr[:employees].find({ employee: 3 }, limit: 1).first
        expect(employee).to_not be_nil
        expect(employee['status']).to eq('Inactive')
      end
    end

    context 'Transactions Retry Example 2' do

      # Start Transactions Retry Example 2

      def commit_with_retry(session)
        begin
          session.commit_transaction
          puts 'Transaction committed.'
        rescue Mongo::Error=> e
          if e.label?('UnknownTransactionCommitResult')
            puts "UnknownTransactionCommitResult, retrying commit operation..."
            retry
          else
            puts 'Error during commit ...'
            raise
          end
        end
      end

      # End Transactions Retry Example 2

      let(:run_transaction) do
        session.start_transaction
        hr[:employees].insert_one({ employee: 4, status: 'Active' }, session: session)
        reporting[:events].insert_one({ employee: 4, status: { new: 'Active', old: nil } },
                                      session: session)
        commit_with_retry(session)
      end

      it 'makes the changes to the database' do
        run_transaction
        employee = hr[:employees].find({ employee: 4 }, limit: 1).first
        expect(employee).to_not be_nil
        expect(employee['status']).to eq('Active')
      end
    end
  end

  context 'Transactions Retry Example 3 (combined example)' do

    let(:client) do
      authorized_client
    end

    let(:run_transaction) do

      # Start Transactions Retry Example 3

      def run_transaction_with_retry(session)
        begin
          yield session # performs transaction
        rescue Mongo::Error => e
          puts 'Transaction aborted. Caught exception during transaction.'
          raise unless e.label?('TransientTransactionError')
          puts "TransientTransactionError, retrying transaction ..."
          retry
        end
      end

      def commit_with_retry(session)
        begin
          session.commit_transaction
          puts 'Transaction committed.'
        rescue Mongo::Error => e
          if e.label?('UnknownTransactionCommitResult')
            puts "UnknownTransactionCommitResult, retrying commit operation ..."
            retry
          else
            puts 'Error during commit ...'
            raise
          end
        end
      end

      # updates two collections in a transaction

      def update_employee_info(session)
        employees_coll = session.client.use(:hr)[:employees]
        events_coll = session.client.use(:reporting)[:events]

        session.start_transaction(read_concern: { level: :snapshot },
                                  write_concern: { w: :majority })
        employees_coll.update_one({ employee: 3 }, { '$set' => { status: 'Inactive'} },
                                  session: session)
        events_coll.insert_one({ employee: 3, status: { new: 'Inactive', old: 'Active' } },
                               session: session)
        commit_with_retry(session)
      end

      session = client.start_session

      begin
        run_transaction_with_retry(session) { |s| update_employee_info(s) }
      rescue StandardError => e
        # Do something with error
        raise
      end

      # End Transactions Retry Example 3
    end

    it 'makes the changes to the database' do
      run_transaction
      employee = hr[:employees].find({ employee: 3 }, limit: 1).first
      expect(employee).to_not be_nil
      expect(employee['status']).to eq('Inactive')
    end
  end
end
