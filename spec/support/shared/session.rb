shared_examples 'an operation using a session' do

  describe 'operation execution', if: sessions_enabled? do

    let(:session) do
      authorized_client.start_session do |s|
        expect(s).to receive(:use).and_call_original
      end
    end

    let!(:before_last_use) do
      session.instance_variable_get(:@last_use)
    end

    let!(:before_operation_time) do
      session.instance_variable_get(:@operation_time)
    end

    let!(:result) do
      operation
    end

    it 'updates the last use value' do
      expect(session.instance_variable_get(:@last_use)).not_to eq(before_last_use)
    end

    it 'updates the operation time value' do
      expect(session.instance_variable_get(:@operation_time)).not_to eq(before_operation_time)
    end
  end
end