require File.expand_path( '../spec_helper.rb', __FILE__ )

describe RedisRpc::Client do
  let(:client){ RedisRpc::Client.new( $REDIS, 'example', 1) }

  context "terminated" do
    it "clear the backlog" do
      allow($REDIS).to receive(:blpop).and_raise SignalException.new("SIGTERM")
      expect {
        client.perform
      }.to raise_error SignalException

      expect($REDIS.llen("example")).to eq 0
    end
  end

  context "timeout" do
    it "clear the backlog" do
      expect {
        client.perform
      }.to raise_error RedisRpc::TimeoutException

      expect($REDIS.llen("example")).to eq 0
    end
  end
end
