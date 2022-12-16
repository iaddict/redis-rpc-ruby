require_relative 'spec_helper'

describe RedisRpc::Server do
  context "running" do
    it "stops" do
      server = RedisRpc::Server.new(Redis.new($REDIS_CONFIG), 'calc', Array.new, logger: Logger.new(STDERR))
      server_thread = Thread.start {
        server.run
      }
      expect(server_thread.stop?).to be false
      expect(server_thread.alive?).to be true

      expect(server.stop!).to be true

      expect(server_thread.stop?).to be true
    end
  end
end
