require_relative 'spec_helper'
require_relative '../examples/calc.rb'

describe Calculator do
  context "locally" do
    let(:calculator) { Calculator.new }

    it 'should calculate' do
      expect(calculator.val).to be == 0.0
      expect(calculator.add(3)).to be == 3.0
      expect(calculator.sub(2)).to be == 1.0
      expect(calculator.mul(14)).to be == 14.0
      expect(calculator.div(7)).to be == 2.0
      expect(calculator.val).to be == 2.0
      expect(calculator.clr).to be == 0.0
      expect(calculator.val).to be == 0.0
    end

    it 'should raise when missing method is called' do
      # noinspection RubyResolve
      expect { calculator.a_missing_method }.to raise_error(NoMethodError)
    end
  end

  context "over rpc" do
    let(:rpc_server_builder) { lambda { RedisRpc::Server.new(Redis.new($REDIS_CONFIG), 'calc', Calculator.new, logger: Logger.new(STDERR)) } }
    before(:each) do
      @rpc_server = Thread.start {
        rpc_server_builder.call.run
      }
    end
    after(:each) { rpc_server_builder.call.stop! && @rpc_server.kill; rpc_server_builder.call.flush_queue! }
    let(:calculator) { RedisRpc::Client.new($REDIS, 'calc', timeout: 2) }

    it 'should calculate' do
      expect(calculator.val).to be == 0.0
      expect(calculator.add(3)).to be == 3.0
      expect(calculator.sub(2)).to be == 1.0
      expect(calculator.mul(14)).to be == 14.0
      expect(calculator.div(7)).to be == 2.0
      expect(calculator.val).to be == 2.0
      expect(calculator.clr).to be == 0.0
      expect(calculator.val).to be == 0.0
    end

    it 'should raise when missing method is called' do
      # noinspection RubyResolve
      expect { calculator.a_missing_method }.to raise_error(RedisRpc::RemoteException)
    end

    it 'should raise when method is not implemented by receiver' do
      # noinspection RubyResolve
      expect { calculator.send(:instance_eval, "puts :remote_exec") }.to raise_error(RedisRpc::RemoteException, "Forbidden RPC call 'instance_eval'")
    end

    it 'should raise timeout when execution expires' do
      expect { calculator.send(:sleep, 3) }.to raise_error RedisRpc::TimeoutException
    end

    context "the request is executed late" do
      it "won't be executed" do
        allow(calculator).to receive(:get_timeout_at).and_return(Time.now.to_i - 1)
        expect { calculator.val }.to raise_error(RedisRpc::RemoteException, /Expired RPC call/)
      end
    end
  end
end
