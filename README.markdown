RedisRpc
========

originated by Nathan Farrington
<http://nathanfarrington.com>

RedisRpc is the easiest to use RPC library in the world. (No small claim!).

repackaged by Phuong Nguyen
This repo only has Ruby implementations

Introduction
------------

[Redis][Redis] is a powerful in-memory data structure server that is useful
for building fast distributed systems. Redis implements message queue
functionality with its use of list data structures and the `LPOP`, `BLPOP`,
and `RPUSH` commands. RedisRpc implements a lightweight RPC mechanism using
Redis message queues to temporarily hold RPC request and response
messages. These messages are encoded as [JSON][JSON] strings for portability.

Many other RPC mechanisms are either programming language specific (e.g. [Java
RMI][JavaRMI]) or require boiler-plate code for explicit typing (e.g.
[Thrift][Thrift]). RedisRpc was designed to be extremely easy to use by
eliminating boiler-plate code while also being programming language neutral.
High performance was not an initial goal of RedisRpc and other RPC libraries
are likely to have better performance. Instead, RedisRpc has better programmer
performance; it lets you get something working immediately.

Calculator Example
------------------
Each library implementation uses the same client and server example based off
of a mutable calculator object. The clients and servers from different
languages are interoperable.

<img
src="https://github.com/phuongnd08/redis-rpc-ruby/raw/master/docs/redisrpc_example.png"
width=438 height=238>

1. The client issues an RPC Request by using the Redis `RPUSH` command to push
an RPC Request message into a Redis list called `calc`.
2. The server retrieves the RPC Request message by using the Redis `BLPOP`
command.
3. The server dispatches the RPC Request to a local object, which in this case
is a Calculator object.
4. The server accepts the return value (or exception) from the Calculator object.
5. The server issues an RPC Response by using the Redis `RPUSH` command to push
an RPC Response message into a Redis list called `calc:rpc:<RAND_STRING>`,
which was chosen by the client.
6. The client retrieves the RPC Response message by using the Redis `BLPOP`
command.

*Note that the server or client can be made non-blocking by using the Redis
LPOP command instead of BLPOP. I currently do not need this feature and have
not added support for this, but patches are welcome.*

That's all there is to it!

Ruby Usage
----------

### client.rb

```ruby
redis_server = Redis.new
message_queue = 'calc'
calculator = RedisRpc::Client.new redis_server, 'calc'
calculator.clr
calculator.add 5
calculator.sub 3
calculator.mul 4
calculator.div 2
assert calculator.val == 4
```

### server.rb

```ruby
redis_server = Redis.new
message_queue = 'calc'
local_object = Calculator.new
server = RedisRpc::Server.new redis_server, message_queue, local_object
server.run
```

Installation
------------

### Ruby Installation

The [redis-rb][redis-rb] library is required. Install using RubyGems:

```ruby
gem install redis-rpc
```

Internal Message Formats
------------------------
All RPC messages are JSON objects. User code will never see these objects
because they are handled by the RedisRpc library.

### RPC Request
An RPC Request contains two members: a `function_call` object and
a `response_queue` string.

A `function_call` object has one required member: a `name` string for the function
name. It also has two optional members: (a) an `args` list for positional
function arguments, and (b) a `kwargs` object for named function arguments.

The `response_queue` string is the name of the Redis list where the
corresponding RPC Response message should be pushed by the server. This queue
is chosen programmatically by the client to be collision free in the Redis
namespace.  Also, this queue is used only for a single RPC Response message
and is not reused for future RPC Response messages.

```javascript
{ "function_call" : {
      "args" : [ 1, 2, 3 ],
      "kwargs" : { "a" : 4, "b" : 5, "c" : 6 },
      "name" : "foo"
    },
  "response_queue" : "calc:rpc:X7Y2US"
}
```

### RPC Response (Successful)
If an RPC is successful, then the RPC Response object will contain a single
member, a `return_value` of some JSON type.

```javascript
{ "return_value" : 4.0 }
```

### RPC Response (Exception)
If an RPC encounters an exceptional condition, then the RPC Response object
will contain a single member, an `exception` string. Note that the value of
the `exception` string might not have any meaning to the client since the
client and server might be written in different languages or the client
might have no knowledge of the server's wrapped object. Therefore the best
course of action is probably to display the `exception` value to the user.

```javascript
{ "exception" : "AttributeError(\\"\'Calculator\' object has no attribute \'foo\'\\",)" }
```

Source Code
-----------
Source code is available at <http://github.com/phuongnd08/redis-rpc-ruby>.

License
-------
This software is available under the [GPLv3][GPLv3] or later.

[Redis]: http://redis.io/

[JSON]: http://json.org/

[JavaRMI]: https://en.wikipedia.org/wiki/Java_remote_method_invocation

[Thrift]: https://en.wikipedia.org/wiki/Apache_Thrift

[redis-rb]: https://github.com/ezmobius/redis-rb

[GPLv3]: http://www.gnu.org/licenses/gpl.html
