
defmodule DynamoNode.BenchMarks do
  import Emulation, only: [spawn: 2, send: 2, whoami: 0]
  import Kernel,
      except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]
  import Metrix


  def run_put_benchmark() do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(1)])
    # mix run -e "DynamoNode.BenchMarks.run_put_benchmark()"
    n = 6
    r = 3
    w = 2

    spawn(:seed_node, fn -> DynamoNode.lauch_node(DynamoNode.init(n, r, w)) end)
    servers = [:a, :b, :c, :d, :e, :f, :g, :h, :i, :j, :k, :l, :m, :n, :o, :p, :q, :r, :s, :t, :u, :v, :w, :x, :y, :z]
    #servers = [:a, :b, :c, :d]
    Enum.map(servers, fn pid -> spawn(pid, fn -> DynamoNode.lauch_node(DynamoNode.init(:seed_node, n, r, w)) end) end)

     # Give things a bit of time to settle down.
    receive do
    after
      10_000 -> :ok
    end

    client = spawn(:client, fn -> DynamoNode.Client.benchmark_client(DynamoNode.Client.new_client(:client)) end)

    servers = Enum.shuffle(servers)
    alphabet = Enum.to_list(?a..?z) ++ Enum.to_list(?0..?9)
    length = 12

    Benchee.run(%{
      "Measuring Put Performance"  => fn input ->
        s = Enum.random(servers)
        send(input, {:put, Enum.take_random(alphabet, length), 200, nil, s})

        receive do
          {_, _ } ->
            0
            # code
        end
      end,
    },
    time: 1000,
    print: [
      benchmarking: true,
      configuration: true,
    ],
    formatters: [{Benchee.Formatters.HTML, file: "put_request_27_nodes/my.html"}, {Benchee.Formatters.Console, extended_statistics: true}],
    inputs: %{"input 1" => :client},
    )

    #measure "api.request", fn -> send(client, {:put, Enum.take_random(alphabet, length), 200, nil, Enum.random(servers)}) end


  after
    Emulation.terminate()
  end
end
