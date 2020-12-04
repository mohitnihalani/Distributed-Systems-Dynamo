defmodule DynamoNode do
  import Emulation, only: [send: 2, timer: 1, now: 0, whoami: 0]
  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Fuzzers
  require Logger

  require Ring
  """
  Dynamo Node
  Each node has it's view of the state, which basically means each node has its own ring
  Node can be initiated using the seed node or without the seed node
  Seed node is used so that node can connect to the ring when joined initially
  If seed node is provided in the confguration, it will try to connect to the seed node using gossip protocol
  and join the states.

  - State -> hash_ring
  - pid
  - seed_node
  """
  # TODO -> Moving version inside the ring so we can have one state which consists on the ring and vector clock for the node
  # It would be easier to merge states, as we can only pass the ring when calling :share_state

  # TODO -> If node is added also transfer k,v pairs
  # Token Calls
  # :share_state -> for share state request
  # :get
  defstruct(
    state: nil,
    pid: nil,
    kv: %DynamoNode.KV{},
    seed_node: nil,

    # Gossip Protocol
    heartbeat_timer: nil,
    heartbeat_timeout: 2000,
    probe_timer: nil,
    probe_timeout: 1000,
    probe_count: 5,
    ack_timer: nil,
    ack_timeout: 500,

    # For Quorum
    n: 3, # (N,R,W) for quorum
    r: 2,
    w: 1,

    # This can also be used as a incarnation number
    # For Swim Protocol
    version: 0
  )


  def init() do
    hash_ring = Ring.new(whoami())
    %DynamoNode{state: hash_ring, pid: whoami()}
  end


  def init(n, r, w) do
    hash_ring = Ring.new(whoami())
    %DynamoNode{state: hash_ring, pid: whoami(), n: n, r: r, w: w}
  end

  def init(seed_node) do
    hash_ring = Ring.new()
    hash_ring = Ring.add_nodes(hash_ring, [whoami(), seed_node])
    node = %DynamoNode{state: hash_ring, pid: whoami(), seed_node: seed_node}
  end

  """
  With Seed Node
  """
  def init(seed_node, n, r, w) do
    hash_ring = Ring.new()
    hash_ring = Ring.add_nodes(hash_ring, [whoami(), seed_node])
    node = %DynamoNode{state: hash_ring, pid: whoami(), seed_node: seed_node, n: n, r: r, w: w}
  end

  @spec get_gossip_timeout(%DynamoNode{}) :: non_neg_integer()
  defp get_gossip_timeout(%DynamoNode{heartbeat_timeout: heartbeat_timeout}=state) do
    :rand.uniform(heartbeat_timeout) + 200
  end

  defp save_heartbeat_timer(node, timer) do
    %{node | heartbeat_timer: timer}
  end

  defp start_ack_timer(node)  do
    %{node | ack_timer: Emulation.timer(node.ack_timeout)}
  end

  defp start_probe_timer(node)  do
    %{node | probe_timer: Emulation.timer(node.probe_timeout)}
  end

  @spec reset_gossip_timeout(%DynamoNode{}) :: %DynamoNode{}
  defp reset_gossip_timeout(%DynamoNode{heartbeat_timer: heartbeat_timer} = node) do
    if heartbeat_timer, do: Emulation.cancel_timer(heartbeat_timer)
    gossip_timeout = get_gossip_timeout(node)
    save_heartbeat_timer(node, Emulation.timer(node.heartbeat_timeout))
  end

  @spec handle_share_state_request(%Ring{}, {atom(), %Ring{}}) :: %Ring{}
  defp handle_share_state_request(state, {sender, otherstate}) do
    state = Ring.find_updated_state(state, otherstate)
    updated_ring = join_states(state, otherstate)
    send(sender, DynamoNode.ShareStateResponse.new(updated_ring))
    updated_ring
  end

  @spec join_states(%Ring{}, %Ring{}) :: %Ring{}
  def join_states(state, otherstate) do
    # Synch both hash rings
    Ring.sync_rings(state, otherstate)
  end


  defp update_node_version(node, other_node, version) do
    %{node | state: Ring.update_node_incarnation(node.state, other_node, version)}
  end

  defp increment_vector_clock(node) do
    %{node | state: Ring.increment_vector_clock(node.state, whoami())}
  end

  @spec add_suspect_node(%DynamoNode{}, atom(), integer()) :: %DynamoNode{}
  defp add_suspect_node(node, suspect_node, incarnation) do
    node = %{node | state: Ring.add_suspect_node(node.state, suspect_node, incarnation, now())}
    spread_suspect_gossip(node, suspect_node)
    node
  end

  defp handle_node_alive(node, suspect_node, incarnation) do
    %{node | state: Ring.handle_node_alive(node.state, suspect_node, incarnation)}
  end

  defp handle_node_fail(node, failed_node) do
    %{node | state: Ring.remove_node(node.state, failed_node)}
  end

  # Spread gossip about suspect node
  @spec spread_suspect_gossip(%DynamoNode{}, atom()) :: no_return()
  defp spread_suspect_gossip(node, suspect_node) do
    node_list = Enum.filter(node.state.nodes, fn x -> x != whoami() end)
    node_list = Enum.shuffle(node_list) |> Enum.take(min(node.probe_count, Enum.count(node_list)))
    incarnation = Ring.get_node_incarnation(node.state, suspect_node)
    send_requests(node_list, DynamoNode.SuspectNode.new(suspect_node, incarnation))
  end

  # Spread Gossip about node failed
  @spec spread_suspect_gossip(%DynamoNode{}, atom()) :: no_return()
  defp spread_node_failed(node, failed_node) do
    node_list = Enum.filter(node.state.nodes, fn x -> x != whoami() end)
    send_requests(node_list, DynamoNode.NodeFailed.new(failed_node))
  end

  @spec check_and_remove_suspect(%DynamoNode{}, atom()) :: %DynamoNode{}
  defp check_and_remove_suspect(node, other_node) do
    if Ring.is_suspect_node(node.state, other_node) do
      incarnation = Ring.get_node_incarnation(node.state, other_node)
      node = handle_node_alive(node, other_node, incarnation)
      node_list = Enum.filter(node.state.nodes, fn x -> x != whoami() end)
      send_requests(node_list, DynamoNode.NodeAlive.new(other_node, incarnation))
      node
    else
      node
    end
  end

  # Starts Indirect Probe request for the node which didn't replied
  defp start_gossip_indirect_probe(node, extra_state, random_node) do
    IO.puts("(#{whoami()}) Running Indirect Probe Request")

    me = whoami()
    indirect_probe_list = Enum.filter(node.state.nodes, fn x -> x != whoami() end)
    indirect_probe_list = Enum.shuffle(indirect_probe_list) |> Enum.take(min(node.probe_count, Enum.count(indirect_probe_list)))
    send_requests(indirect_probe_list, DynamoNode.IndirectProbe.new(random_node, Ring.get_node_incarnation(node.state, random_node), me))

    node = start_probe_timer(node)
    receive do

      :timer ->
        IO.puts("Indirect Probe Failed <> #{whoami()} marking #{random_node} as SUSPECT")
        #TODO Handle node suspect
        # 1. Add node to suspect node
        node = add_suspect_node(node, random_node, -1)
        {node, extra_state}

      {sender, %DynamoNode.IndirectProbeResponse{
        node: ^random_node,
        incarnation: incarnation,
        status: :alive,
        requestee: ^me
      }} ->
        Emulation.cancel_timer(node.probe_timer)
        node = update_node_version(node, random_node, incarnation)
        {node, extra_state}
    end

  end

  # Run Gossip Protocol
  def run_gossip_protocol(node, extra_state) do

    # Filter Current Node from the list
    node_list = Enum.filter(node.state.nodes, fn x -> x != whoami() end)
    if Enum.count(node_list) <= 0 do
      node = reset_gossip_timeout(node)
      run_node(node, extra_state)
    else
      random_node = Enum.random(node_list)
      send(random_node,DynamoNode.ShareStateRequest.new(node.state))
      node = start_ack_timer(node)
      node = increment_vector_clock(node)
      receive do
        {^random_node, %DynamoNode.ShareStateResponse{
          state: otherstate
        }} ->

          IO.puts("(#{whoami()}) Received Share State Response from (#{random_node})
          <> #{whoami()} has #{Ring.get_node_count(node.state)} and #{random_node} have #{Ring.get_node_count(otherstate)}")
          Emulation.cancel_timer(node.ack_timer)
          node = %{node | state: join_states(node.state, otherstate)}
          #node = check_and_remove_suspect(node, random_node)
          node = reset_gossip_timeout(node)
          run_node(node, extra_state)

        :timer ->
          IO.puts("(#{whoami()}) Gossip Protocol Request Timeout #{random_node}")
          #{node, extra_state} = start_gossip_indirect_probe(node, extra_state, random_node)
          node = reset_gossip_timeout(node)
          run_node(node, extra_state)
      end

    end
  end

  @spec get_preference_list(%Ring{}, any(), non_neg_integer()) :: [atom()]
  def get_preference_list(ring, key, n) do
    Ring.nodes_for_key(ring, key, n)
  end

  def send_requests(node_list, message) do
    Enum.map(node_list, fn pid -> send(pid, message) end)
  end

  def check_quorum_write(node, extra_state, client) do
    if Map.has_key?(extra_state, client) do
      # Check If "W" acknowledgement is received
      if Map.fetch!(extra_state, client) + 1 >= node.w do
        IO.puts("(#{whoami()}) Have enough confirmations #{Map.fetch!(extra_state, client)} #{node.w} #{node.n} #{node.r}")
        send(client, :ok)
        {node,  Map.delete(extra_state, client)}
      else
        extra_state = Map.put(extra_state, client, Map.fetch!(extra_state, client) + 1)
        {node, extra_state}
      end
    else
      # Probably already send the acknowledgement
      {node, extra_state}
    end
  end

  def merge_conflicting_version(versions) do
    # TODO Merge Context
  end

  def check_quorum_read(node, extra_state, client, entry) do
    if Map.has_key?(extra_state, client) do
      if Enum.count(Map.get(extra_state, client, []) + 1 >= node.r) do
        # TODO Merge Context
        {merged_values, merged_context} = merge_conflicting_version(Map.get(extra_state, client))
        send(client, {merged_values, merged_context})
        {node,  Map.delete(extra_state, client)}
      else
        extra_state = Map.put(extra_state, client, [entry | Map.get(extra_state, client, [])])
        {node, extra_state}
      end
    else
      # Probably already send the acknowledgement
      {node, extra_state}
    end
  end

  @spec add_entry_to_db(%DynamoNode{}, any(), map(), any(), boolean()) :: %DynamoNode{}
  def add_entry_to_db(node, key, context, value, coordinator) do
    #TODO Add vector clocks to add the version
    version = Ring.get_node_version(node.state, whoami())
    if coordinator do
      %{node | kv: DynamoNode.KV.put(node.kv, whoami(), version, key, context, value)}
    else
      %{node | kv: DynamoNode.KV.put(node.kv, key, context, value)}
    end

  end

  """
  Method to handle put request
  """
  defp handle_put_request(node, extra_state, client, key, value, context) do
    # Get the preferred list
    preference_list = get_preference_list(node.state, key, node.n)
    #Enum.each(preference_list, fn node -> IO.puts(node) end)
    pid = whoami()
    case preference_list do
      [^pid | tail]->
        # If Coordinator Node, store and send to other nodes
        node = add_entry_to_db(node, key, context, value, true)
        send_requests(tail, DynamoNode.PutEntry.new(key, context, value, client))
        # Store into extra state to check if received "W Quorum Request"
        extra_state = Map.put_new(extra_state, client, 0)
        IO.puts("#{whoami} Checking Quorum Write #{Map.fetch!(extra_state, client)}")
        check_quorum_write(node, extra_state, client)
      [head | tail]->
        # If not, send the the preferred node for the given key
        IO.puts("#{whoami} Redirecting Put Request for key <> #{key} to #{head}")
        send(head, {:redirect_put, {client, key, value, context}})
        {node, extra_state}
      _ ->
        # I don't know about this
        {node, extra_state}
    end
  end

  """
  Handling get request
  """
  defp handle_get_request(node, extra_state, client, key) do
      # Get the preferred list
      preference_list = get_preference_list(node.state, key, node.n)
      Enum.each(preference_list, fn node -> IO.puts(node) end)
      pid = whoami()

      case preference_list do
        [^pid | tail]->
          # I am the coordinator node, get from the database and ask other nodes
          entry = DynamoNode.KV.get(node.kv, key)
          case entry do
            :noentry ->
              # Noentry can happen, when client saved this request to server A,
              # but currenty this entry is handled by server B, during joining of ring,
              # B didn't have this entry in it's database so it asks for the given entry to all
              # the servers in the ring
              node_list = Enum.filter(node.state.nodes, fn x -> x != whoami() end)
              send(node_list, DynamoNode.GetEntry.new(key, client))
              {node, extra_state}

            %DynamoNode.Entry{key: ^key} ->
              send_requests(tail, DynamoNode.GetEntry.new(key, client))
              # This is needed for "R = 1".
              extra_state = Map.put_new(extra_state, client, [entry])
              check_quorum_read(node, extra_state, client, extra_state)
            true ->
              #TODO Think about what to do if the entry is not present in the db
              {node, extra_state}
          end
        [head | tail] ->
          IO.puts("#{whoami} Redirecting Get Request for key <> #{key} to #{head}")
          send(head, {:redirect_get, {client, key}})
          {node, extra_state}
          # I am not the coordinator send to the appropriate node
        _ ->
          # I don't know about this
          {node, extra_state}
      end
  end

  @spec lauch_node(%DynamoNode{}) :: no_return()
  def lauch_node(node) do
    IO.puts("Launching node #{whoami()}")
    node = reset_gossip_timeout(node)
    IO.puts(Ring.get_node_count(node.state))
    run_node(node, %{})
  end

  """
  This is where most recursion happend maintaing state
  """
  defp run_node(node, extra_state) do
    #IO.puts("Running Node  #{whoami()}")
    node = increment_vector_clock(node)
    receive do

      :timer ->

        if Ring.get_node_count(node.state) >= 2 do
          run_gossip_protocol(node, extra_state)
        else
          run_node(node, extra_state)
        end

      #---------------------------  Gossip Protocol Request ------------------------------------------- #
      # Share State Request For Gossip Protocol
      {sender, %DynamoNode.ShareStateRequest{state: otherstate}} ->
        IO.puts("#{whoami()} (#{Ring.get_node_count(node.state)}) Received Share State request from #{sender} (#{Ring.get_node_count(otherstate)})")

        node = %{node | state: handle_share_state_request(node.state, {sender, otherstate})}
        run_node(node, extra_state)
        # code

      {sender, %DynamoNode.IndirectProbe{node: other_node, incarnation: other_incarnation, requestee: requestee} = probe_request} ->
        IO.puts("(#{whoami()}) received Indirect probing request for (#{other_node}) from (#{sender})")
        incarnation = Ring.get_node_incarnation(node.state, other_node)
        if other_node == whoami() ||  incarnation > other_incarnation do
          send(sender, DynamoNode.IndirectProbeResponse.new(other_node, incarnation, :alive, requestee))
        else
          send(other_node, probe_request)
        end

        run_node(node, extra_state)

      {sender, %DynamoNode.IndirectProbeResponse{node: other_node, incarnation: other_incarnation, status: status, requestee: requestee} = probe_response} ->
        me = whoami()
        case requestee do
          ^me ->
            #TODO IF in suspect list mark as alive else don't do anythind
            run_node(node, extra_state)

          _ ->
            send(requestee, probe_response)
            run_node(node, extra_state)
        end

      # Handle Suspect Node
      {sender, %DynamoNode.SuspectNode{node: suspect_node, incarnation: other_incarnation}} ->
        IO.puts("(#{whoami()}) received suspect node request for (#{suspect_node}) from (#{sender})")
        if Ring.get_node_incarnation(node.state, suspect_node) <= other_incarnation do
          node = add_suspect_node(node, suspect_node, other_incarnation)
          run_node(node, extra_state)
        else
          send(sender, DynamoNode.NodeAlive.new(suspect_node, Ring.get_node_incarnation(node.state, suspect_node)))
          run_node(node, extra_state)
        end

      # Handle Node Alive
      {sender, %DynamoNode.NodeAlive{node: suspect_node, incarnation: other_incarnation}} ->
        IO.puts("(#{whoami()}) received Node Alive response for (#{suspect_node}) from (#{sender})")
        if Ring.get_node_incarnation(node.state, suspect_node) < other_incarnation do
          # Handle Node Alive
          run_node(handle_node_alive(node, suspect_node, other_incarnation), extra_state)
        else
          # Old message do nothing
          run_node(node, extra_state)
        end

      # Handle Failed Node
      {sender, %DynamoNode.NodeFailed{node: failed_node}} ->
        IO.puts("(#{whoami()}) received Node Failed request for (#{failed_node}) from (#{sender})")
        run_node(handle_node_fail(node, failed_node), extra_state)

      # ---------------  Handle Get Request -----------------------------#
      {sender, %DynamoNode.GetEntry{key: key, client: client}} ->
        IO.puts("(#{whoami()}) received Get Entry Request from (#{sender}) <> #{key}")
        # Handle  GetEntry for replication
        # TODO
        entry = DynamoNode.KV.get(node.kv, key)
        send(sender, DynamoNode.GetEntryResponse.new(client, entry, key))
        run_node(node, extra_state)

      {sender, %DynamoNode.GetEntryResponse{
        client: client,
        entry: entry,
        key: key,
      }} ->
        # Handle  GetEntry Response
        # Merge the context and send response to the client
        # TODO
        IO.puts("(#{whoami()}) received Get Entry Response from (#{sender}) <> #{key}")
        case entry do
          :noentry ->
            run_node(node, extra_state)
          true ->
            {node, extra_state} = check_quorum_read(node, extra_state, client, entry)
            run_node(node, extra_state)
        end

      # ---------------  Handle Put Request -----------------------------#
      {sender, %DynamoNode.PutEntry{
        context: context,
        value: value,
        key: key,
        client: client
      }} ->

        IO.puts("(#{whoami()}) received Put Entry Request from (#{sender}) <> #{key}")
        # Handle Put Entry for replication
        #TODO
        node = add_entry_to_db(node, key, context, value, false)
        send(sender, DynamoNode.PutEntryResponse.new(key, :ok, client))
        run_node(node, extra_state)

      # Put Entry Response from the other node
      {sender, %DynamoNode.PutEntryResponse{ack: ack, key: key, client: client}} ->
        # Handle Put Entry for response for the replication
        #TODO

        IO.puts("(#{whoami()}) received Put Entry Response from (#{sender}) <> #{key}")
        {node, extra_state} = check_quorum_write(node, extra_state, client)
        run_node(node, extra_state)

      # ---------------  Handle Redirect Client Request -----------------------------#

      # Put Request for the client
      # If request received by the node is not the coordinator node
      # It will redirect request to the appropriate client
      {sender, {:redirect_put, {client, key, value, context}}} ->
        # TODO Put given key, with this value and context
        # First Check If you are the preferred coordinator
        IO.puts("#{whoami} Received redirected put Request for key: #{key}")
        #client, key, value, context
        {node, extra_state} = handle_put_request(node, extra_state, client, key, value, context)
        run_node(node, extra_state)

      # Get Request for the client
      {sender, {:redirect_get, {client, key}}} ->
        #TODO Handle Client Request
        run_node(node, extra_state)

      # ---------------  Handle Client Request -----------------------------#

      # Put Request for the client
      {sender, {:put, {key, value, context}}} ->
        # TODO Put given key, with this value and context
        # First Check If you are the preferred coordinator
        {node, extra_state} = handle_put_request(node, extra_state, sender, key, value, context)
        run_node(node, extra_state)

      # Get Request for the client
      {sender, {:get, key}} ->
        #TODO Handle Client Request
        run_node(node, extra_state)

      # --------------- Testing ---------------------------------------- #
      {sender, :check} ->

        #IO.puts(sender)
        send(sender, :ok)
        run_node(node, extra_state)

      {sender, :get_state} ->
        send(sender, node.state)
        run_node(node, extra_state)
    end

  end
end


defmodule DynamoNode.Client do
  import Emulation, only: [send: 2, whoami: 0]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  alias __MODULE__
  defstruct(
    client_id: nil
  )

  @spec new_client(atom()) :: %Client{}
  def new_client(client_id) do
    %Client{client_id: client_id}
  end

  def benchmark_client(client) do
    IO.puts("Benchamrking client")
    receive do
      {pid, {:put, key, value, context, node}} ->
        message = put_request(client, key, value, context, node)
        IO.puts("Sending Reply Back")
        send(pid,  message)
        benchmark_client(client)
        # code
    end
    benchmark_client(client)
  end


  @spec check_node_status(%Client{}, atom()) :: {:fail,%Client{}} | {:ok,%Client{}}
  def check_node_status(client, node) do
    IO.puts("Check Node Status #{whoami()}")
    send(node, :check)
    receive do
      {^node, :ok} -> {:ok,client}
    after
      1000 -> {:fail,client}
    end
  end

  def client_get_state(client, node) do
    IO.puts("Getting State for #{node}")
    send(node, :get_state)
    receive do
      {^node, state} -> {state,client}
    after
      1000 -> {:fail,client}
    end
  end

  @spec put_request(%Client{}, any(), any(), any(), atom()) :: {:ok | :fail, %Client{}}
  def put_request(client, key, value, context, node) do
    IO.puts(key)
    send(node, {:put, {key, value, context}})
    receive do
      {_sender, :ok} ->
        {:ok, client}
    after
      5000 -> {:fail,client}
    end

  end

  def get(client, key, node) do

  end
end
