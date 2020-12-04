defmodule DynamoNode.Entry do
  @moduledoc """
  Response for RequestVote requests.
  """
  alias __MODULE__
  @enforce_keys [:context, :key, :value]
  defstruct(
    context: nil, # Vector Clock
    value: nil,
    key: nil
  )
  @doc """
  Create a new Entry
  """
  def new(key, context, value) do
    %Entry{context: context, value: value, key: key}
  end
end


defmodule DynamoNode.KV do
  alias __MODULE__

  defstruct(
    db: %{}
  )

  def new() do
    %KV{db: %{}}
  end

  @spec put(%KV{}, any(), any(), any()) :: %KV{}
  def put(kv, key, context, value) do
    #TODO
    # Put the given key
    # Replace the current context of the sender in the vector clock with the correct version
    # i.e if key = foo, value = 200, sender = :a, current_Version = 4, context = [(:a, 1)]
    # then new put should be context = [(:a, 5)]
    if Map.has_key?(kv.db, key) do
      #TODO Update Context Using Vector Clock
      entry = Map.get(kv.db, key)
      %{kv | db: Map.put(kv.db, key, %{entry | value: value, context: context})}
    else
      #TODO put new
      entry = DynamoNode.Entry.new(key, context, value)
      %{kv | db: Map.put_new(kv.db, key, entry)}
    end
  end

  @spec put(%KV{}, atom(), integer(), any(), map(), any()) :: %KV{}
  def put(kv, proc, version, key, context, value) do
    if Map.has_key?(kv.db, key) do
      #TODO Update Context Using Vector Clock
      context = VectorClock.update_vector_clock(proc, context, version)
      entry = Map.get(kv.db, key)
      %{kv | db: Map.put(kv.db, key, %{entry | value: value, context: context})}
    else
      #TODO put new
      context = %{}
      context = VectorClock.update_vector_clock(proc, context, version)
      entry = DynamoNode.Entry.new(key, context, value)
      %{kv | db: Map.put_new(kv.db, key, entry)}
    end
  end

  @spec get(%DynamoNode.KV{db: Map}, any()) :: %DynamoNode.Entry{} | :noentry
  def get(kv, key) do
    #TODO
    # Return Key for the given value
    Map.get(kv.db, key, :noentry)
  end
end

defmodule DynamoNode.PutEntry do

  @moduledoc """
  PutEntryReuest to replicate key value request to other nodes
  This is intiated by the corrdinator node
  """
  alias __MODULE__

  @enforce_keys [:context, :key, :value, :client]
  defstruct(
    context: nil, # Vector Clock
    value: nil,
    key: nil,
    client: nil
  )

  def new(key, context, value, client) do
    %PutEntry{context: context, value: value, key: key, client: client}
  end
end

defmodule DynamoNode.PutEntryResponse do

  @moduledoc """
  PutEntryReply by the replication node to the coordinator node.
  """
  alias __MODULE__

  @enforce_keys [:key, :ack, :client]
  defstruct(
    ack: nil, # :ok
    key: nil,
    client: nil
  )

  def new(key, ack, client) do
    %PutEntryResponse{ack: ack, key: key, client: client}
  end
end
defmodule DynamoNode.GetEntry do
  alias __MODULE__

  @moduledoc """
    Get entry request to get value from the replicated nodes.
    This is intiated by the coordinator node for quorum based protocol.
  """
  @enforce_keys [:key, :client]
  defstruct(
    key: nil,
    client: nil
  )

  def new(key, client) do
    %GetEntry{key: key, client: client}
  end
end


defmodule DynamoNode.GetEntryResponse do

  @moduledoc """
  GetEntryResponse by the replication node to the coordinator node.
  """
  alias __MODULE__

  @enforce_keys [:key, :client, :entry]
  defstruct(
    key: nil, # :ok
    client: nil,
    entry: nil
  )

  def new(key, client, entry) do
    %GetEntryResponse{key: key, client: client, entry: entry}
  end
end

defmodule DynamoNode.ShareStateRequest do

  @moduledoc """
  Share State Request. Useful for gossip protocol.
  This shoudl share a ring.
  """

  alias __MODULE__

  @enforce_keys [:state]
  defstruct(
    state: nil
  )

  @spec new(%Ring{}) :: %ShareStateRequest{}
  def new(state) do
    %ShareStateRequest{state: state}
  end
end

defmodule DynamoNode.ShareStateResponse do

  @moduledoc """
  Share State Response. To be used for gossip protocol
  """

  alias __MODULE__

  @enforce_keys [:state]
  defstruct(
    state: nil
  )

  @spec new(%Ring{}) :: %ShareStateResponse{state: %Ring{}}
  def new(state) do
    %ShareStateResponse{state: state}
  end
end

defmodule DynamoNode.IndirectProbe do

  @moduledoc """
  Indirect Probe Request for Gossip Protocol
  """
  alias __MODULE__

  @enforce_keys [:node, :incarnation, :requestee]
  defstruct(
    node: nil,
    incarnation: nil,
    requestee: nil
  )

  def new(node, incarnation, requestee) do
    %IndirectProbe{node: node, incarnation: incarnation, requestee: requestee}
  end
end

defmodule DynamoNode.IndirectProbeResponse do

  @moduledoc """
  Indirect Probe Request for Gossip Protocol
  """
  alias __MODULE__

  @enforce_keys [:node, :incarnation, :status, :requestee]
  defstruct(
    node: nil,
    incarnation: nil,
    status: nil,
    requestee: nil
  )

  def new(node, incarnation, status, requestee) do
    %IndirectProbeResponse{node: node, incarnation: incarnation, status: status, requestee: requestee}
  end
end

defmodule DynamoNode.SuspectNode do

  @moduledoc """
  Indirect Probe Request for Gossip Protocol
  """
  alias __MODULE__

  @enforce_keys [:node, :incarnation]
  defstruct(
    node: nil,
    incarnation: nil,
  )

  def new(node, incarnation) do
    %SuspectNode{node: node, incarnation: incarnation}
  end
end

defmodule DynamoNode.NodeAlive do

  @moduledoc """
  Indirect Probe Request for Gossip Protocol
  """
  alias __MODULE__

  @enforce_keys [:node, :incarnation]
  defstruct(
    node: nil,
    incarnation: nil,
  )

  def new(node, incarnation) do
    %NodeAlive{node: node, incarnation: incarnation}
  end
end

defmodule DynamoNode.NodeFailed do

  @moduledoc """
  Indirect Probe Request for Gossip Protocol
  """
  alias __MODULE__

  @enforce_keys [:node]
  defstruct(
    node: nil,
  )

  def new(node) do
    %NodeFailed{node: node}
  end
end
