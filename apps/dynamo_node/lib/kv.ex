defmodule Raft.KV do
  @moduledoc """
  Response for RequestVote requests.
  """
  alias __MODULE__
  defstruct(
    db: Map
  )

  def new() do
    %KV{db: Map.new()}
  end

  def put(key, context, value) do
    #TODO
    # Put the given key
  end

  def get(key) do
    #TODO
    # Return Key for the given value
  end
end

defmodule Raft.Entry do
  @moduledoc """
  Key Value Pair Message
  """
  alias __MODULE__

  @enforce_keys [:context, :key, :value]
  defstruct(
    context: nil, # Vector Clock
    value: nil,
    key: nil
  )

  @doc """
  Create a new RequestVoteResponse.
  """
  #@spec new(key, context, value) :: %Entry{}
  def new(key, context, value) do
    %Entry{context: context, value: value, key: key}
  end
end

defmodule Raft.PutEntryRequest do

  @moduledoc """
  PutEntryReuest to replicate key value request to other nodes
  This is intiated by the corrdinator node
  """
  alias __MODULE__

  @enforce_keys [:context, :key, :value]
  defstruct(
    context: nil, # Vector Clock
    value: nil,
    key: nil
  )

  def new(key, context, value) do
    %PutEntryRequest{context: context, value: value, key: key}
  end
end

defmodule Raft.PutEntryReply do

  @moduledoc """
  PutEntryReply by the replication node to the coordinator node.
  """
  alias __MODULE__

  @enforce_keys [:key, :value, :ack]
  defstruct(
    ack: nil, # :ok
    value: nil,
    key: nil
  )

  def new(key, value, ack) do
    %PutEntryReply{ack: ack, value: value, key: key}
  end
end

defmodule Raft.GetEntryReply do

  @moduledoc """
  PutEntryReply by the replication node to the coordinator node.
  """
  alias __MODULE__

  @enforce_keys [:key, :value, :ack]
  defstruct(
    ack: nil, # :ok
    value: nil,
    key: nil
  )

  def new(key, value, ack) do
    %GetEntryReply{ack: ack, value: value, key: key}
  end
end

defmodule Raft.GetEntryRequest do
  alias __MODULE__

  @moduledoc """
    Get entry request to get value from the replicated nodes.
    This is intiated by the coordinator node for quorum based protocol.
  """
  @enforce_keys [:key]
  defstruct(
    key: nil
  )

  def new(key) do
    %GetEntryRequest{key: key}
  end
end

defmodule Raft.ShareStateRequest do

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

defmodule Raft.ShareStateResponse do

  @moduledoc """
  Share State Response. To be used for gossip protocol
  """

  alias __MODULE__

  @enforce_keys [:state]
  defstruct(
    state: nil
  )

  @spec new(%Ring{}) :: %ShareStateResponse{}
  def new(state) do
    %ShareStateResponse{state: state}
  end
end
