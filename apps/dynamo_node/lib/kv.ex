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
  Create a new RequestVoteResponse.
  """
  #@spec new(key, context, value) :: %Entry{}
  def new(key, context, value) do
    %Entry{context: context, value: value, key: key}
  end
end

defmodule Raft.PutEntry do
  alias __MODULE__

  alias __MODULE__

  @enforce_keys [:context, :key, :value]
  defstruct(
    context: nil, # Vector Clock
    value: nil,
    key: nil
  )

  def new(key, context, value) do
    %PutEntry{context: context, value: value, key: key}
  end
end

defmodule Raft.GetEntry do
  alias __MODULE__

  alias __MODULE__

  @enforce_keys [:key]
  defstruct(
    key: nil
  )

  def new(key) do
    %GetEntry{key: key}
  end
end
