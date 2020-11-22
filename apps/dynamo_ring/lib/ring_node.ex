defmodule Ring.RingNode do
  @moduledoc """
  Ring Node for The consistent hashing ring
  """
  alias __MODULE__

  defstruct(
    key: nil,
    data: nil
  )

  @spec new(any) :: %RingNode{}
  def new(node) do
    %RingNode{key: node, data: %{}}
  end

end
