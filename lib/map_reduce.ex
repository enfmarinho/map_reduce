defmodule MapReduce do
  def number_of_native_threads do
    System.schedulers()
    # System.schedulers_online()
  end

  def concatena([], curr),do: curr 
  def concatena([head | tail], curr) do
    concatena(tail, head ++ curr)
  end

  # DIVIDIR DATASET

  # Divide a lista de termos em uma lista de listas
  def formar_listas(list, num) when num >= length(list) do
    [list]
  end
  def formar_listas(list, num) do
    nova_list =
      list
      |> Enum.split(num)
    [nova_list |> elem(0)] ++ (nova_list |> elem(1) |> formar_listas(num))
  end

  defp split_lista(list) do
    multi = IO.gets("Digite o multiplicador: ")
     |> String.replace("\n", "")   # Remove o '\n'
     |> String.to_integer
    num = number_of_native_threads() * multi
    formar_listas(list, num)
  end

  # Essa funcao é usada para o caso de contagem de palavras
  def dividir_dataset(file_path)  do
    file_path
    |> File.read()
    |> elem(1)
    |> String.replace([",", ";", ".", ":", "(", ")"], "") # Tira pontuação
    |> String.split(" ")
  end

  defp master(list, fun_map, fun_reduce) do


    # JOGAR NOS MAPS

    # ENviar para os escravos

    # TERMINOU?

    # Receber da fila de mensagens dos escravos
    # Juntar as particoes numa lista

    # SHUFFLE
    # Ordenar pela chaves

    # TERMINOU O SHUFFLE?
    # REDUCE
    # Manda os escravos realizarem os reduces
  end

  def recebe_map([], fun) do
    []
  end
  def recebe_map([h|t], fun) do
    [fun.(h)] ++ recebe_map(t, fun)
  end

  # 'acc' é o valor neutro da operacao
  def recebe_reduce([], _, acc) do
    acc
  end
  def recebe_reduce(list,fun, _) when length(list) == 1 do
    hd(list)
  end
  def recebe_reduce(list, fun, acc) when length(list) >= 2 do
    # Separa os dois primeiros elementos da lista
    list1 = list |> Enum.split(2) |> elem(0)
    list2 = list |> Enum.split(2) |> elem(1)
    fun.(Enum.at(list, 0), Enum.at(list, 1)) |> fun.(recebe_reduce(list2, fun, acc))
  end

  defp recebe_threads_map() do
    # recebe threads da map
  end

  def map_gerence(_, []) do
    :ok
  end
  def map_gerence(fun_map, [h|t]) do
      spawn(MapReduce, :concurrent_map, [h, fun_map, self])
      map_gerence(fun_map, t)
  end

  def concurrent_map(list ,fun_map, pid) do
    send pid, recebe_map(list, fun_map)
  end

  defp shuffle(list) when is_list(list) do

  end
  # == Essa seria a função pública principal
  # Chamada para o caso geral (com lista)
  def main(list, map_func, reduce_func) when is_list(list) do
    list
    |> split_lista()
    |> master(map_func, reduce_func)
  # |> saida()
  end
  # Chamada recebendo o caminho de um arquivo (para o caso de fazer a contagem de palavras)
  def main(file_path \\ "test.txt", map_func, reduce_func) do
    dividir_dataset(file_path)
    |> split_lista
  end

end
