# Codigo movel: compressao de um blob, em um unico passo.
#
# Tarefa curta de proposito. Serve para encher a fila no teste de persistencia
# (item C): com todos os workers desligados, dezenas destas sao submetidas, e ao
# subir um worker todas precisam completar — nenhuma pode ter se perdido.
import zlib


def step(state, data, api):
    level = int(api.params.get("level", "6"))
    out = zlib.compress(data, level)
    return (
        {"n": 1, "entrada": len(data), "saida": len(out), "nivel": level},
        True,
        out,
    )
