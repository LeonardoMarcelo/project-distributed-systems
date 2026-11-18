# Codigo movel: redimensiona uma imagem chamando o programa legado.
#
# Esta receita e a prova do wrapper do item A. Ela nao sabe que existe um
# ImageMagick, nem se o binario e `convert` (v6) ou `magick` (v7), nem que ha um
# subprocesso envolvido. Para ela, `api.run_legacy` e uma chamada de biblioteca
# que recebe bytes e devolve bytes — "um servico como qualquer outro".
#
# A tarefa declara `requires: ["legacy:imagemagick"]`, e o coordenador a roteia
# para a fila que so o worker legado consome. Num worker sem a capacidade,
# `api.run_legacy` levanta erro e a tarefa falha com mensagem, em vez de
# executar errado em silencio.


def step(state, data, api):
    resize = api.params.get("resize", "64x64")
    out = api.run_legacy(
        "imagemagick",
        ["png:-", "-resize", resize, "png:-"],
        stdin=data,
    )
    return (
        {"n": 1, "entrada": len(data), "saida": len(out), "resize": resize},
        True,
        out,
    )
