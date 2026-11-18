"""Catalogo de codigo movel.

Estes arquivos `.py` NAO sao importados pelo worker. O cliente le o *fonte* e o
manda dentro do envelope; o worker recebe o texto, instancia e executa na sua VM
de processo. E a mobilidade fraca do item E — se o worker importasse estes
modulos, nao haveria mobilidade nenhuma, so uma chamada local.

Cada receita implementa um unico contrato:

    def step(state, data, api) -> (novo_state, concluido, resultado_ou_None)

e precisa ser *deterministica dado o state*, porque uma reentrega reexecuta a
partir do ultimo checkpoint. As receitas so podem importar o que a lista branca
da VM de processo permite (ver worker/_runner.py: ALLOWED_IMPORTS).

Stdlib pura.
"""
import os

_HERE = os.path.dirname(os.path.abspath(__file__))


class UnknownRecipe(Exception):
    """Nome de receita que nao esta no catalogo."""


#: O que existe, o que cada uma exige do worker, e seus parametros padrao.
#: `requires` e o que o coordenador usa para rotear: uma tarefa que exige
#: `legacy:imagemagick` vai para a fila que so o worker legado consome.
CATALOG = {
    "pi": {
        "file": "pi.py",
        "requires": [],
        "default_params": {"steps": "20", "per_step": "5000"},
        "description": "Monte Carlo de pi em varios passos; e a tarefa da demonstracao "
                       "de migracao, porque dura o suficiente para ser interrompida",
    },
    "compress": {
        "file": "compress.py",
        "requires": [],
        "default_params": {"level": "6"},
        "description": "compressao zlib de um blob em um unico passo; tarefa curta, "
                       "usada para encher a fila nos testes de persistencia",
    },
    "img.filter": {
        "file": "img_filter.py",
        "requires": ["legacy:imagemagick"],
        "default_params": {"resize": "64x64"},
        "description": "redimensiona uma imagem chamando o ImageMagick pelo wrapper "
                       "de programa legado",
    },
}


def load(name):
    """Devolve o codigo-fonte da receita, como texto."""
    entry = CATALOG.get(name)
    if entry is None:
        raise UnknownRecipe(
            "receita desconhecida: " + repr(name)
            + "; disponiveis: " + ", ".join(sorted(CATALOG))
        )
    with open(os.path.join(_HERE, entry["file"]), "rb") as handle:
        return handle.read().decode("utf-8")


def params_for(name, overrides=None):
    """Parametros padrao da receita, com as sobreposicoes do cliente aplicadas."""
    entry = CATALOG.get(name)
    if entry is None:
        raise UnknownRecipe("receita desconhecida: " + repr(name))
    merged = dict(entry["default_params"])
    merged.update({str(k): str(v) for k, v in (overrides or {}).items()})
    return merged


def requires_for(name):
    entry = CATALOG.get(name)
    if entry is None:
        raise UnknownRecipe("receita desconhecida: " + repr(name))
    return list(entry["requires"])
