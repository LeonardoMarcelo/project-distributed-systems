"""CLI da aplicacao cliente: a camada mais de cima do sistema.

Note o que NAO aparece em nenhum comando deste arquivo: endereco de worker,
quantidade de workers, nome de fila, nome de exchange, qual replica de
coordenador esta no comando. O cliente conhece um endereco de servico de nomes e
mais nada — e dai que vem a transparencia de acesso e de localizacao do item A.

Roda dentro de um conteiner (`docker compose run --rm client ...`, ou o atalho
`./minigrid`), porque precisa de `grpcio`, que tem extensao compilada. O unico
processo do sistema fora do Docker e o worker do host, e ele usa somente a
biblioteca padrao.
"""
import argparse
import json
import sys
import time

from minigrid import recipes
from minigrid.client import MiniGridClient, NoCoordinator, TaskFailed


def _client(args):
    return MiniGridClient(consul_url=args.consul)


def cmd_submit(args):
    client = _client(args)
    data = b""
    if args.input:
        with open(args.input, "rb") as handle:
            data = handle.read()

    params = {}
    for item in args.param or []:
        if "=" not in item:
            raise SystemExit("--param espera chave=valor, recebi: " + item)
        key, value = item.split("=", 1)
        params[key] = value
    if args.steps:
        params["steps"] = str(args.steps)

    code = None
    if args.code:
        with open(args.code, "r") as handle:
            code = handle.read()

    task_id = client.submit(args.recipe, data=data, params=params, code=code)
    print(task_id)

    if args.wait:
        return _follow(client, task_id, args.output)
    return 0


def _follow(client, task_id, output=None):
    for event in client.watch(task_id):
        linha = "  " + event["kind"]
        if event["worker_id"]:
            linha += " em " + event["worker_id"] + " (" + event["arch"] + "/" \
                     + event["virt"] + ")"
        if event["step"]:
            linha += " passo " + str(event["step"])
        if event["reason"]:
            linha += " motivo=" + event["reason"]
        if event["error"]:
            linha += " erro=" + event["error"]
        print(linha, file=sys.stderr, flush=True)
        if event["kind"] == "failed":
            print("tarefa falhou: " + event["error"], file=sys.stderr)
            return 1

    blob = client.result(task_id)
    if output:
        with open(output, "wb") as handle:
            handle.write(blob)
        print("resultado gravado em " + output + " (" + str(len(blob)) + " bytes)",
              file=sys.stderr)
    else:
        sys.stdout.buffer.write(blob)
        sys.stdout.buffer.write(b"\n")
    return 0


def cmd_status(args):
    print(json.dumps(_client(args).status(args.task_id), indent=2,
                     ensure_ascii=False))
    return 0


def cmd_result(args):
    blob = _client(args).result(args.task_id)
    if args.output:
        with open(args.output, "wb") as handle:
            handle.write(blob)
        print("gravado em " + args.output + " (" + str(len(blob)) + " bytes)")
    else:
        sys.stdout.buffer.write(blob)
    return 0


def cmd_watch(args):
    return _follow(_client(args), args.task_id, args.output)


def cmd_workers(args):
    info = _client(args).workers()
    print("coordenador lider: " + (info["leader"] or "desconhecido"))
    if not info["workers"]:
        print("nenhum worker registrado no servico de nomes")
        return 0
    print("")
    print("%-26s %-10s %-12s %-9s %s" % ("ID", "ARQUITETURA", "VIRTUALIZACAO",
                                         "ESTADO", "CAPACIDADES"))
    for worker in info["workers"]:
        print("%-26s %-10s %-12s %-9s %s" % (
            worker["id"], worker["arch"] or "?", worker["virt"] or "?",
            worker["status"], ",".join(worker["capabilities"]) or "-",
        ))
    return 0


def cmd_recipes(args):
    for name in sorted(recipes.CATALOG):
        entry = recipes.CATALOG[name]
        exige = ", ".join(entry["requires"]) or "nada"
        print(name)
        print("  " + entry["description"])
        print("  exige: " + exige)
        print("  parametros: " + json.dumps(entry["default_params"]))
    return 0


def cmd_bench(args):
    """Submete N tarefas e mede onde cada uma foi executada.

    Serve a tabela de heterogeneidade do relatorio: a distribuicao entre
    arquiteturas e tipos de virtualizacao sai daqui, nao de estimativa.
    """
    client = _client(args)
    ids = [client.submit(args.recipe, params={"steps": str(args.steps)})
           for _ in range(args.count)]
    print("submetidas " + str(len(ids)) + " tarefas", file=sys.stderr)

    por_ambiente = {}
    pendentes = set(ids)
    inicio = time.time()
    while pendentes and (time.time() - inicio) < args.timeout:
        for task_id in list(pendentes):
            info = client.status(task_id)
            if info["state"] in ("done", "failed"):
                chave = (info["arch"] or "?") + "/" + (info["virt"] or "?") \
                        + "/" + (info["worker_id"] or "?")
                por_ambiente[chave] = por_ambiente.get(chave, 0) + 1
                pendentes.discard(task_id)
        if pendentes:
            time.sleep(1)

    print("")
    print("%-45s %s" % ("ARQUITETURA/VIRTUALIZACAO/WORKER", "TAREFAS"))
    for chave in sorted(por_ambiente):
        print("%-45s %d" % (chave, por_ambiente[chave]))
    if pendentes:
        print("")
        print(str(len(pendentes)) + " tarefas nao terminaram em "
              + str(args.timeout) + "s", file=sys.stderr)
        return 1
    return 0


def build_parser():
    parser = argparse.ArgumentParser(
        prog="minigrid",
        description="Cliente do MiniGrid. Nenhum comando recebe endereco de "
                    "worker ou de coordenador: tudo vem do servico de nomes.",
    )
    parser.add_argument("--consul", default=None,
                        help="URL do Consul (padrao: $MINIGRID_CONSUL)")
    sub = parser.add_subparsers(dest="comando", required=True)

    p = sub.add_parser("submit", help="submete uma tarefa")
    p.add_argument("recipe", help="nome da receita (veja `recipes`)")
    p.add_argument("--input", help="arquivo com os dados de entrada")
    p.add_argument("--output", help="arquivo para gravar o resultado")
    p.add_argument("--param", action="append", help="chave=valor, repetivel")
    p.add_argument("--steps", type=int, help="atalho para --param steps=N")
    p.add_argument("--code", help="arquivo .py com codigo proprio, no lugar da receita")
    p.add_argument("--wait", action="store_true",
                   help="acompanha por push ate concluir")
    p.set_defaults(func=cmd_submit)

    p = sub.add_parser("status", help="consulta o estado de uma tarefa")
    p.add_argument("task_id")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("result", help="obtem o resultado de uma tarefa")
    p.add_argument("task_id")
    p.add_argument("--output")
    p.set_defaults(func=cmd_result)

    p = sub.add_parser("watch", help="acompanha as transicoes por push")
    p.add_argument("task_id")
    p.add_argument("--output")
    p.set_defaults(func=cmd_watch)

    p = sub.add_parser("workers", help="lista os workers do servico de nomes")
    p.set_defaults(func=cmd_workers)

    p = sub.add_parser("recipes", help="lista o catalogo de codigo movel")
    p.set_defaults(func=cmd_recipes)

    p = sub.add_parser("bench", help="submete N tarefas e mostra onde rodaram")
    p.add_argument("--recipe", default="pi")
    p.add_argument("--count", type=int, default=12)
    p.add_argument("--steps", type=int, default=3)
    p.add_argument("--timeout", type=int, default=180)
    p.set_defaults(func=cmd_bench)

    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except NoCoordinator as exc:
        print("erro: " + str(exc), file=sys.stderr)
        return 2
    except TaskFailed as exc:
        print("erro: " + str(exc), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
