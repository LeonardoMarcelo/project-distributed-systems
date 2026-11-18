# Codigo movel: estimativa de pi por Monte Carlo, em varios passos.
#
# E a tarefa da demonstracao de migracao (item E) porque dura o suficiente para
# ser interrompida no meio: `steps` passos de `per_step` amostras cada.
#
# A exigencia dificil nao e o calculo, e o determinismo. O executor retoma do
# ultimo checkpoint, e a fila entrega ao-menos-uma-vez — entao o passo numero k
# pode ser executado mais de uma vez, possivelmente em arquiteturas diferentes.
# Se o gerador fosse semeado pelo relogio ou pelo estado global do `random`, a
# mesma tarefa daria respostas diferentes dependendo de onde foi interrompida, e
# "retomar" deixaria de significar coisa alguma.
#
# Por isso a semente vem do *numero do passo*: executar o passo k a partir do
# mesmo checkpoint produz exatamente as mesmas amostras, em qualquer worker.
import json
import random


def step(state, data, api):
    total_steps = int(api.params.get("steps", "20"))
    per_step = int(api.params.get("per_step", "5000"))

    n = int(state.get("n", 0))
    inside = int(state.get("inside", 0))
    total = int(state.get("total", 0))

    rng = random.Random(1000003 * (n + 1))
    for _ in range(per_step):
        x = rng.random()
        y = rng.random()
        if x * x + y * y <= 1.0:
            inside += 1
    total += per_step
    n += 1

    done = n >= total_steps
    result = None
    if done:
        result = json.dumps({
            "pi": 4.0 * inside / total,
            "inside": inside,
            "total": total,
            "steps": n,
        }, sort_keys=True).encode("utf-8")

    return {"n": n, "inside": inside, "total": total}, done, result
