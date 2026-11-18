# Vazio de proposito, igual a minigrid/__init__.py.
#
# `worker.sandbox`, `worker.executor`, `worker.legacy` e `worker.registry` sao
# importados pelo worker do host, que roda fora do Docker sem nada instalado.
# Um import pesado aqui vazaria para eles. Vigiado por tests/test_purity.py.
