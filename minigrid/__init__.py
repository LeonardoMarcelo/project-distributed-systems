# Vazio de proposito.
#
# `minigrid.envelope`, `minigrid.naming` e `minigrid.stomp` precisam importar
# usando apenas a biblioteca padrao, porque o worker do host (item G) roda fora
# do Docker, na maquina do avaliador, sem nada instalado. Qualquer import pesado
# aqui vazaria para esses modulos e quebraria essa garantia.
#
# O teste tests/test_purity.py vigia isso.
