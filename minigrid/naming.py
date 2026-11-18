"""Cliente do servico de nomes e coordenacao (item F), sobre a API HTTP do Consul.

Tres papeis, todos pedidos pelo enunciado:

1. *Registro de servicos* — nenhum worker tem endereco fixo; ao subir, cada um se
   registra aqui com um health check por TTL. Parar de bater o heartbeat faz o
   Consul marcar o servico como critico e, depois de
   `DeregisterCriticalServiceAfter`, removê-lo.
2. *Eleicao de lider* — sessao com TTL mais lock numa chave do KV. Quem adquire a
   chave e o lider; se a sessao expira ou e destruida, a chave e liberada e outro
   assume (ver coordinator/election.py).
3. *Estado das tarefas* — metadados, checkpoints e a posse de cada tarefa.

Escrito com `urllib.request` em vez de uma biblioteca de cliente porque este
modulo tambem roda no worker do host, fora do Docker, onde nada pode ser
instalado. Stdlib pura (ver minigrid/__init__.py).
"""
import base64
import json
import urllib.error
import urllib.parse
import urllib.request


class ConsulError(Exception):
    """Resposta inesperada do Consul."""


class Consul(object):
    def __init__(self, base_url="http://localhost:8500", timeout=15):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    # ---------------------------------------------------------------- interno

    def _request(self, method, path, body=None, timeout=None, raw_body=False):
        """Devolve (status, headers, bytes). Nao levanta em 404."""
        data = None
        if body is not None:
            data = body if raw_body else json.dumps(body).encode("utf-8")
        req = urllib.request.Request(self.base_url + path, data=data, method=method)
        if data is not None and not raw_body:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=timeout or self.timeout) as resp:
                return resp.status, dict(resp.headers), resp.read()
        except urllib.error.HTTPError as exc:
            payload = exc.read()
            if exc.code == 404:
                return 404, dict(exc.headers), payload
            raise ConsulError(
                method + " " + path + " -> HTTP " + str(exc.code) + ": "
                + payload.decode("utf-8", "replace")
            )

    def _json(self, method, path, body=None, timeout=None):
        status, _, payload = self._request(method, path, body, timeout)
        if status == 404 or not payload:
            return None
        return json.loads(payload.decode("utf-8"))

    @staticmethod
    def _quote(key):
        return urllib.parse.quote(key, safe="/")

    # ------------------------------------------------- registro de servicos

    def register_service(self, service_id, name, tags, ttl_seconds,
                         deregister_after="30s", meta=None):
        """Registra o servico no agente local com health check por TTL.

        O check nasce `critical` e so vira `passing` no primeiro `pass_check` —
        comportamento do Consul, nao nosso: um worker que registra e morre antes
        do primeiro heartbeat nunca e considerado saudavel.
        """
        payload = {
            "ID": service_id,
            "Name": name,
            "Tags": list(tags or []),
            "Check": {
                "CheckID": "service:" + service_id,
                "Name": "ttl de " + service_id,
                "TTL": str(ttl_seconds) + "s",
                "DeregisterCriticalServiceAfter": deregister_after,
            },
        }
        if meta:
            payload["Meta"] = {str(k): str(v) for k, v in meta.items()}
        self._request("PUT", "/v1/agent/service/register", payload)

    def pass_check(self, service_id):
        """Heartbeat: renova o TTL do health check do servico."""
        self._request("PUT", "/v1/agent/check/pass/service:" + self._quote(service_id))

    def deregister_service(self, service_id):
        self._request("PUT", "/v1/agent/service/deregister/" + self._quote(service_id))

    def list_services(self, name):
        """Servicos registrados com esse nome, cada um com ID, Tags, Meta e Status.

        Usa os endpoints do *agente* (nao do catalogo) porque sao imediatamente
        consistentes: um worker que acabou de se registrar aparece na hora.
        """
        services = self._json("GET", "/v1/agent/services") or {}
        checks = self._json("GET", "/v1/agent/checks") or {}

        status_by_service = {}
        for check in checks.values():
            sid = check.get("ServiceID")
            if not sid:
                continue
            current = status_by_service.get(sid)
            incoming = check.get("Status", "critical")
            # critical vence warning, que vence passing.
            rank = {"passing": 0, "warning": 1, "critical": 2}
            if current is None or rank.get(incoming, 2) > rank.get(current, 0):
                status_by_service[sid] = incoming

        out = []
        for svc in services.values():
            if svc.get("Service") != name:
                continue
            out.append({
                "ID": svc.get("ID"),
                "Service": svc.get("Service"),
                "Tags": svc.get("Tags") or [],
                "Meta": svc.get("Meta") or {},
                "Status": status_by_service.get(svc.get("ID"), "critical"),
            })
        return out

    # ------------------------------------------------------------------- KV

    def kv_put(self, key, value, acquire=None, release=None):
        """Grava um valor. Com `acquire`, tenta tomar o lock da chave.

        Devolve True se gravou (ou adquiriu o lock), False se o lock esta com
        outra sessao. Esse False e o que decide a eleicao de lider e a posse de
        tarefa — nao e erro.
        """
        path = "/v1/kv/" + self._quote(key)
        query = []
        if acquire:
            query.append("acquire=" + urllib.parse.quote(acquire))
        if release:
            query.append("release=" + urllib.parse.quote(release))
        if query:
            path += "?" + "&".join(query)
        status, _, payload = self._request("PUT", path, value, raw_body=True)
        return payload.strip() == b"true"

    def kv_get(self, key):
        """Valor da chave em bytes, ou None se nao existe."""
        entries = self._json("GET", "/v1/kv/" + self._quote(key))
        if not entries:
            return None
        encoded = entries[0].get("Value")
        if encoded is None:
            return b""
        return base64.b64decode(encoded)

    def kv_delete(self, key, recurse=False):
        path = "/v1/kv/" + self._quote(key)
        if recurse:
            path += "?recurse=true"
        self._request("DELETE", path)

    def kv_keys(self, prefix):
        """Chaves sob o prefixo. Lista vazia se nao ha nenhuma.

        O reaper do coordenador usa isso para varrer `minigrid/tasks/*/owner` e
        descobrir tarefas cujo dono desapareceu.
        """
        keys = self._json("GET", "/v1/kv/" + self._quote(prefix) + "?keys")
        return keys or []

    def kv_watch(self, key, index, wait_seconds):
        """Consulta bloqueante: devolve (novo_index, valor_ou_None).

        O seguidor do coordenador fica aqui parado ate a chave do lider mudar.
        Com `index=0` responde na hora; com o indice da leitura anterior, so
        responde quando algo muda ou quando `wait_seconds` estoura.
        """
        path = ("/v1/kv/" + self._quote(key)
                + "?index=" + str(int(index)) + "&wait=" + str(int(wait_seconds)) + "s")
        status, headers, payload = self._request(
            "GET", path, timeout=wait_seconds + self.timeout
        )
        new_index = int(headers.get("X-Consul-Index", index) or index)
        if status == 404 or not payload:
            return new_index, None
        entries = json.loads(payload.decode("utf-8"))
        if not entries:
            return new_index, None
        encoded = entries[0].get("Value")
        return new_index, b"" if encoded is None else base64.b64decode(encoded)

    # -------------------------------------------------------------- sessoes

    def session_create(self, name, ttl_seconds, behavior="delete", lock_delay="0s"):
        """Cria a sessao usada como lock.

        `behavior=delete` apaga as chaves travadas quando a sessao morre, o que e
        o que faz o seguidor assumir sozinho. `lock_delay=0s` e deliberado: o
        padrao do Consul e 15 s, somados ao tempo de failover.
        """
        payload = {
            "Name": name,
            "TTL": str(ttl_seconds) + "s",
            "Behavior": behavior,
            "LockDelay": lock_delay,
        }
        out = self._json("PUT", "/v1/session/create", payload)
        if not out or "ID" not in out:
            raise ConsulError("criacao de sessao nao devolveu ID: " + repr(out))
        return out["ID"]

    def session_renew(self, session_id):
        out = self._json("PUT", "/v1/session/renew/" + self._quote(session_id))
        if out is None:
            raise ConsulError("sessao " + session_id + " nao existe mais")
        return out

    def session_destroy(self, session_id):
        self._request("PUT", "/v1/session/destroy/" + self._quote(session_id))

    def session_info(self, session_id):
        """Dados da sessao, ou None se ela expirou ou foi destruida."""
        entries = self._json("GET", "/v1/session/info/" + self._quote(session_id))
        return entries[0] if entries else None

    def session_list(self):
        return self._json("GET", "/v1/session/list") or []
