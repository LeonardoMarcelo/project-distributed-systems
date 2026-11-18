"""Testes das receitas de codigo movel e do wrapper do programa legado.

As receitas sao o *codigo* da mobilidade fraca: o cliente le o `.py`, manda o
fonte no envelope, e o worker instancia e executa. Nenhuma delas e importada
pelo worker — se fossem, nao haveria mobilidade nenhuma, so uma chamada local.
"""
import base64
import json
import shutil

import pytest

from minigrid import recipes
from worker import executor, legacy, sandbox

from test_executor import FakeTransport, env

L = sandbox.Limits(cpu_seconds=20, wall_seconds=60)

# PNG 1x1 valido, embutido para o teste nao depender de arquivo externo.
SAMPLE_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8DwHwAFBQIA"
    "X8jx0gAAAABJRU5ErkJggg=="
)

has_imagemagick = pytest.mark.skipif(
    not any(shutil.which(b) for b in ("convert", "magick")),
    reason="ImageMagick ausente nesta maquina; existe no conteiner legado e no CI",
)


@pytest.fixture
def fake():
    return FakeTransport()


# ----------------------------------------------------------------- catalogo

def test_catalog_lists_the_three_recipes():
    assert set(recipes.CATALOG) == {"pi", "compress", "img.filter"}


def test_load_returns_source_code_not_a_module():
    source = recipes.load("pi")
    assert isinstance(source, str) and "def step(" in source


def test_load_of_unknown_recipe_raises():
    with pytest.raises(recipes.UnknownRecipe):
        recipes.load("nao-existe")


def test_only_img_filter_requires_the_legacy_capability():
    assert recipes.CATALOG["img.filter"]["requires"] == ["legacy:imagemagick"]
    assert recipes.CATALOG["pi"]["requires"] == []
    assert recipes.CATALOG["compress"]["requires"] == []


def test_recipes_do_not_import_anything_outside_the_sandbox_allowlist():
    """Uma receita que importasse `os` seria barrada em execucao, nao aqui.
    Este teste pega o erro na hora de escrever a receita, nao na demo."""
    from worker import _runner

    for name in recipes.CATALOG:
        for line in recipes.load(name).splitlines():
            stripped = line.strip()
            if stripped.startswith("import "):
                module = stripped.split()[1].split(".")[0]
                assert module in _runner.ALLOWED_IMPORTS, (name, module)


# ---------------------------------------------------------------- receita pi

def test_pi_converges_within_tolerance(fake):
    executor.run_task(
        env(recipes.load("pi"), params={"steps": "20", "per_step": "5000"}),
        fake, "w1", L, checkpoint_every=5,
    )
    assert fake.results, fake.last("failed")
    assert abs(json.loads(fake.results[0][0])["pi"] - 3.14159) < 0.05


def test_pi_is_deterministic_given_the_same_state():
    """Exigencia do executor: uma reentrega reexecuta a partir do checkpoint,
    entao `step` precisa ser deterministica dado o state."""
    state = {"n": 3, "inside": 10, "total": 20}
    a = sandbox.run_step(recipes.load("pi"), dict(state), b"", L, params={"per_step": "100"})
    b = sandbox.run_step(recipes.load("pi"), dict(state), b"", L, params={"per_step": "100"})
    assert a.error is None and a.state == b.state


def test_pi_resumed_midway_gives_the_same_answer_as_uninterrupted(fake):
    """O coracao do item E: migrar no meio nao pode mudar o resultado."""
    params = {"steps": "8", "per_step": "2000"}

    direto = FakeTransport()
    executor.run_task(env(recipes.load("pi"), params=params), direto, "w1", L)

    cedido = FakeTransport()
    assert executor.run_task(env(recipes.load("pi"), params=params), cedido, "w1", L,
                             should_stop=_stop_after(3)) == "yielded"
    assert cedido.checkpoints["t1"]["n"] == 3
    # Outro worker retoma do checkpoint que ficou gravado.
    assert executor.run_task(env(recipes.load("pi"), params=params), cedido, "w2", L) == "done"

    assert json.loads(direto.results[0][0]) == json.loads(cedido.results[0][0])
    assert cedido.results[0][1]["worker_id"] == "w2"
    assert cedido.results[0][1]["resumed_from_step"] == 3


def _stop_after(n):
    box = {"c": 0}

    def predicate():
        box["c"] += 1
        return box["c"] >= n

    return predicate


# ---------------------------------------------------------- receita compress

def test_compress_roundtrips(fake):
    import zlib

    executor.run_task(env(recipes.load("compress"), data=b"x" * 10000,
                          recipe="compress"), fake, "w1", L)
    assert fake.results, fake.last("failed")
    assert zlib.decompress(fake.results[0][0]) == b"x" * 10000


def test_compress_reports_the_ratio_in_the_state(fake):
    executor.run_task(env(recipes.load("compress"), data=b"z" * 10000,
                          recipe="compress"), fake, "w1", L)
    assert fake.results[0][1]["steps"] == 1


# ----------------------------------------------- wrapper do programa legado

@has_imagemagick
def test_resolve_imagemagick_finds_an_invocation():
    argv = legacy.resolve_imagemagick()
    assert argv and argv[0] in ("convert", "magick")


@has_imagemagick
def test_run_legacy_converts_an_image(tmp_path):
    out = legacy.run_legacy(
        "imagemagick", ["png:-", "-resize", "32x32", "png:-"],
        stdin=SAMPLE_PNG, workdir=str(tmp_path),
    )
    assert out[:8] == b"\x89PNG\r\n\x1a\n"


def test_run_legacy_raises_with_stderr_on_failure(tmp_path):
    with pytest.raises(legacy.LegacyFailed) as exc:
        legacy.run_legacy("false", [], stdin=b"", workdir=str(tmp_path))
    assert exc.value.returncode != 0
    assert isinstance(exc.value.stderr, str)


def test_run_legacy_raises_when_the_program_is_absent(tmp_path):
    with pytest.raises(legacy.LegacyFailed) as exc:
        legacy.run_legacy("nao-existe-este-binario", [], workdir=str(tmp_path))
    assert "nao-existe-este-binario" in str(exc.value)


def test_run_legacy_never_uses_a_shell(tmp_path):
    """Argumento com metacaractere e tratado como dado, nao como sintaxe."""
    out = legacy.run_legacy("echo", ["a; rm -rf /tmp/naoexiste"], workdir=str(tmp_path))
    assert b"; rm -rf" in out


@has_imagemagick
def test_img_filter_recipe_runs_through_the_sandbox_api(fake):
    """O wrapper chega ao codigo movel como chamada de biblioteca (item A)."""
    executor.run_task(
        env(recipes.load("img.filter"), data=SAMPLE_PNG, recipe="img.filter",
            params={"resize": "32x32"}, requires=["legacy:imagemagick"]),
        fake, "w1", L,
    )
    assert fake.results, fake.last("failed")
    assert fake.results[0][0][:8] == b"\x89PNG\r\n\x1a\n"


def test_img_filter_fails_readably_without_the_legacy_capability(fake):
    """Tarefa roteada para um worker sem a capacidade falha com mensagem."""
    executor.run_task(
        env(recipes.load("img.filter"), data=SAMPLE_PNG, recipe="img.filter",
            params={"resize": "32x32"}, requires=[]),
        fake, "w1", L,
    )
    assert fake.last("failed") is not None
    assert "legacy" in fake.last("failed")["error"].lower()
