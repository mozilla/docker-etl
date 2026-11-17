"""Resolver._guarded: a result computed while something it reads is still
being computed is cached only until that finishes."""

from metric_usage import lineage as L


def test_reader_of_a_provisional_result_is_provisional():
    r = L.Resolver({}, "p")
    g = r._guarded

    def a():
        g("k", lambda: {"k": g("a", lambda: "never")})   # k reads a, half-built
        g("j", lambda: {"j": g("k", lambda: "recomputed")})  # j reads k from memo
        assert "k" in r.memo and "j" in r.memo
        return "a done"

    g("a", a)
    # both read a before it was done, so neither is kept once it is
    assert "k" not in r.memo
    assert "j" not in r.memo
    assert r.memo["a"] == "a done"
    assert g("j", lambda: "fresh") == "fresh"


def test_result_that_read_a_finished_key_is_kept():
    r = L.Resolver({}, "p")
    g = r._guarded

    def outer():
        g("inner", lambda: g("outer", lambda: "never"))  # inner reads outer, half-built
        return "outer done"

    g("outer", outer)
    # outer read inner after inner finished, so outer itself is complete
    assert r.memo["outer"] == "outer done"
    assert r.pending == {} and r.waiting == {}
