"""Testes do sync.py (Pi). A nuvem é um mock de requests.post: nada é enviado de verdade.

Ideia que os testes protegem:
  - No Pi, mac e lote_id IDENTIFICAM o lote: agrupam as linhas, deduplicam (no worker) e
    marcam o que já foi enviado.
  - Na nuvem essa identificação não existe. O sync manda só o que é analisável
    (senderAddress, gateway_id, rssi e as medições) e descarta o resto.
  - Linha sem identificação não é sincronizada: fica pendente no Pi.

Cenários (escolha pelo nome da classe):
  python -m unittest tests.test_sync.Cenario1   # só rodando, sem dados
  python -m unittest tests.test_sync.Cenario2   # tráfego normal por 5 min + envio
  python -m unittest tests.test_sync.Cenario3   # tráfego exorbitante + envio
Tudo:
  python -m unittest tests.test_sync -v

O tempo é simulado (datetime do sync trocado por um relógio falso): 5 min aqui não levam 5 min.
"""
import copy
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import requests

import config
import sync
from config import DB_CONFIG
from core.lotes import MAX_IDADE_SYNC_MIN, LoteInvalido, preparar_lote

SCHEMA = Path(__file__).resolve().parents[1] / "database" / "schema.sql"
FMT = "%Y-%m-%d %H:%M:%S"
INICIO = datetime(2026, 11, 8, 12, 0, 0, tzinfo=timezone.utc)
LIMITE_IDADE_MIN = MAX_IDADE_SYNC_MIN  # importado: o teste segue o teto real, não uma cópia dele
GATEWAY = "gateway_lab_01"
URL_NUVEM = "http://nuvem.teste/api/salvar_dados"

# Contrato do que chega na nuvem: nada de mac nem lote_id.
CHAVES_PAYLOAD = {"senderAddress", "gateway_id", "rssi", "medicoes"}
CHAVES_MEDICAO = {"idade_min", *(f"temp_ds{i}" for i in range(1, 7))}


class RelogioFalso(datetime):
    """Substitui o datetime do sync: now() devolve o instante simulado."""
    t = INICIO

    @classmethod
    def now(cls, tz=None):
        return cls.t


class Resp:
    """Imita requests.Response no que o sync usa (status_code, ok, text)."""

    def __init__(self, status, text=""):
        self.status_code = status
        self.ok = status < 400
        self.text = text


class NuvemFalsa:
    """Imita a rota /api/salvar_dados: valida o lote e responde 200 ou 400.
    Com online=False levanta ConnectionError, como numa queda de rede.
    latencia_s avança o relógio simulado a cada POST (nuvem lenta)."""

    def __init__(self, latencia_s=0):
        self.online = True
        self.latencia_s = latencia_s
        self.chamadas = 0
        self.urls = []
        self.timeouts = []
        self.recebidos = []
        self.rejeitados = []

    @staticmethod
    def validar(p):
        """None se o payload é válido; senão, o motivo da recusa."""
        if set(p) != CHAVES_PAYLOAD:
            return f"chaves fora do contrato: {sorted(set(p) ^ CHAVES_PAYLOAD)}"
        meds = p["medicoes"]
        if not (type(p["senderAddress"]) is int and 1 <= p["senderAddress"] <= 65534):
            return "senderAddress inválido"
        if not (isinstance(meds, list) and 1 <= len(meds) <= 16):
            return "medicoes deve ter de 1 a 16 leituras"
        if any(set(m) != CHAVES_MEDICAO for m in meds):
            return "medição fora do contrato"
        idades = [m["idade_min"] for m in meds]
        if len(set(idades)) != len(idades):
            return "idade_min repetida"
        if not all(type(i) is int and 0 <= i <= LIMITE_IDADE_MIN for i in idades):
            return "idade_min fora do intervalo"
        return None

    def __call__(self, url, json=None, timeout=None):
        self.chamadas += 1
        self.urls.append(url)
        self.timeouts.append(timeout)
        if not self.online:
            raise requests.ConnectionError("sem rede")
        RelogioFalso.t += timedelta(seconds=self.latencia_s)
        motivo = self.validar(json)
        if motivo:
            self.rejeitados.append(json)
            return Resp(400, motivo)
        self.recebidos.append(json)
        return Resp(200)


def mac_do_no(n_no):
    return f"{n_no:012X}"


def temperatura(j, i):
    """Valor da sonda i (1..6) na leitura j de um lote."""
    return 25 + j / 10 + i


def receber_lote(conn, n_no, lote_id, recebido_em, leituras=16, com_temperaturas=True):
    """Insere um lote como o worker faria: 1 linha por leitura, 30 min entre elas.
    com_temperaturas=False imita o laboratório, onde o preparar_lote zera as sondas."""
    mac = mac_do_no(n_no)
    linhas = []
    for j in range(leituras):
        idade = (leituras - 1 - j) * 30
        data_hora = (recebido_em - timedelta(minutes=idade)).strftime(FMT)
        temps = [temperatura(j, i) if com_temperaturas else None for i in range(1, 7)]
        linhas.append((data_hora, GATEWAY, n_no, *temps, -80.0, mac, lote_id))
    conn.executemany(
        "INSERT INTO medicoes (data_hora, gateway_id, sensor_id, temp_ds1, temp_ds2, "
        "temp_ds3, temp_ds4, temp_ds5, temp_ds6, rssi, mac, lote_id) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        linhas,
    )
    conn.commit()


class Base(unittest.TestCase):
    latencia_s = 0

    def setUp(self):
        # o model abre uma conexão por chamada: precisa de um arquivo, não de :memory:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        caminho = str(Path(tmp.name) / "pi.db")
        self._patch(patch.dict(DB_CONFIG, {"path": caminho}))
        self.conn = sqlite3.connect(caminho)  # só para inserir lotes e conferir contagens
        self.conn.executescript(SCHEMA.read_text())
        self.addCleanup(self.conn.close)
        RelogioFalso.t = INICIO
        self.nuvem = NuvemFalsa(self.latencia_s)
        self._patch(patch.object(sync, "datetime", RelogioFalso))
        self._patch(patch.object(sync, "CLOUD_URL", URL_NUVEM))
        self._patch(patch.object(sync.requests, "post", side_effect=self.nuvem))

    def _patch(self, p):
        p.start()
        self.addCleanup(p.stop)

    def avanca(self, segundos):
        RelogioFalso.t += timedelta(seconds=segundos)

    def pendentes(self):
        return self.conn.execute("SELECT COUNT(*) FROM medicoes WHERE enviado_em IS NULL").fetchone()[0]

    def total(self):
        return self.conn.execute("SELECT COUNT(*) FROM medicoes").fetchone()[0]

    def marcas(self, n_no):
        rows = self.conn.execute(
            "SELECT DISTINCT enviado_em FROM medicoes WHERE mac = ?", (mac_do_no(n_no),)).fetchall()
        return [r[0] for r in rows]

    def inserir_linha_solta(self, mac, lote_id, sensor_id=9):
        """Uma linha que não veio de um lote identificado (inserção manual ou legada)."""
        self.conn.execute(
            "INSERT INTO medicoes (data_hora, gateway_id, sensor_id, temp_ds1, mac, lote_id) "
            "VALUES (?,?,?,?,?,?)",
            (INICIO.strftime(FMT), GATEWAY, sensor_id, 25.0, mac, lote_id))
        self.conn.commit()


class SemanticaDoSync(Base):
    # ---- o que vai para a nuvem -------------------------------------------------

    def test_payload_nao_leva_mac_nem_lote_id(self):
        receber_lote(self.conn, 7, 3, INICIO)
        sync.sincronizar()
        payload = self.nuvem.recebidos[0]
        self.assertEqual(set(payload), CHAVES_PAYLOAD)
        self.assertNotIn("mac", payload)
        self.assertNotIn("lote_id", payload)
        self.assertEqual(payload["senderAddress"], 7)
        self.assertEqual(len(payload["medicoes"]), 16)
        for m in payload["medicoes"]:
            self.assertEqual(set(m), CHAVES_MEDICAO)

    def test_nuvem_recusa_payload_que_vaza_identificacao(self):
        """Garante que a nuvem falsa é mesmo estrita: se o sync voltasse a mandar mac/lote_id,
        todos os testes de envio falhariam (400 -> 'descartado')."""
        receber_lote(self.conn, 7, 3, INICIO)
        montar_original = sync.montar

        def montar_vazando(linhas, agora):
            p = montar_original(linhas, agora)
            p["mac"], p["lote_id"] = linhas[0]["mac"], linhas[0]["lote_id"]
            return p

        with patch.object(sync, "montar", montar_vazando), self.assertLogs(level="WARNING"):
            sync.sincronizar()
        self.assertEqual(self.nuvem.recebidos, [])
        self.assertEqual(len(self.nuvem.rejeitados), 1)
        self.assertEqual(self.marcas(7), ["descartado"])

    def test_payload_preserva_gateway_rssi_e_temperaturas(self):
        receber_lote(self.conn, 7, 3, INICIO)
        sync.sincronizar()
        payload = self.nuvem.recebidos[0]
        self.assertEqual(payload["gateway_id"], GATEWAY)
        self.assertEqual(payload["rssi"], -80.0)
        for j, m in enumerate(payload["medicoes"]):
            for i in range(1, 7):
                self.assertEqual(m[f"temp_ds{i}"], temperatura(j, i))

    def test_temperatura_nula_segue_como_nula(self):
        receber_lote(self.conn, 7, 3, INICIO, com_temperaturas=False)
        sync.sincronizar()
        for m in self.nuvem.recebidos[0]["medicoes"]:
            self.assertTrue(all(m[f"temp_ds{i}"] is None for i in range(1, 7)))

    def test_medicoes_em_ordem_cronologica(self):
        receber_lote(self.conn, 1, 1, INICIO)
        sync.sincronizar()
        idades = [m["idade_min"] for m in self.nuvem.recebidos[0]["medicoes"]]
        self.assertEqual(idades, sorted(idades, reverse=True))  # a mais velha primeiro
        self.assertEqual(idades, [450 - 30 * j for j in range(16)])

    def test_usa_a_url_configurada_e_define_timeout(self):
        receber_lote(self.conn, 1, 1, INICIO)
        sync.sincronizar()
        self.assertEqual(self.nuvem.urls, [URL_NUVEM])
        self.assertIsNotNone(self.nuvem.timeouts[0])  # sem timeout, uma queda trava o sync

    def test_idade_min_e_relativa_ao_momento_do_envio(self):
        receber_lote(self.conn, 1, 1, INICIO)
        self.avanca(3 * 3600)  # sync atrasa 3 h
        sync.sincronizar()
        idades = [m["idade_min"] for m in self.nuvem.recebidos[0]["medicoes"]]
        self.assertEqual((min(idades), max(idades)), (180, 450 + 180))

    # ---- resposta da nuvem ------------------------------------------------------

    def test_200_marca_enviado_em(self):
        receber_lote(self.conn, 1, 1, INICIO)
        sync.sincronizar()
        self.assertEqual(self.pendentes(), 0)
        self.assertEqual(self.marcas(1), [INICIO.strftime(FMT)])

    def test_ciclo_seguinte_nao_reenvia(self):
        receber_lote(self.conn, 1, 1, INICIO)
        sync.sincronizar()
        sync.sincronizar()
        self.assertEqual(self.nuvem.chamadas, 1)

    def test_400_marca_descartado_e_nao_trava(self):
        receber_lote(self.conn, 1, 1, INICIO - timedelta(minutes=LIMITE_IDADE_MIN + 1))
        receber_lote(self.conn, 2, 1, INICIO)
        with self.assertLogs(level="WARNING"):
            sync.sincronizar()
        self.assertEqual(self.marcas(1), ["descartado"])
        self.assertEqual(len(self.nuvem.recebidos), 1)
        self.assertEqual(self.pendentes(), 0)

    def test_descartado_nao_e_reenviado(self):
        receber_lote(self.conn, 1, 1, INICIO - timedelta(minutes=LIMITE_IDADE_MIN + 1))
        with self.assertLogs(level="WARNING"):
            sync.sincronizar()
        sync.sincronizar()
        self.assertEqual(self.nuvem.chamadas, 1)

    def test_sem_resposta_mantem_pendente_e_para(self):
        for erro in (requests.ConnectionError("sem rede"), requests.Timeout("lenta")):
            with self.subTest(erro=type(erro).__name__):
                self.conn.execute("DELETE FROM medicoes")
                receber_lote(self.conn, 1, 1, INICIO)
                receber_lote(self.conn, 2, 1, INICIO)
                with patch.object(sync.requests, "post", side_effect=erro) as post:
                    with self.assertLogs(level="WARNING"):
                        sync.sincronizar()
                self.assertEqual(post.call_count, 1)  # não insiste no ciclo
                self.assertEqual(self.pendentes(), 32)

    def test_erro_que_nao_e_400_mantem_pendente_e_para(self):
        """Só o 400 descarta. Token errado (401), rota errada (404), limite (429) e 5xx
        são problemas da nuvem/config: os dados têm de esperar, não ser perdidos."""
        receber_lote(self.conn, 1, 1, INICIO)
        receber_lote(self.conn, 2, 1, INICIO)
        for status in (401, 404, 429, 500, 502, 503):
            with self.subTest(status=status):
                with patch.object(sync.requests, "post", return_value=Resp(status)) as post:
                    with self.assertLogs(level="ERROR"):
                        sync.sincronizar()
                self.assertEqual(post.call_count, 1)
                self.assertEqual(self.pendentes(), 32)

    # ---- lotes e ordem ----------------------------------------------------------

    def test_um_lote_por_vez_na_ordem_do_model(self):
        for n in (3, 1, 2):
            receber_lote(self.conn, n, 1, INICIO)
        sync.sincronizar()
        # pendentes() ordena por mac, lote_id, data_hora
        self.assertEqual([p["senderAddress"] for p in self.nuvem.recebidos], [1, 2, 3])
        self.assertTrue(all(len(p["medicoes"]) == 16 for p in self.nuvem.recebidos))

    def test_lotes_do_mesmo_no_sao_enviados_separados(self):
        for lote_id in (1, 2, 3):
            receber_lote(self.conn, 1, lote_id, INICIO)
        sync.sincronizar()
        self.assertEqual(len(self.nuvem.recebidos), 3)
        self.assertTrue(all(len(p["medicoes"]) == 16 for p in self.nuvem.recebidos))

    def test_falha_no_segundo_lote_nao_desfaz_o_primeiro(self):
        receber_lote(self.conn, 1, 1, INICIO)
        receber_lote(self.conn, 1, 2, INICIO)
        with patch.object(sync.requests, "post", side_effect=[Resp(200), Resp(503)]):
            with self.assertLogs(level="ERROR"):
                sync.sincronizar()
        estados = self.conn.execute(
            "SELECT DISTINCT lote_id, enviado_em FROM medicoes ORDER BY lote_id").fetchall()
        self.assertEqual(estados, [(1, INICIO.strftime(FMT)), (2, None)])

    def test_volta_da_queda_nao_duplica(self):
        for n in range(1, 4):
            receber_lote(self.conn, n, 1, INICIO)
        self.nuvem.online = False
        with self.assertLogs(level="WARNING"):
            sync.sincronizar()
        self.nuvem.online = True
        sync.sincronizar()
        sync.sincronizar()  # ciclo extra: nada mais a enviar
        self.assertEqual([p["senderAddress"] for p in self.nuvem.recebidos], [1, 2, 3])

    # ---- identificação: sem ela, o Pi não sincroniza ----------------------------

    def test_linha_sem_lote_id_nao_e_enviada(self):
        self.inserir_linha_solta(mac_do_no(9), None)
        sync.sincronizar()
        self.assertEqual(self.nuvem.chamadas, 0)
        self.assertEqual(self.pendentes(), 1)

    def test_linha_sem_mac_nao_e_enviada(self):
        """Sem mac, marcar_enviado (WHERE mac = ?) nunca casaria: a linha seria reenviada
        a cada ciclo e duplicada na nuvem. Exige mac IS NOT NULL em MedicaoModel.pendentes()."""
        self.inserir_linha_solta(None, 1)
        sync.sincronizar()
        sync.sincronizar()
        self.assertEqual(self.nuvem.chamadas, 0)
        self.assertEqual(self.pendentes(), 1)

    def test_linha_solta_nao_atrapalha_os_lotes_identificados(self):
        self.inserir_linha_solta(None, 1)
        self.inserir_linha_solta(mac_do_no(9), None)
        receber_lote(self.conn, 1, 1, INICIO)
        sync.sincronizar()
        self.assertEqual(len(self.nuvem.recebidos), 1)
        self.assertEqual(self.pendentes(), 2)  # só as duas soltas continuam pendentes


class RelogioPorLote(Base):
    latencia_s = 60  # cada POST demora 1 min (nuvem lenta)

    def test_idade_min_conta_o_tempo_gasto_com_os_lotes_anteriores(self):
        for n in (1, 2, 3):
            receber_lote(self.conn, n, 1, INICIO)
        sync.sincronizar()
        # cada POST leva 1 min: o 2º e o 3º lote saem 1 e 2 min depois do 1º
        minimas = [min(m["idade_min"] for m in p["medicoes"]) for p in self.nuvem.recebidos]
        self.assertEqual(minimas, [0, 1, 2])


class ContratoComPreparaLote(Base):
    """A NuvemFalsa é uma simplificação. Aqui a 'nuvem' é o preparar_lote REAL, chamado como
    a rota deveria chamar (teto do sync, sem retenção do gateway). Assume que /api/salvar_dados
    valida assim. Se a rota real usar outros parâmetros, ajuste _nuvem_real."""

    LAB = 7

    def setUp(self):
        super().setUp()
        self._patch(patch.object(config, "LAB_SENSOR_ID", str(self.LAB)))
        self._patch(patch.object(config, "LAB_GATEWAY_ID", GATEWAY))
        self._patch(patch.object(sync.requests, "post", side_effect=self._nuvem_real))
        self.aceitos = []
        self.recusados = []

    def _nuvem_real(self, url, json=None, timeout=None):
        try:
            preparar_lote(copy.deepcopy(json), max_idade=MAX_IDADE_SYNC_MIN, max_retencao=None)
        except LoteInvalido as e:
            self.recusados.append((json, str(e)))
            return Resp(400, str(e))
        self.aceitos.append(json)
        return Resp(200)

    def test_lote_do_laboratorio_e_aceito(self):
        receber_lote(self.conn, self.LAB, 1, INICIO, com_temperaturas=False)
        sync.sincronizar()
        self.assertEqual(self.recusados, [])
        self.assertEqual(len(self.aceitos), 1)
        self.assertEqual(self.pendentes(), 0)

    def test_lote_no_limite_do_teto_e_aceito(self):
        # a leitura mais velha (15 x 30 = 450 min antes do recebimento) fica com idade == teto
        receber_lote(self.conn, self.LAB, 1, INICIO - timedelta(minutes=LIMITE_IDADE_MIN - 450),
                     com_temperaturas=False)
        sync.sincronizar()
        self.assertEqual(self.recusados, [])
        self.assertEqual(self.pendentes(), 0)

    def test_lote_acima_do_teto_e_recusado_e_descartado(self):
        receber_lote(self.conn, self.LAB, 1, INICIO - timedelta(minutes=LIMITE_IDADE_MIN - 449),
                     com_temperaturas=False)
        with self.assertLogs(level="WARNING"):
            sync.sincronizar()
        self.assertEqual(self.aceitos, [])
        self.assertEqual(self.marcas(self.LAB), ["descartado"])

    def test_lote_de_no_fora_do_laboratorio_e_recusado(self):
        """Regra atual do preparar_lote: 'medicoes' só vale para o ponto de laboratório."""
        receber_lote(self.conn, 8, 1, INICIO)
        with self.assertLogs(level="WARNING"):
            sync.sincronizar()
        self.assertEqual(self.aceitos, [])
        self.assertEqual(self.marcas(8), ["descartado"])


class Cenario1(Base):
    """1: só rodando. Sem dados, vários ciclos, nenhum envio e nenhum erro."""

    def test_ocioso(self):
        for _ in range(5):
            sync.sincronizar()
            self.avanca(60)
        self.assertEqual(self.nuvem.chamadas, 0)
        self.assertEqual(self.total(), 0)


class Cenario2(Base):
    """2: 5 min de tráfego normal (10 nós por Pi, 1 lote de 16 cada) e envio."""

    NOS = 10

    def test_cinco_minutos_normais(self):
        n_no = 0
        for _ in range(5):  # 5 ciclos de 60 s; 2 lotes chegam por minuto
            for _ in range(self.NOS // 5):
                n_no += 1
                receber_lote(self.conn, n_no, 1, RelogioFalso.t)
            self.avanca(60)
            sync.sincronizar()
        self.assertEqual(len(self.nuvem.recebidos), self.NOS)
        self.assertEqual(len(self.nuvem.rejeitados), 0)
        self.assertEqual(self.pendentes(), 0)
        self.assertEqual(self.total(), self.NOS * 16)


class Cenario3(Base):
    """3: tráfego exorbitante (300 nós x 3 lotes) em 5 min, com queda da nuvem no meio."""

    NOS = 300
    LOTES = 3

    def test_exorbitante_com_queda_no_meio(self):
        for n in range(1, self.NOS + 1):
            for lote_id in range(1, self.LOTES + 1):
                receber_lote(self.conn, n, lote_id, RelogioFalso.t)
        self.avanca(60)
        self.nuvem.online = False
        with self.assertLogs(level="WARNING"):
            sync.sincronizar()
        self.assertEqual(self.nuvem.chamadas, 1)  # a queda interrompe o ciclo no 1º POST
        self.assertEqual(self.pendentes(), self.NOS * self.LOTES * 16)
        self.nuvem.online = True
        self.avanca(60)
        sync.sincronizar()
        total = self.NOS * self.LOTES
        self.assertEqual(len(self.nuvem.recebidos), total)  # um POST por lote
        self.assertEqual(len(self.nuvem.rejeitados), 0)
        self.assertEqual(self.pendentes(), 0)
        self.assertTrue(all(len(p["medicoes"]) <= 16 for p in self.nuvem.recebidos))

    def test_lote_velho_demais_nao_trava_a_fila(self):
        receber_lote(self.conn, 1, 1, INICIO - timedelta(minutes=LIMITE_IDADE_MIN + 1))
        for n in range(2, 52):
            receber_lote(self.conn, n, 1, INICIO)
        with self.assertLogs(level="WARNING"):
            sync.sincronizar()
        self.assertEqual(len(self.nuvem.recebidos), 50)
        self.assertEqual(self.marcas(1), ["descartado"])
        self.assertEqual(self.pendentes(), 0)


if __name__ == "__main__":
    unittest.main()