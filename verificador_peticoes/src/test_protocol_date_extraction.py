import sys
from projudi_extractor import ProjudiExtractor

def run():
    e = ProjudiExtractor()
    cases = [
        ("Data de Protocolo: 13/11/2025", "13/11/2025"),
        ("Data do Protocolo: 13/11/2025", "13/11/2025"),
        ("Protocolada em 13.11.2025", "13/11/2025"),
        ("Protocolado em 13-11-2025", "13/11/2025"),
        ("Protocolo 2025-11-13", "13/11/2025"),
        ("Protocolo 2025/11/13", "13/11/2025"),
        ("manifestacao 13/11/2025 _9565_56790_", "13/11/2025"),
        ("Juntada em 12/11/2025 — Protocolo em 13/11/2025", "13/11/2025"),
    ]
    oks = []
    for txt, exp in cases:
        got = e._pick_protocol_date(txt)
        print(f"{txt} -> {got}")
        oks.append(got == exp)
    titles = [
        ("id_999_doc._00_5188032_43_2019_8_09_0152_9565_56790_manifestacao_13.11.2025.pdf", "13/11/2025"),
        ("id_999_doc._00_5188032_43_2019_8_09_0152_9565_56790_manifestacao_13/11/2025.pdf", "13/11/2025"),
        ("id_999_doc._00_5188032_43_2019_8_09_0152_9565_56790_manifestacao_13-11-2025.pdf", "13/11/2025")
    ]
    for t, exp in titles:
        info = e._parse_nome_documento(t)
        print(f"title -> {info.get('data')}")
        oks.append(info.get('data') == exp)
    print("OK" if all(oks) else "FAIL")

if __name__ == "__main__":
    run()