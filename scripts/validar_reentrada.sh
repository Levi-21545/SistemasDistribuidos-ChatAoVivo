#!/bin/sh
# Orquestra somente o projeto chat-t06; Python e clientes rodam em contêineres.
set -eu
cd "$(dirname "$0")/.."

compose() {
    docker compose -p chat-t06 -f compose.yaml -f compose.testes.yaml --profile testes "$@"
}

limpar() {
    resultado=$?
    trap - EXIT INT TERM
    if [ "$resultado" -ne 0 ]; then compose logs --tail=100 no1 no2 no3; fi
    compose down --volumes
    exit "$resultado"
}

# Não interfere numa execução já aberta deste roteiro.
if [ -n "$(compose ps -aq)" ]; then
    echo "O projeto chat-t06 já tem contêineres; encerre essa execução antes de validar." >&2
    exit 1
fi
trap limpar EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [ "$#" -eq 0 ]; then set -- 3 1 2; fi
for no in "$@"; do
    case "$no" in 1|2|3) ;; *) echo "Nó inválido: $no" >&2; exit 1 ;; esac
done
compose build no1 no2 no3 testes
for no in "$@"; do
    compose up -d no1 no2 no3
    compose run --rm --no-deps testes python scripts/validar_reentrada.py antes "$no"
    compose kill "no$no"
    compose run --rm --no-deps testes python scripts/validar_reentrada.py queda "$no"
    compose start "no$no"
    compose run --rm --no-deps testes python scripts/validar_reentrada.py retorno "$no"
    compose down --volumes
done
