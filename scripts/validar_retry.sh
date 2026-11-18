#!/bin/sh
# Validação de retry (T08) com HTTP real: congela o nó 1 por 1 s (menos que a
# suspeita de 3 s, mais que o timeout HTTP de 0,5 s de compose.retry.yaml)
# enquanto o nó 2 envia. Orquestra somente o projeto chat-t08.
set -eu
cd "$(dirname "$0")/.."

compose() {
    docker compose -p chat-t08 -f compose.yaml -f compose.retry.yaml -f compose.testes.yaml --profile testes "$@"
}

limpar() {
    resultado=$?
    trap - EXIT INT TERM
    compose unpause no1 >/dev/null 2>&1 || true
    if [ "$resultado" -ne 0 ]; then compose logs --tail=100 no1 no2 no3; fi
    compose down --volumes
    exit "$resultado"
}

if [ -n "$(compose ps -aq)" ]; then
    echo "O projeto chat-t08 já tem contêineres; encerre essa execução antes de validar." >&2
    exit 1
fi
trap limpar EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

compose build no1 no2 no3 testes
compose up -d no1 no2 no3
compose run --rm --no-deps testes python scripts/validar_retry.py antes
# O cliente conecta e espera; o host congela o nó 1 por 1 s.
compose run --rm --no-deps testes python scripts/validar_retry.py enviar &
cliente=$!
sleep 4
compose pause no1
sleep 1
compose unpause no1
wait "$cliente"
compose run --rm --no-deps testes python scripts/validar_retry.py conferir
# Prova do retry: tentativas do nó 2 para o nó 1 que falharam e foram repetidas.
echo "tentativas repetidas pelo canal FIFO do nó 2 para o nó 1: $(compose logs no2 | grep -c 'falha ao enviar .* para nó 1')"
