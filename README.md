# Estufa Concorrente

Projeto desenvolvido para a disciplina de Sistemas Operacionais Embarcados. O programa simula o monitoramento da umidade do solo e o controle de irrigação, demonstrando processos, threads, comunicação entre processos e sincronização.

## Objetivo

Simular um sistema que recebe leituras de umidade do solo e liga ou desliga a irrigação conforme limites definidos.

- A irrigação liga quando a umidade fica abaixo de 35%.
- A irrigação desliga quando a umidade chega a 50% ou mais.
- As leituras são fixas para que a execução seja reproduzível.

## Requisitos

- Ubuntu ou outra distribuição Linux
- GCC
- Bibliotecas POSIX para threads, semáforos e filas de mensagens

No Ubuntu, instale as ferramentas de compilação com:

```bash
sudo apt update
sudo apt install build-essential
