<!--
SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>

SPDX-License-Identifier: MIT
-->
![Badge em Produção](https://img.shields.io/badge/status-em%20produ%C3%A7%C3%A3o-green)
![Badge em Produção](https://img.shields.io/badge/release%20date-january-blue)

# ETL de dados públicos para o banco de dados da ImpulsoGov

Extração, tratamento e caregamento de dados públicos direta ou indiretamente relacionados ao Sistema Único de Saúde brasileiro, tendo como destino o banco de dados da [Impulso Gov](https://impulsogov.org/).
Nesse documento você encontrará informações sobre o contexto de uso desses dados e orientações sobre funcionamento, instalação, licença de uso e glossário de termos.

*******
## :mag_right: Índice
1. [Contexto](#contexto)
2. [Estrutura do repositório](#estrutura)
3. [Rodando em produção](#rodando)
4. [Instruções para instalação e acesso ao projeto](#instalacao)
    1. [Instalação via código fonte](#subinstalacao1)
    1. [Instalação e execução locais com Docker](#subinstalacao2)
5. [Glossário de conceitos](#glossario)
6. [Contribua](#contribua)
7. [Licença](#licenca)
*******


<div id='contexto'/>  

## :rocket: Contexto

Um dos propósitos da ImpulsoGov, enquanto organização, é transformar dados da saúde pública do Brasil em informações que ofereçam oferecer suporte de decisão aos gestores de saúde pública em todo o Brasil. Embora o SUS tenha uma riqueza de dados há muitas dificuldades para reunir, coletar e analisar dados em diferentes sistemas de informação. O projeto de ETL de dados públicos surgiu para automatizar a extração desses dados de diferentes fontes através de raspagem de dados, estruturação e integração em nosso próprio banco de dados. Hoje o projeto sustenta a base de dados que alimenta nossas ferramentas gratuítas disponíveis em nossas plataformas como o [Impulso Previne](https://www.impulsoprevine.org/) e a de Indicadores de Saúde Mental.

*******
  
  
 <div id='estrutura'/>  
 
 ## :milky_way: Estrutura do repositório

O repositório possui um pacote Python contendo a lógica para a
captura de dados públicos, sob o nome `impulsoetl`.

```plain
etl
├─ src
│  ├─ impulsoetl
│  │  └─ ...
└─ ...
```

O pacote `impulsoetl` contém as lógicas de obtenção de dados do SIASUS, SIHSUS
e alguns dados do SCNES e do SISAB, incluindo interfaces com agendadores de
tarefas e registradores de logs de transações.


*******
 <div id='rodando'/> 
 
## :gear: Rodando em produção

O pacote `impulsoetl` utiliza ações do
[GitHub Actions](https://docs.github.com/actions) para enviar imagens para o
[DockerHub da Impulso Gov](https://hub.docker.com/orgs/impulsogov/repositories)
sempre que há uma atualização da branch principal do repositório. Diariamente,
essa imagem é baixada para uma máquina virtual que executa as capturas
pendentes.

Para executar os pacotes em produção, defina as credenciais necessárias como [segredos no repositório](https://docs.github.com/en/actions/security-guides/encrypted-secrets). Se necessário, ajuste os arquivos do diretório [.github/workflows](./.github/workflows) com as definições apropriadas para a execução das tarefas de implantação e de execução dos fluxos de ETL.
*******

<div id='instalacao'/> 

 ## 🛠️ Instruções para instalação e acesso ao projeto
 
 <div id='subinstalacao1'/> 
 
 ### Instalação via código fonte (para desenvolvimento local)
 
 A instalação do pacote depende do gerenciador de dependências [Poetry][].

Com o Poetry instalado, em sistemas com gerenciador de pacotes `apt` (ex. Debian, Ubuntu), rode as instruções abaixo no terminal de linha de comando:

[Poetry]: https://python-poetry.org/docs/#installation

```sh
# clonar e acessar a raíz do repositório
$ git clone https://github.com/ImpulsoGov/etl.git
$ cd etl

# Instalar pacote e dependências
$ poetry install -E impulsoetl
```

 <div id='subinstalacao2'/> 
 
 ### Instalação e execução locais com Docker
  
  Antes de rodar o container com o pacote `impulsoetl` localmente, crie um arquivo nomeado `.env` na raiz do repositório. Esse arquivo deve conter as credenciais de acesso ao banco de dados e outras configurações de execução do ETL. Você pode utilizar o modelo do arquivo `.env.sample` como referência.

Em seguida, execute os comandos abaixo em um terminal de linha de comando (a execução completa pode demorar):

```sh
$ docker build -t impulsoetl .
$ docker run -p 8888:8888 impulsoetl:latest
```

Esses comandos vão construir uma cópia local da imagem do Impulso e tentar executar as capturas de dados públicos agendadas no banco de dados.

*******
<div id='glossario'/>  

## :closed_book: Glossário de siglas

| Sigla  | Definição |
| :---    | :----    |
| SCNES    | O [Sistema do Cadastro Nacional de Estabelecimentos de Saúde](https://cnes.datasus.gov.br/pages/estabelecimentos/consulta.jsp) (SCNES) contém informações cadastrais de estabelecimentos, equipes e profissionais de saúde de todo o Brasil.   |
| SIASUS    | O [Sistema de Informações Ambulatoriais SUS](https://cnes.datasus.gov.br/pages/estabelecimentos/consulta.jsp) (SIASUS) é o sistema responsável por receber toda informação dos atendimentos realizados no âmbito ambulatorial do SUS por meio do Boletim de Produção Ambulatorial (BPA) |
| SIHSUS    | O [Sistema de Informações Hospitalares do SUS](https://datasus.saude.gov.br/acesso-a-informacao/morbidade-hospitalar-do-sus-sih-sus/) (SIHSUS) reune todos os atendimentos provenientes de internações hospitalares que foram financiados pelo SUS |
| SIM    | O Sistema de Informação Sobre Mortalidade (SIM) armazena dados de vigilância epidemiológica nacional captando informações sobre mortalidade para todas as instâncias do sistema de saúde. |
| SINAN    | O Sistema de Informação de Agravos de Notificação (SINAN) recebe dados de notificação e investigação de casos de doenças e agravos que constam da lista nacional de doenças de notificação compulsória. |
| SISAB    | O Sistema de Informação em Saúde para a Atenção Básica (SISAB) permite consultar informações da Atenção Básica como dados de cadastros, produção, validação da produção para fins de financiamento e de adesão aos programas e estratégias da Política Nacional de Atenção Básica. |
*******

<div id='contribua'/>  

## :left_speech_bubble: Contribua
Sinta-se à vontade para contribuir em nosso projeto! Abra uma issue ou envie PRs.

*******
<div id='licenca'/>  

## :registered: Licença
MIT © (?)
