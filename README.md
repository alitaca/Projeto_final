# Projeto Final - Mobilidade em São Paulo/SP

O Projeto Mobilidade em São Paulo foi criado com o objetivo de identificar mudanças no padrão de mobilidade urbana dos paulistanos.

## Começando

São Paulo é a cidade com a maior população do país, com mais de 12 milhões de pessoas, segundo estimativas do IGBE para 2020. O atendimento a essa população requer muita estrutura e planejamento. Um dos problemas mais complexos da cidade é o transporte. Muito se fala da quantidade de carros em São Paulo e do trânsito caótico. Realmente, a quantidade de carros na cidade é bem grande: são mais de 20 milhões de veículos cadastrados na cidade segundo levantamento do IBGE em 2020.

Mas a cidade também conta com diversos outros modais, como ônibus, metrô, entre outros. Neste trabalho, os principais dados utilizados serão os dados de ônibus. Também foram avaliados dados de metrô em menor profundidade.

Segundo dados da SPTrans, empresa que administra o serviço de transporte coletivo na cidade, São Paulo possui uma frota cadastrada de quase de 14 mil coletivos, que atende, em média, mais de 10 milhões de passageiros por dia. Em dias úteis, a frota precorre 3 milhões Km por dia, em mais de 200 mil viagens, e distribuídas em mais de 1.300 linhas.

Nos últimos tempos, muito se tem discutido sobre as alternativas à utilização de carros na cidade, e aqui iremos discutir se o transporte por ônibus é uma das alternativas que o paulistano está buscando.

Ver:

```
transporte_sp.pptx
```


## Desenvolvimento

### Pré-requisitos

* Python 3
* PostgreSQL 13
* Navegador Web

### Buscando dados

O dataset principal foi extraído do repositório [mittelmax/sptrans_Data](https://github.com/mittelmax/sptrans_Data.git). 
Os dados complementares foram buscados no site da SPTrans, e os dados de metrô foram extraídos do site do Metrô de São Paulo.

### Base de dados

O carregamento dos dados foi feito por meio de uma pipeline de dados desenvolvida em Python que carrega o dataset principal, mas também faz a busca de novos dados na internet. Os dados extraídos são então tratados e salvos.

Para a visualização, a base de dados foi salva em uma base de dados SQL local, e, portanto, não está disponível online. Porém, a pipeline se encontra no repósitório deste projeto.

### Avaliações

A base de dados principal possui 2.796.237 linhas com dados de passageiros de ônibus compilados desde 2015. A análise desses dados permitiu verificar a utilização dos ônibus no período pré-pandemia, e também o impacto inicial das medidas de restrição de circulação. Uma forte queda no número de passageiros foi observada em abril de 2020, mas após esse mês, foi observado um aumento desse número até agosto de 2020. Para avaliar melhor esse comportamento após as medidas de restrições, foi decidido que os dados de 2020 seriam sobrescritos com dados atualizados disponíveis no site da SPTrans, e seriam incluídos os dados de 2021 até o momento. A base de dados final utilizada possui 3.123.675 linhas e 16 colunas.

Para avaliar a redução de passageiros apresentada nos gráficos temporais, foi feito um teste de hipótese. Porém, o teste não apresentou resultados promissores, e, portanto, esta vertente foi abandonada.

Foi observado que houve, sim, redução no número de passageiros a partir de 2016, indicando que o ônibus não é a alternativa escolhida em São Paulo. Outro indicador que mostrou que a utilização de ônibus vem diminuindo é o número de linhas operadas. Os dados de metrô não demonstraram mudança significativa antes da pandemia, o que reforça que os meios de transporte coletivos não são as opções de transporte que as pessoas estão buscando na cidade, embora pôde ser observado um aumento dos passageiros de gratuidades e estudantes nos ônibus.

As conclusões do estudo são apresentadas no arquivo [transporte_sp.pptx](https://github.com/alitaca/Projeto_final/blob/7d8e1450329f5b51e66ea9baef25be86d7ee1ea5/transporte_sp.pptx)

## Construído com

As ferramentas utilizadas para criar o projeto

* [Python](https://www.python.org/) - Usado para obter e tratar o dataset
* [PostgreSQL](https://www.postgresql.org/) - Usado para
* [MS Power BI](https://powerbi.microsoft.com/en/) - Usado para visualização de dados

### Bibliotecas

* [os](https://docs.python.org/3/library/os.html)
* [requests](https://pypi.org/project/requests/)
* [BeautifulSoup](https://pypi.org/project/beautifulsoup4/)
* [pandas](https://pandas.pydata.org/)
* [re](https://docs.python.org/3/library/re.html)
* [logging](https://docs.python.org/3/howto/logging.html)
* [sqlalchemy](https://www.sqlalchemy.org/)
* [scipy](https://www.scipy.org/)

### Data Sources

* https://github.com/mittelmax/sptrans_Data.git
* https://www.prefeitura.sp.gov.br/cidade/secretarias/transportes/institucional/sptrans/acesso_a_informacao/index.php?p=295718
* https://transparencia.metrosp.com.br/dataset/demanda


## Link do Repositório Online

* [Repositório](https://github.com/alitaca/Projeto_final.git) - Link para o repositório do projeto.

## Autores

* **Aline** - *Trabalho Inicial* - [alitaca](https://github.com/alitaca)


## Licença

Este projeto está sob licença - veja o arquivo LICENSE.md para detalhes.

## Expressões de gratidão

* Aos professores da Ironhack, em especial a Rai e o Adriano :100:
* A Guilherme e Eduardo, colegas de curso que ajudaram com os insights :brain:
* Ao [mittelmax](https://github.com/mittelmax), o salvador que disponibilizou os dados (tratados!!!:raised_hands:) para o estudo