# Projeto Final - Mobilidade em São Paulo/SP

O Projeto Mobilidade em São Paulo foi criado com o objetivo de identificar mudanças no padrão de mobilidade urbana dos paulistanos.

## Começando

Ver:

'''
[transporte_sp.pptx](https://github.com/alitaca/Projeto_final/blob/7d8e1450329f5b51e66ea9baef25be86d7ee1ea5/transporte_sp.pptx)
'''

### Pré-requisitos

* Python 3
* PostgreSQL 13
* Navegador Web


## Desenvolvimento

### Buscando dados

O dataset principal foi extraído do repositório [mittelmax/sptrans_Data](https://github.com/mittelmax/sptrans_Data.git). 
Os dados complementares foram buscados no site da SPTrans, e os dados de metrô foram extraídos do site do Metrô de São Paulo.

### Base de dados

O carregamento dos dados foi feito por meio de uma pipeline de dados que carrega o dataset compilado, mas também faz a busca de novos dados na internet. Os dados extraídos são então tratados e salvos.

Para a visualização, a base de dados foi salvo em uma base de dados SQL local, portanto, não está disponível online. Porém, a pipeline se encontra no repósitório deste projeto.

Para avaliar a redução de passageiros apresentada nos gráficos temporais, foi feito um teste de hipótese. Porém, o teste não apresentou resultados promissores, e, portanto, esa vertente foi abandonada.

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
* https://www.detran.sp.gov.br/wps/portal/portaldetran/detran/estatisticastransito/3a410653-0dd2-45df-a324-bdfc4711d988


## Link do Repositório Online

* [Repositório](https://github.com/alitaca/Projeto_final.git) - Link para o repositório do projeto.

## Autores

* **Aline** - *Trabalho Inicial* - [alitaca](https://github.com/alitaca)


## Licença

Este projeto está sob licença - veja o arquivo LICENSE.md para detalhes.

## Expressões de gratidão

* Aos professores da Ironhack, em especial a Rai e o Adriano :100:
* A Guilherme e Eduardo, colegas de curso que ajudaram com os insights :brain:
