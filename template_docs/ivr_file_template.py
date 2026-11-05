FILE_HEADER = '''
\\documentclass{report}
\\usepackage{longtable}
\\usepackage[margin=1.0cm]{geometry}
\\setlength{\\footskip}{0.5cm}
\\usepackage{url}
\\setlength{\\parindent}{0pt}

\\title{Laudo Quantitativo}
\\author{}
\\date{\\today}


\\begin{document}

\\maketitle
'''

DESCRICAO = '''    
\\section{{Descrição}}
\\subsection{{0) PROCESSO}}
\\begin{{itemize}}
	\\item{{Número do processo: {numero_processo}}}
	\\item{{Período de produção: {inicio_producao} a {fim_producao}}}
    \\item{{Data-base do cálculo: {data_base}}}
\\end{{itemize}}


\\subsection{{1) POLO ATIVO}}
\\begin{{itemize}}
	\\item{{Razão social: {razao_social}}} 
	\\item{{Nome fantasia: {nome_fantasia}}}
	\\item{{Código CNES: {cnes}}}
	{cnpj}
	\\item{{UF: {estado}}} 
	\\item{{Cidade: {cidade}}}
\\end{{itemize}}


\\subsection{{2) POLO PASSIVO}}
\\begin{{itemize}}
	\\item{{União Federal}}
\\end{{itemize}}
'''

METODOLOGIA = '''
\\section{Metodologia}


\\subsection{Seleção e tratamento dos registros}

A seleção dos registros considera o estabelecimento de saúde identificado pelo CNES, as competências abrangidas pela apuração e as fontes de produção utilizadas.

O filtro por CNES é aplicado durante a conversão dos arquivos DBF para CSV, mediante comparação entre o código CNES do estabelecimento analisado e o código registrado em cada ocorrência. Nos arquivos PA do SIA/SUS, utiliza-se o campo \\texttt{PA\\_CODUNI}; nos arquivos SP do SIH/SUS, utiliza-se o campo \\texttt{SP\\_CNES}. Quando aplicado esse filtro, somente os registros correspondentes ao estabelecimento identificado são incluídos nos arquivos CSV destinados à apuração.

Para os registros ambulatoriais provenientes dos arquivos PA do SIA/SUS, são utilizados os seguintes campos:

\\begin{itemize}
    \\item \\texttt{PA\\_CODUNI}: código CNES do estabelecimento;
    \\item \\texttt{PA\\_CMP}: competência da produção;
    \\item \\texttt{PA\\_PROC\\_ID}: código do procedimento;
    \\item \\texttt{PA\\_QTDAPR}: quantidade aprovada;
    \\item \\texttt{PA\\_VALAPR}: valor aprovado.
\\end{itemize}

Para os registros hospitalares provenientes dos arquivos SP do SIH/SUS, são utilizados:

\\begin{itemize}
    \\item \\texttt{SP\\_CNES}: código CNES do estabelecimento;
    \\item \\texttt{SP\\_AA} e \\texttt{SP\\_MM}: ano e mês da competência;
    \\item \\texttt{SP\\_ATOPROF}: código do procedimento;
    \\item \\texttt{SP\\_QTD\\_ATO}: quantidade registrada;
    \\item \\texttt{SP\\_VALATO}: valor registrado.
\\end{itemize}

Os registros apresentados no detalhamento dos procedimentos são consolidados por competência mensal e código de procedimento, mediante a soma das respectivas quantidades e valores.

A ocorrência de um mesmo procedimento mais de uma vez na mesma competência não caracteriza, isoladamente, duplicidade indevida. Todas as ocorrências legítimas consideradas na apuração contribuem para os totais apresentados.

\\subsection{Critério de apuração das diferenças}

A apuração considera, para cada competência, os valores efetivamente registrados como pagos pelo SUS e os valores devidos segundo o critério IVR adotado no cálculo.

A diferença devida e não paga corresponde à subtração do valor registrado como pago pelo SUS do valor devido calculado segundo o critério estabelecido.

Os valores individuais são consolidados por competência mensal e, posteriormente, agrupados por ano, permitindo identificar a origem e a evolução dos montantes apresentados.

\\subsection{Atualização monetária e juros}

Sobre as diferenças devidas e não pagas são aplicados os fatores de atualização correspondentes aos períodos abrangidos pelo cálculo.

Para os créditos anteriores a janeiro de 2022, considera-se o fator de atualização anterior à incidência da SELIC, conforme os critérios de correção monetária e juros adotados na apuração.

O crédito consolidado até dezembro de 2021 é posteriormente submetido à incidência da taxa SELIC a partir de janeiro de 2022, observados os fatores acumulados até a data-base utilizada no cálculo.

A metodologia de consolidação e posterior incidência da SELIC encontra referência na Resolução CJF n. 784/2022 e nas disposições da Resolução CNJ n. 303/2019, alterada pela Resolução CNJ n. 448/2022, sem prejuízo da observância do título judicial e das alterações normativas aplicáveis ao período calculado.

A parcela de atualização anterior a janeiro de 2022 e o acréscimo decorrente da SELIC são apresentados separadamente em valores monetários, preservando-se o resultado da aplicação do fator total utilizado pelo sistema.

Os demonstrativos mensais apresentam as taxas aplicadas, os respectivos acréscimos monetários e o valor atualizado de cada competência.

\\subsection{Consolidação e apresentação dos resultados}

Os valores apurados são apresentados em três níveis de consolidação:

\\begin{itemize}
    \\item \\textbf{Resumo total:} diferença devida e não paga, atualização e montante final;
    \\item \\textbf{Resumo anual:} consolidação das diferenças e dos valores atualizados por exercício;
    \\item \\textbf{Resumo mensal:} discriminação dos valores originais, taxas, parcelas de atualização e valores finais por competência.
\\end{itemize}

Adicionalmente, o relatório apresenta o detalhamento dos procedimentos, discriminando os códigos, descrições, quantidades, valores pagos, diferenças devidas e fontes identificadas.

A apresentação consolidada não representa exclusão de ocorrências legítimas de um mesmo procedimento, mas sua agregação para facilitar a conferência dos resultados.

\\subsection{Processamento computacional e reprodutibilidade}

A extração, conversão e consolidação dos dados são realizadas mediante rotinas computacionais desenvolvidas em Python e C, com utilização de arquivos intermediários em formato DBF e CSV.

A metodologia permite a reprodução dos cálculos a partir das mesmas bases de origem, dos critérios de seleção, dos parâmetros financeiros e da versão do processamento utilizados.

A correspondência entre os registros de origem e os valores consolidados deve ser verificada mediante os arquivos e parâmetros efetivamente utilizados na execução.
'''


CONCLUSAO = '''
\\section{{Conclusões}}
Com base nas informações extraídas do DATASUS de procedimentos hospitalares e ambulatoriais (valores e quantidades), onde este perito processou 100\\% (cem por cento) destas informações e, por último, aplicando as correções monetárias e juros de mora, tem-se o total da ação de cumprimento de sentença de R\\$ \\textbf{{{valor_total}}}.
'''

TOTAL_HEADER = '''
\\section{Resumo Total}
\\begin{longtable}[c]{|p{2.6cm}|p{2.6cm}|p{2.6cm}|p{2.3cm}|}
	\\caption{Resumo total} \\\\ \\hline
	\\textbf{Diferença devida e não paga (R\$)} &
	\\textbf{Atualização (R\$)} &
	\\textbf{Valor total devido (R\$)}
	\\endhead \\hline
'''

TOTAL_FOOTER = '''
    \\end{longtable}
'''

YEAR_HEADER = '''
\\section{Resumo Anual}
\\begin{longtable}[c]{|p{1.7cm}|p{2.6cm}|p{2.6cm}|p{2.3cm}|}
	\\caption{Resumo ano a ano} \\\\ \\hline
	\\textbf{Ano} &
	\\textbf{Diferença devida e não paga (R\$)} &
	\\textbf{Atualização (R\$)} &
	\\textbf{Valor total devido (R\$)}
	\\endhead \\hline
'''

YEAR_FOOTER = '''
    \\end{longtable}
'''

MONTH_HEADER = '''
\\section{Resumo mensal}
\\begin{longtable}[c]{|p{1.3cm}|p{2.2cm}|p{2.5cm}|p{2.0cm}|p{2.0cm}|p{2.0cm}|p{2.0cm}|c|}
	\\caption{Resumo mês a mês} \\\\ \\hline
	\\textbf{Mês do Crédito} &
	\\textbf{Valor original pago pelo SUS (R\$)} &
	\\textbf{Diferença devida e não paga (R\$)} &
	\\textbf{Atualização antes de 01/2022} &
	\\textbf{Valor pré-Selic (R\$)} &
	\\textbf{Selic a partir de 01/2022} &
	\\textbf{Valor total devido (R\$)}
	\\endhead \\hline
'''

MONTH_FOOTER = '''
\\end{longtable}
'''

FILE_FOOTER =  '''\\end{document}'''
